import random
import pandas as pd
import os
import boto3
import botocore
from segment_utils import *
from datetime import date, datetime
from io import StringIO
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from aws_lambda_powertools import Logger
import json
import time
import paramiko


logger = Logger(service="texasHL7sftp")
logger.append_keys(doh="")
logger.append_keys(patientid="")
logger.append_keys(vaccinedate="")


def datestdtojd(stddate):
    fmt = "%Y-%m-%d"
    sdtdate = datetime.strptime(stddate, fmt)
    sdtdate = sdtdate.timetuple()
    jdate = sdtdate.tm_yday
    if len(str(jdate)) == 3:
        return jdate
    else:
        return str(jdate).zfill(3)


def year():
    currentDateTime = datetime.now()
    date = currentDateTime.date()
    year = date.strftime("%y")
    return year


def get_credentials():
    secret_name = os.environ["SECRET_NAME"]
    region_name = os.environ["AWS_REGION"]

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.info("The requested secret was not found")
        elif ex.response["Error"]["Code"] == "InvalidRequestException":
            logger.info("The request was invalid due to:", ex)
        elif ex.response["Error"]["Code"] == "InvalidParameterException":
            logger.info("The request had invalid params:", ex)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if "SecretString" in get_secret_value_response:
            text_secret_data = get_secret_value_response["SecretString"]
        # else:
        #     binary_secret_data = get_secret_value_response["SecretBinary"]

    credentials = json.loads(text_secret_data)
    username = credentials["username"]
    password = credentials["password"]

    return username, password


# logs the HL7 message to a log file in the S3 bucket
def log_to_bucket(logType, status):
    s3 = boto3.resource("s3")
    today = datetime.today().strftime("%Y-%m-%d")
    fileName = "vaccine-logs/" + logType + "/" + today + ".json"
    obj = s3.Object(os.environ["BUCKET_NAME"], fileName)
    if logType == "Errors":
        logger.info("HL7 message error. Error: " + status)
    hl7List = [status]
    try:
        currentLog = json.load(obj.get()["Body"])
    except:
        logger.info("Creating new JSON for this date: " + str(today))
        currentLog = []

    currentLog = currentLog + hl7List
    jsonStr = json.dumps(currentLog)
    obj.put(Body=jsonStr)
    return


def writeHL7DocumentToFile(
    hl7_string, upload_bucket, patient_id, vaccination_date, error_dict, index
):
    try:
        logger.info("Writing HL7 Document to file...")
        logger.info(
            "Using patient_id {} and bucket {}".format(patient_id, upload_bucket)
        )
        today = datetime.today().strftime("%Y-%m-%d")
        document_string = "".join(hl7_string)
        hl7_file_name = f"NOMIHEALTV{str(year())}{str(datestdtojd(date.today().strftime('%Y-%m-%d')))}.{str(index)}.hl7"

        s3 = boto3.client("s3")

        s3.put_object(
            Bucket=upload_bucket,
            Key="texas-hl7-messages/" + hl7_file_name,
            Body=bytes(document_string, encoding="utf-8"),
        )

        logger.info("Write to HL7 successful")
        submit_hl7(
            document_string,
            hl7_file_name,
            patient_id,
            vaccination_date,
            error_dict,
            hl7_string,
        )
    except Exception as ex:
        error_str = f"Unable to submit HL7 to sftp with PatientID {patient_id} and vaccination date {vaccination_date}. {ex}"
        logger.error(error_str)
        log_to_bucket("Errors", error_str)


def submit_hl7(
    document_string, hl7_file_name, patient_id, vaccination_date, error_dict, hl7_string
):
    username, password = get_credentials()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname="immtrac-ftps1.dshs.state.tx.us",
        port=22,
        username=username,
        password=password,
        look_for_keys=False,
    )
    sftp = ssh.open_sftp()
    logger.info("Connection established.")

    sftp.putfo(StringIO(document_string), "/users/NOMIHEALTV/hl7-dropoff/" + hl7_file_name)
    logger.info("HL7 file transferred.")
    
    ssh.close()
    logger.info("Connection closed.")

    logger.info("Patient ID: " + patient_id + " Vaccination Date: " + vaccination_date)

    error_dict["Patient ID"].append(patient_id)
    error_dict["Vaccine Date"].append(vaccination_date)
    error_dict["HL7 Message"].append(hl7_string)


def lambda_handler(event, context):
    start_time = time.time()
    logger.info("Beginning lambda.")
    upload_bucket = os.environ["BUCKET_NAME"]
    object_key = unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    logger.info("File being used is: " + object_key)
    region = os.environ["AWS_REGION"]
    s3 = boto3.client(
        "s3", region, config=botocore.config.Config(s3={"addressing_style": "path"})
    )
    csv_obj = s3.get_object(Bucket=upload_bucket, Key=object_key)
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    input_df = pd.read_csv(StringIO(csv_string))
    # check the log of records we've already sent
    error_csv_obj = s3.get_object(Bucket=upload_bucket, Key="texas-vax/MessageLog.txt")
    error_body = error_csv_obj["Body"]
    error_csv_string = error_body.read().decode("utf-8")
    error_df = pd.read_csv(StringIO(error_csv_string))
    error_dict = {"Patient ID": [], "Vaccine Date": [], "HL7 Message": [], "Error": []}
    # don't bother with records we've already checked
    try:
        error_df["ID Date Combo"] = error_df.apply(
            lambda row: (row["Patient ID"], row["Vaccine Date"]), axis=1
        )
        input_df["ID Date Combo"] = input_df.apply(
            lambda row: (row["Patient ID"], row["Vaccine Administered Date"]), axis=1
        )
        sent_list = error_df["ID Date Combo"].unique()
        input_data = input_df[~input_df["ID Date Combo"].isin(sent_list)]
    except Exception as e:
        logger.error(f"File uploaded to bucket is blank. {e}")

    for index in range(len(input_data)):
        state = input_data["Vaccine_State"][index]
        patientid = input_data["Patient ID"][index]
        vaxdate = input_data["Vaccine Administered Date"][index]
        logger.append_keys(doh=state)
        logger.append_keys(patientid=patientid)
        logger.append_keys(vaccinedate=vaxdate)

        cur_time = time.time()

        time_diff = cur_time - start_time
        if time_diff >= 840:
            break

        control_number: str = "7501" + str(random.randrange(10000, 99999))
        logger.info("control_number: " + control_number)
        hl7_document = []
        patient_record = input_data.iloc[index]
        patient_id = patient_record["Patient ID"]
        vaccination_date = patient_record["Vaccine Administered Date"]
        # Vaccine_timestamp = datetime.strptime(date_time_vaccine_string, '%Y-%m-%d')
        # vaccination_date = str(Vaccine_timestamp.strftime('%Y%m%d'))
        message_timestamp = datetime.now().strftime("%Y%m%d%H%M%S") + "+0000"
        index: int
        msh_dict = dict()
        msh_dict["message_time_stamp"] = message_timestamp
        msh_dict["message_control_id"] = control_number

        try:
            MSHSegment = createMSHBlock(msh_dict)
            hl7_document.append(MSHSegment)
        except Exception as ex:
            error_str = f"{index} failed at MSH message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at MSH segment")
            continue

        try:
            PIDSegment = createPIDBlock(patient_record)
            hl7_document.append(PIDSegment)
        except Exception as ex:
            error_str = f"{index} failed at PID message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at PID segment")
            continue

        try:
            PD1Segment = createPD1Block(patient_record)
            hl7_document.append(PD1Segment)
        except Exception as ex:
            error_str = f"{index} failed at PD1 message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at PD1 segment")
            continue

        try:
            ORCSegment = createORCBlock(patient_record, control_number)
            hl7_document.append(ORCSegment)
        except Exception as ex:
            error_str = f"{index} failed at ORC message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at MSH segment")
            continue

        try:
            RXASegment = createRXABlock(patient_record)
            hl7_document.append(RXASegment)
        except Exception as ex:
            error_str = f"{index} failed at RXA message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at RXA segment")
            continue

        try:
            RXRSegment = createRXRBlock(patient_record)
            hl7_document.append(RXRSegment)
        except Exception as ex:
            error_str = f"{index} failed at RXR message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at RXR segment")
            continue

        try:
            OBXSegment = createOBXBlock(patient_record)
            hl7_document.append(OBXSegment)
        except Exception as ex:
            error_str = f"{index} failed at OBX message generation, Patient ID is : {patient_id} and Vaccination Date is : {vaccination_date}. {ex}"
            logger.error(error_str)
            log_to_bucket("Errors", error_str)

            error_dict["Patient ID"].append(patient_id)
            error_dict["Vaccine Date"].append(vaccination_date)
            error_dict["HL7 Message"].append("COULD NOT GENERATE")
            error_dict["Error"].append("Failed at OBX segment")
            continue
        hl7_string = "".join(hl7_document)
        writeHL7DocumentToFile(
            hl7_string, upload_bucket, patient_id, vaccination_date, error_dict, index
        )
        logger.info(f"{state} COMPLETED ROW " + str(index))

    cur_df = pd.DataFrame.from_dict(error_dict, orient="index")
    df = cur_df.transpose()
    df = error_df.append(cur_df, ignore_index=True)
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer,
        columns=["Patient ID", "HL7 Message", "Vaccine Date", "Error"],
        index=False,
    )
    s3.put_object(
        Bucket=upload_bucket, Key="texas-vax/MessageLog.txt", Body=csv_buffer.getvalue()
    )

    logger.info("FUNCTION COMPLETE")
