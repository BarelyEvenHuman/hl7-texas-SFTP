from pathlib import Path
from string import Template
from datetime import date, timedelta, datetime
from HL7_utils import *

TEMPLATE_BASE = str(Path("templates"))
MSH_TEMPLATE = "msh.txt"
OBR_TEMPLATE = "obr.txt"
OBX_TEMPLATE = "obx.txt"
ORC_TEMPLATE = "orc.txt"
PID_TEMPLATE = "pid.txt"
RXA_TEMPLATE = "rxa.txt"
RXR_TEMPLATE = "rxr.txt"
PD1_TEMPLATE = "pd1.txt"


def loadFileTemplate(fileName):
    with open(TEMPLATE_BASE + "/" + fileName, "r") as file:
        return file.read()


def imprintTemplate(template_name, value_dict):
    sectionTemplate = loadFileTemplate(template_name)
    hl7Section = Template(sectionTemplate).substitute(value_dict)
    return hl7Section


# Generates a message header block by imprinting values from the data frame into a string
# template that is loaded from the file system
def createMSHBlock(msh_dict):
    return imprintTemplate(MSH_TEMPLATE, msh_dict)


# Generates a common order block by imprinting values from the data frame into a string
# template that is loaded from the file system
def createORCBlock(dataRow, control_number):
    orc_dict = dict()
    space = dataRow["Medical Professional"].find(" ")
    first = dataRow["Medical Professional"][:space].replace(" ","")
    last = dataRow["Medical Professional"][space:].replace(" ","")

    orc_dict["order_number"] = hl7StringRead(dataRow["Patient ID"])
    if len(orc_dict["order_number"]) > 20:
        orc_dict["order_number"] = orc_dict["order_number"][0:20]
    orc_dict["filler_order_number"] = control_number
    orc_dict["provider_npi"] = "1891733374"
    orc_dict["provider_last_name"] = "STEELY"
    orc_dict["provider_first_name"] = "JUNE"
    orc_dict["provider_phone_number"] = "385^3756419"
    orc_dict["checkedinby"] = dataRow["Patient Checked in By"]
    orc_dict["clinician_first"] = hl7StringRead(first)
    orc_dict["clinician_last"] = hl7StringRead(last)

    return imprintTemplate(ORC_TEMPLATE, orc_dict)


# Generates a patient identification block by imprinting values from the data frame into a string
# template that is loaded from the file system
def createPIDBlock(dataRow):

    pid_dict = dict()
    pid_dict["patient_mrn"] = hl7StringRead(dataRow["Patient ID"])
    if len(pid_dict["patient_mrn"]) > 20:
        pid_dict["patient_mrn"] = pid_dict["patient_mrn"][0:20]
    pid_dict["patient_ssn"] = ""

    pid_dict["patient_last"] = hl7StringRead(dataRow["Last Name"])
    pid_dict["patient_first"] = hl7StringRead(dataRow["First Name"])
    pid_dict["patient_mi"] = hl7StringRead(dataRow["Middle Initial"])

    date_time_string = dataRow["Date of Birth"]
    timestamp = datetime.strptime(date_time_string, "%Y-%m-%d")
    future = datetime.today() + relativedelta(years=1)
    if timestamp >= future:
        timestamp = timestamp.replace(year=timestamp.year - 100)
    pid_dict["patient_dob"] = timestamp.strftime("%Y%m%d")

    # pid_dict["patient_dob"] = convertStringDateToHL7(hl7StringRead(dataRow["Date of Birth"]))
    pid_dict["patient_gender"] = convertPatientGender(dataRow["Gender"])
    pid_dict["patient_race"] = convertPatientRace(dataRow["Race"])

    pid_dict["patient_address_1"] = dataRow["Street Address"]
    pid_dict["patient_address_2"] = ""
    pid_dict["patient_address_city"] = hl7StringRead(dataRow["City"])
    pid_dict["patient_address_state"] = findStateAbbreviation(dataRow["State"])
    pid_dict["patient_address_zip"] = dataRow["Zip Code"]

    # TODO - find and cast the phone number into an HL7 compliant format
    pid_dict["patient_phone"] = convertPhoneNumberToHL7(
        hl7StringRead(dataRow["Phone Number"])
    )
    # pid_dict["patient_ethinicity"] = "U"
    pid_dict["patient_ethinicity"] = convertPatientEthnicity(dataRow["Ethnicity"])

    return imprintTemplate(PID_TEMPLATE, pid_dict)


# Generates a vaccination block by imprinting values from the data frame into a string
# template that is loaded from the file system
def createRXABlock(dataRow):
    rxa_dict = dict()
    # null out the clinical data block in case we have a bad / blank row in the data
    rxa_dict["cvx_code"] = "999"
    rxa_dict["cvx_description"] = "UNDEFINED"
    rxa_dict["vis_description"] = "UNDEFINED"
    rxa_dict["vax_manufacturer"] = "UNDEFINED"
    rxa_dict["mfg_code"] = "UNK"

    vax_type = ""
    try:
        vax_type = hl7StringRead(dataRow["Appointment Service Name"])
    except Exception:
        pass
    vax_manufacturer = hl7StringRead(dataRow["Manufacturer"])

    age = int(dataRow["Age"])
    if (
        VACCINE_TYPE_PFIZER.lower() in vax_type.lower()
        or VACCINE_TYPE_PFR.lower() in vax_manufacturer.lower()
        or VACCINE_TYPE_PFIZER.lower() in vax_manufacturer.lower()
    ) and age in range(5, 12):
        rxa_dict["cvx_code"] = "218"
        rxa_dict[
            "cvx_description"
        ] = "COVID-19, mRNA, LNP-S, PF, 10 mcg/0.2 mL dose, tris-sucrose"
        rxa_dict["vis_description"] = "COVID-19 Pfizer Vaccine"
        rxa_dict["vax_manufacturer"] = "Pfizer"
        rxa_dict["mfg_code"] = "PFR"
    elif (
        VACCINE_TYPE_PFIZER.lower() in vax_type.lower()
        or VACCINE_TYPE_PFR.lower() in vax_manufacturer.lower()
        or VACCINE_TYPE_PFIZER.lower() in vax_manufacturer.lower()
    ):
        rxa_dict["cvx_code"] = "208"
        rxa_dict["cvx_description"] = "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose"
        rxa_dict["vis_description"] = "COVID-19 Pfizer Vaccine"
        rxa_dict["vax_manufacturer"] = "Pfizer"
        rxa_dict["mfg_code"] = "PFR"
    elif (
        VACCINE_TYPE_MODERNA.lower() in vax_type.lower()
        or VACCINE_TYPE_MOD.lower() in vax_manufacturer.lower()
        or VACCINE_TYPE_MODERNA.lower() in vax_manufacturer.lower()
    ):
        rxa_dict["cvx_code"] = "207"
        rxa_dict["cvx_description"] = "COVID-19, mRNA, LNP-S, PF, 100 mcg/0.5 mL dose"
        rxa_dict["vis_description"] = "COVID-19 Moderna Vaccine"
        rxa_dict["vax_manufacturer"] = "Moderna"
        rxa_dict["mfg_code"] = "MOD"
    elif (
        VACCINE_TYPE_OXFORD.lower() in vax_type.lower()
        or VACCINE_TYPE_ASZ.lower() in vax_manufacturer.lower()
        or VACCINE_TYPE_OXFORD.lower() in vax_manufacturer.lower()
    ):
        rxa_dict["cvx_code"] = "210"
        rxa_dict[
            "cvx_description"
        ] = "COVID-19 vaccine, vector-nr, rS-ChAdOx1, PF, 0.5 mL"
        rxa_dict["vis_description"] = "COVID-19 AstraZeneca Vaccine"
        rxa_dict["vax_manufacturer"] = "AstraZeneca"
        rxa_dict["mfg_code"] = "ASZ"
    elif (
        VACCINE_TYPE_JANSSEN.lower() in vax_type.lower()
        or VACCINE_TYPE_JNJ.lower() in vax_manufacturer.lower()
        or VACCINE_TYPE_JOHNSON.lower() in vax_manufacturer.lower()
        or VACCINE_TYPE_JANSSEN.lower() in vax_manufacturer.lower()
    ):
        rxa_dict["cvx_code"] = "212"
        rxa_dict["cvx_description"] = "COVID-19 vaccine, vector-nr, rS-Ad26, PF, 0.5 mL"
        rxa_dict["vis_description"] = "COVID-19 Janssen Vaccine"
        rxa_dict["vax_manufacturer"] = "Janssen"
        rxa_dict["mfg_code"] = "JSN"
    elif INFLUENZA_TYPE_Afluria.lower() in vax_manufacturer.lower():
        rxa_dict["cvx_code"] = "158"
        rxa_dict["cvx_description"] = "Influenza vaccine, 5 mL"
        rxa_dict["vis_description"] = "Influenza-19 Afluria Quadrivalent Vaccine"
        rxa_dict["vax_manufacturer"] = "Afluria Quadrivalent"
        rxa_dict["mfg_code"] = "SEQ"
    elif INFLUENZA_TYPE_Fluad.lower() in vax_manufacturer.lower():
        rxa_dict["cvx_code"] = "205"
        rxa_dict["cvx_description"] = "Influenza vaccine, 0.5 mL"
        rxa_dict["vis_description"] = "Influenza-19 Fluad Quadrivalent Vaccine"
        rxa_dict["vax_manufacturer"] = "Fluad Quadrivalent"
        rxa_dict["mfg_code"] = "SEQ"
    elif(VACCINE_TYPE_JYNNEOS.lower() in vax_manufacturer.lower()):
        rxa_dict["cvx_code"] = "206"
        rxa_dict["cvx_description"] = "Vaccinia, smallpox monkeypox vaccine live, PF"
        rxa_dict["vis_description"] = "Monkey Pox JYNNEOS Vaccine"
        rxa_dict["vax_manufacturer"] = "JYNNEOS"
        rxa_dict["mfg_code"] = "BN"


    space = dataRow["Medical Professional"].find(" ")
    first = dataRow["Medical Professional"][:space].replace(" ","")
    last = dataRow["Medical Professional"][space:].replace(" ","")
    rxa_dict["lot_number"] = findVaccineLot(dataRow["Lot"])
    rxa_dict["lot_exiration_date"] = convertStringDateToHL7(dataRow["Expiration"])
    date_time_string = dataRow["Vaccine Administered Date/Time"]
    timestamp = datetime.strptime(date_time_string, "%Y-%m-%dT%H:%MZ")
    future = datetime.today() + relativedelta(years=1)
    if timestamp >= future:
        timestamp = timestamp.replace(year=timestamp.year - 100)
    rxa_dict["procedure_date"] = timestamp.strftime("%Y%m%d%H%M%S")
    # rxa_dict["procedure_date"] = convertStringDateTimeToHL7(dataRow["Vaccine Administered Date/Time"])
    rxa_dict["clinician_first"] = hl7StringRead(first)
    rxa_dict["clinician_last"] = hl7StringRead(last)
    rxa_dict["location"] = ""
    # rxa_dict["location"] = hl7StringRead(dataRow["Appointment Location Name"])

    rxa_dict["report_date"] = datetime.now().strftime("%Y%m%d")

    return imprintTemplate(RXA_TEMPLATE, rxa_dict)


# Generates a vaccine administratrion block by imprinting values from the data frame into a string
# template that is loaded from the file system
def createRXRBlock(dataRow):
    rxr_dict = dict()

    injection_route = getAdministration(dataRow["Injection Route"])
    injection_site = getBodySite(dataRow["Administration Site"])

    rxr_dict["admin_code"] = injection_route["code"]
    rxr_dict["admin_decription"] = injection_route["description"]
    rxr_dict["location_code"] = injection_site["code"]
    rxr_dict["location_description"] = injection_site["description"]

    return imprintTemplate(RXR_TEMPLATE, rxr_dict)


# Generates an ethinicity block by imprinting values from the data frame into a string
# template that is loaded from the file system
def createPD1Block(dataRow):
    pd1_dict = dict()
    date_time_string = dataRow["Vaccine Administered Date/Time"]
    timestamp = datetime.strptime(date_time_string, "%Y-%m-%dT%H:%MZ")
    pd1_dict["Protection_Indicator"] = timestamp.strftime("%Y%m%d")
    return imprintTemplate(PD1_TEMPLATE, pd1_dict)


# Generates three observation segments by impriting values from the data frame into a string
# template that is loaded from the file system
def createOBXBlock(dataRow):
    obx_dict = dict()

    date_time_string = dataRow["Vaccine Administered Date/Time"]
    timestamp = datetime.strptime(date_time_string, "%Y-%m-%dT%H:%MZ")
    # timestamp = datetime.strptime(date_time_string, '%m/%d/%Y %H:%M')
    # future = datetime.today() + relativedelta(years=1)
    # if timestamp >= future:
    #    timestamp = timestamp.replace(year=timestamp.year - 100)
    obx_dict["vaccination_date"] = timestamp.strftime("%Y%m%d")
    return imprintTemplate(OBX_TEMPLATE, obx_dict)
