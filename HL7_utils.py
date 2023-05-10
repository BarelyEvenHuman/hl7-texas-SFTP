from datetime import date, timedelta, datetime
import phonenumbers
import us
import re
from dateutil.relativedelta import relativedelta

VACCINE_TYPE_MODERNA = "Moderna"
VACCINE_TYPE_PFIZER = "Pfizer"
VACCINE_TYPE_OXFORD = "Astra"
VACCINE_TYPE_JANSSEN = "Janssen"
VACCINE_TYPE_JOHNSON = "Johnson"
VACCINE_TYPE_MOD = "MOD"
VACCINE_TYPE_PFR = "PFR"
VACCINE_TYPE_ASZ = "ASZ"
VACCINE_TYPE_JNJ = "J&J"
VACCINE_TYPE_JYNNEOS = "JYN"
INFLUENZA_TYPE_Afluria = "Afluria Quadrivalent"
INFLUENZA_TYPE_Fluad = "Fluad Quadrivalent"


BODY_SITE_DICT = {
    "BN": "Bilateral Nares",
    "LA": "Left Arm",
    "LD": "Left Deltoid",
    "LG": "Left Gluteus Medius",
    "LLFA": "Left Lower Forearm",
    "LT": "Left Thigh",
    "LVL": "Left Vastus Lateralis",
    "RA": "Right Arm",
    "RD": "Right Deltoid",
    "RG": "Right Gluteus Medius",
    "RLFA": "Right Lower Forearm",
    "RT": "Right Thigh",
    "RVL": "Right Vastus Lateralis",
}
ADMINISTER_PROCESS = {
    "EP": "Epidural",
    "IH": "Inhalation",
    "IA": "Intra-arterial",
    "IB": "Intrabursal",
    "IC": "Intracardiac",
    "ICV": "Intracervical (uterus)",
    "ID": "Intradermal",
    "IHA": "Intrahepatic Artery",
    "IM": "Intramuscular",
    "IN": "Intranasal",
    "IO": "Intraocular",
    "IP": "Intraperitoneal",
    "IS": "Intrasynovial",
    "IT": "Intrathecal",
    "IU": "Intrauterine",
    "IV": "Intravenous",
    "NS": "Nasal",
    "NP": "Nasal Prongs",
    "NG": "Nasogastric",
    "NT": "Nasotrachial Tube",
    "OP": "Ophthalmic",
    "PO": "Oral",
    "OTH": "Other/Miscellaneous",
    "OT": "Otic",
    "PF": "Perfusion",
    "RM": "Rebreather Mask",
    "SC": "Subcutaneous",
    "SL": "Sublingual",
    "TP": "Topical",
    "TD": "Transdermal",
    "TL": "Translingual",
}


# substitutes a caret if the string is blank from the input stream
def hl7StringRead(some_string):
    try:
        if isinstance(some_string, str):
            if not some_string:
                return "^"
            else:
                return some_string
        else:
            return ""
    except Exception:
        return ""


# looks up the state abbreviation code from the name
def findStateAbbreviation(state_name):
    raw_value = hl7StringRead(state_name)

    if len(raw_value) > 2:
        state = us.states.lookup(raw_value)
        return state.abbr

    return ""


# returns the key in a string dictionary if the values match
def searchDictonary(a_dictionary, some_value):
    for key, value in a_dictionary.items():
        # for v in value:
        if some_value.lower() in value.lower():
            return key


def getAdministration(administration_string):
    try:
        raw_string = hl7StringRead(administration_string)
        key = searchDictonary(ADMINISTER_PROCESS, raw_string)
        administration_dict = {"code": key, "description": ADMINISTER_PROCESS[key]}
        return administration_dict
    except Exception:
        return {"code": "", "description": ""}


def getBodySite(site_string):
    try:
        raw_string = hl7StringRead(site_string)
        key = searchDictonary(BODY_SITE_DICT, raw_string)
        site_dict = {"code": key, "description": BODY_SITE_DICT[key]}
        return site_dict
    except Exception:
        return {"code": "", "description": ""}


def convertStringDateToHL7(inputDate):
    try:
        if isinstance(inputDate, str):
            future = datetime.today() + relativedelta(years=1)
            timestamp = datetime.strptime(inputDate, "%m/%d/%y")
            if future <= timestamp:  # must be in the past
                timestamp = timestamp.replace(year=timestamp.year - 100)

            return timestamp.strftime("%Y%m%d%H")
        else:
            return ""
    except ValueError:
        return ""


def convertStringDateTimeToHL7(inputDate):
    try:
        if isinstance(inputDate, str):
            future = datetime.today() + relativedelta(years=1)
            timestamp = datetime.strptime(inputDate, "%m/%d/%y %H:%M")
            if future <= timestamp:  # must be in the past
                timestamp = timestamp.replace(year=timestamp.year - 100)

            return timestamp.strftime("%Y%m%d%H%M%S")
        else:
            return ""
    except ValueError:
        return ""


def convertPhoneNumberToHL7(inputPhoneNumber):
    try:
        if isinstance(inputPhoneNumber, str):
            phone_number = phonenumbers.parse(inputPhoneNumber, "US")
            digits = str(phone_number.national_number)
            formatted = digits[:3] + "^" + digits[-7:]
            return formatted
        else:
            return ""
    except Exception:
        return ""


def convertPatientGender(gender_string):
    raw_string = hl7StringRead(gender_string)
    if raw_string.lower().startswith("m"):
        return "M"
    if raw_string.lower().startswith("f"):
        return "F"
    if raw_string.lower().startswith("n"):
        return "N"
    if "transgender" in raw_string.lower():
        return "T"
    if raw_string.lower().startswith("o"):
        return "O"
    return ""


def convertPatientRace(race_string):
    raw_string = hl7StringRead(race_string)
    if raw_string.lower().startswith("w"):
        return "2106-3^White"
    if raw_string.lower().startswith("asian"):
        return "2028-9^Asian"
    if raw_string.lower().startswith("black"):
        return "2054-5^Black"
    if raw_string.lower().startswith("africa"):
        return "2054-5^African_American"
    if "alaska" in raw_string.lower():
        return "1002-5^alaska_native"
    if raw_string.lower().startswith("other"):
        return "2131-1^Other_Race"
    if "hawaii" in raw_string.lower():
        return "2076-8^native_hawaiian"
    if "pacific" in raw_string.lower():
        return "2076-8^pacific_islander"
    return "2131-1^Other_Race"


def convertPatientEthnicity(ethnicity_string):
    raw_string = hl7StringRead(ethnicity_string)
    if raw_string.lower().startswith("not"):
        return "2186-5^Not Hispanic or Latino"
    if raw_string.lower().startswith("hispanic") or raw_string.lower().startswith(
        "latino"
    ):
        return "2135-2^Hispanic or Latino"
    return "2186-5^Not Hispanic or Latino"


# this assumes that the format is "Vendor - Lot"
def findVaccineLot(lot_string):
    raw_string = hl7StringRead(lot_string)
    segments = re.split(r"[\s-]+", raw_string)

    return segments[-1]
