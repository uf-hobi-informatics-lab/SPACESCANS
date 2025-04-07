"""

    This file contains the table definitions for the ZIP_9 geocoded Exposome 
    SQLite database. 

"""

from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase

# The base table used for building the rest of the
# tables.
class exposome_base(DeclarativeBase):
    pass

# Temporary table for testing purposes
class TEST(exposome_base):
    __tablename__='test_table'
    USERID: Mapped[str] = mapped_column(String(20), primary_key=True)
    PASSWORD: Mapped[str] = mapped_column(String(10))
    EMAIL: Mapped[str] = mapped_column(String(50))
    FAVORITE_COLOR: Mapped[str] = mapped_column(String(50))

class acag(exposome_base):
    __tablename__='ACAG'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR_MONTH: Mapped[str] = mapped_column(String(7), primary_key=True)
    BC: Mapped[Float] = mapped_column(Float)
    NH4: Mapped[Float] = mapped_column(Float)
    NIT: Mapped[Float] = mapped_column(Float)
    OM: Mapped[Float] = mapped_column(Float)
    SO4: Mapped[Float] = mapped_column(Float)
    SOIL: Mapped[Float] = mapped_column(Float)
    SS: Mapped[Float] = mapped_column(Float)
    

class caces(exposome_base):
    __tablename__='CACES'
    zip9: Mapped[str] = mapped_column(String(9), primary_key=True)
    year: Mapped[str] = mapped_column(String(4), primary_key=True)
    o3: Mapped[Float] = mapped_column(Float)
    co: Mapped[Float] = mapped_column(Float)
    so2: Mapped[Float] = mapped_column(Float)
    no2: Mapped[Float] = mapped_column(Float)
    pm10: Mapped[Float] = mapped_column(Float)
    pm25: Mapped[Float] = mapped_column(Float)


class epa_nata(exposome_base):
    __tablename__='EPA_NATA'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR: Mapped[str] = mapped_column(String(4), primary_key=True)
    natapol1: Mapped[Float] = mapped_column(Float)
    natapol2: Mapped[Float] = mapped_column(Float)
    natapol3: Mapped[Float] = mapped_column(Float)
    natapol4: Mapped[Float] = mapped_column(Float)
    natapol5: Mapped[Float] = mapped_column(Float)
    natapol6: Mapped[Float] = mapped_column(Float)
    natapol7: Mapped[Float] = mapped_column(Float)
    natapol8: Mapped[Float] = mapped_column(Float)
    natapol9: Mapped[Float] = mapped_column(Float)
    natapol10: Mapped[Float] = mapped_column(Float)
    natapol11: Mapped[Float] = mapped_column(Float)
    natapol12: Mapped[Float] = mapped_column(Float)
    natapol13: Mapped[Float] = mapped_column(Float)
    natapol14: Mapped[Float] = mapped_column(Float)
    natapol15: Mapped[Float] = mapped_column(Float)
    natapol16: Mapped[Float] = mapped_column(Float)
    natapol17: Mapped[Float] = mapped_column(Float)
    natapol18: Mapped[Float] = mapped_column(Float)
    natapol19: Mapped[Float] = mapped_column(Float)
    natapol20: Mapped[Float] = mapped_column(Float)
    natapol21: Mapped[Float] = mapped_column(Float)
    natapol22: Mapped[Float] = mapped_column(Float)
    natapol23: Mapped[Float] = mapped_column(Float)
    natapol24: Mapped[Float] = mapped_column(Float)
    natapol25: Mapped[Float] = mapped_column(Float)
    natapol26: Mapped[Float] = mapped_column(Float)
    natapol27: Mapped[Float] = mapped_column(Float)
    natapol28: Mapped[Float] = mapped_column(Float)
    natapol29: Mapped[Float] = mapped_column(Float)
    natapol30: Mapped[Float] = mapped_column(Float)
    natapol31: Mapped[Float] = mapped_column(Float)
    natapol32: Mapped[Float] = mapped_column(Float)
    natapol33: Mapped[Float] = mapped_column(Float)
    natapol34: Mapped[Float] = mapped_column(Float)
    natapol35: Mapped[Float] = mapped_column(Float)
    natapol36: Mapped[Float] = mapped_column(Float)
    natapol37: Mapped[Float] = mapped_column(Float)
    natapol38: Mapped[Float] = mapped_column(Float)
    natapol39: Mapped[Float] = mapped_column(Float)
    natapol40: Mapped[Float] = mapped_column(Float)
    natapol41: Mapped[Float] = mapped_column(Float)
    natapol42: Mapped[Float] = mapped_column(Float)
    natapol43: Mapped[Float] = mapped_column(Float)
    natapol44: Mapped[Float] = mapped_column(Float)
    natapol45: Mapped[Float] = mapped_column(Float)
    natapol46: Mapped[Float] = mapped_column(Float)
    natapol47: Mapped[Float] = mapped_column(Float)
    natapol48: Mapped[Float] = mapped_column(Float)
    natapol49: Mapped[Float] = mapped_column(Float)
    natapol50: Mapped[Float] = mapped_column(Float)
    natapol51: Mapped[Float] = mapped_column(Float)
    natapol52: Mapped[Float] = mapped_column(Float)
    natapol53: Mapped[Float] = mapped_column(Float)
    natapol54: Mapped[Float] = mapped_column(Float)
    natapol55: Mapped[Float] = mapped_column(Float)
    natapol56: Mapped[Float] = mapped_column(Float)
    natapol57: Mapped[Float] = mapped_column(Float)
    natapol58: Mapped[Float] = mapped_column(Float)
    natapol59: Mapped[Float] = mapped_column(Float)
    natapol60: Mapped[Float] = mapped_column(Float)
    natapol61: Mapped[Float] = mapped_column(Float)
    natapol62: Mapped[Float] = mapped_column(Float)
    natapol63: Mapped[Float] = mapped_column(Float)
    natapol64: Mapped[Float] = mapped_column(Float)
    natapol65: Mapped[Float] = mapped_column(Float)
    natapol66: Mapped[Float] = mapped_column(Float)
    natapol67: Mapped[Float] = mapped_column(Float)
    natapol68: Mapped[Float] = mapped_column(Float)
    natapol69: Mapped[Float] = mapped_column(Float)
    natapol70: Mapped[Float] = mapped_column(Float)
    natapol71: Mapped[Float] = mapped_column(Float)
    natapol72: Mapped[Float] = mapped_column(Float)
    natapol73: Mapped[Float] = mapped_column(Float)
    natapol74: Mapped[Float] = mapped_column(Float)
    natapol75: Mapped[Float] = mapped_column(Float)
    natapol76: Mapped[Float] = mapped_column(Float)
    natapol77: Mapped[Float] = mapped_column(Float)
    natapol78: Mapped[Float] = mapped_column(Float)
    natapol79: Mapped[Float] = mapped_column(Float)
    natapol80: Mapped[Float] = mapped_column(Float)
    natapol81: Mapped[Float] = mapped_column(Float)
    natapol82: Mapped[Float] = mapped_column(Float)
    natapol83: Mapped[Float] = mapped_column(Float)
    natapol84: Mapped[Float] = mapped_column(Float)
    natapol85: Mapped[Float] = mapped_column(Float)
    natapol86: Mapped[Float] = mapped_column(Float)
    natapol87: Mapped[Float] = mapped_column(Float)
    natapol88: Mapped[Float] = mapped_column(Float)
    natapol89: Mapped[Float] = mapped_column(Float)
    natapol90: Mapped[Float] = mapped_column(Float)
    natapol91: Mapped[Float] = mapped_column(Float)
    natapol92: Mapped[Float] = mapped_column(Float)
    natapol93: Mapped[Float] = mapped_column(Float)
    natapol94: Mapped[Float] = mapped_column(Float)
    natapol95: Mapped[Float] = mapped_column(Float)
    natapol96: Mapped[Float] = mapped_column(Float)
    natapol97: Mapped[Float] = mapped_column(Float)
    natapol98: Mapped[Float] = mapped_column(Float)
    natapol99: Mapped[Float] = mapped_column(Float)
    natapol100: Mapped[Float] = mapped_column(Float)
    natapol101: Mapped[Float] = mapped_column(Float)
    natapol102: Mapped[Float] = mapped_column(Float)
    natapol103: Mapped[Float] = mapped_column(Float)
    natapol104: Mapped[Float] = mapped_column(Float)
    natapol105: Mapped[Float] = mapped_column(Float)
    natapol106: Mapped[Float] = mapped_column(Float)
    natapol107: Mapped[Float] = mapped_column(Float)
    natapol108: Mapped[Float] = mapped_column(Float)
    natapol109: Mapped[Float] = mapped_column(Float)
    natapol110: Mapped[Float] = mapped_column(Float)
    natapol111: Mapped[Float] = mapped_column(Float)
    natapol112: Mapped[Float] = mapped_column(Float)
    natapol113: Mapped[Float] = mapped_column(Float)
    natapol114: Mapped[Float] = mapped_column(Float)
    natapol115: Mapped[Float] = mapped_column(Float)
    natapol116: Mapped[Float] = mapped_column(Float)
    natapol117: Mapped[Float] = mapped_column(Float)
    natapol118: Mapped[Float] = mapped_column(Float)
    natapol119: Mapped[Float] = mapped_column(Float)
    natapol120: Mapped[Float] = mapped_column(Float)
    natapol121: Mapped[Float] = mapped_column(Float)
    natapol122: Mapped[Float] = mapped_column(Float)
    natapol123: Mapped[Float] = mapped_column(Float)
    natapol124: Mapped[Float] = mapped_column(Float)
    natapol125: Mapped[Float] = mapped_column(Float)
    natapol126: Mapped[Float] = mapped_column(Float)
    natapol127: Mapped[Float] = mapped_column(Float)
    natapol128: Mapped[Float] = mapped_column(Float)
    natapol129: Mapped[Float] = mapped_column(Float)
    natapol130: Mapped[Float] = mapped_column(Float)
    natapol131: Mapped[Float] = mapped_column(Float)
    natapol132: Mapped[Float] = mapped_column(Float)
    natapol133: Mapped[Float] = mapped_column(Float)
    natapol134: Mapped[Float] = mapped_column(Float)
    natapol135: Mapped[Float] = mapped_column(Float)
    natapol136: Mapped[Float] = mapped_column(Float)
    natapol137: Mapped[Float] = mapped_column(Float)
    natapol138: Mapped[Float] = mapped_column(Float)
    natapol139: Mapped[Float] = mapped_column(Float)
    natapol140: Mapped[Float] = mapped_column(Float)
    natapol141: Mapped[Float] = mapped_column(Float)
    natapol142: Mapped[Float] = mapped_column(Float)
    natapol143: Mapped[Float] = mapped_column(Float)
    natapol144: Mapped[Float] = mapped_column(Float)
    natapol145: Mapped[Float] = mapped_column(Float)
    natapol146: Mapped[Float] = mapped_column(Float)
    natapol147: Mapped[Float] = mapped_column(Float)
    natapol148: Mapped[Float] = mapped_column(Float)
    natapol149: Mapped[Float] = mapped_column(Float)
    natapol150: Mapped[Float] = mapped_column(Float)
    natapol151: Mapped[Float] = mapped_column(Float)
    natapol152: Mapped[Float] = mapped_column(Float)
    natapol153: Mapped[Float] = mapped_column(Float)
    natapol154: Mapped[Float] = mapped_column(Float)
    natapol155: Mapped[Float] = mapped_column(Float)
    natapol156: Mapped[Float] = mapped_column(Float)
    natapol157: Mapped[Float] = mapped_column(Float)
    natapol158: Mapped[Float] = mapped_column(Float)
    natapol159: Mapped[Float] = mapped_column(Float)
    natapol160: Mapped[Float] = mapped_column(Float)
    natapol161: Mapped[Float] = mapped_column(Float)
    natapol162: Mapped[Float] = mapped_column(Float)
    natapol163: Mapped[Float] = mapped_column(Float)
    natapol164: Mapped[Float] = mapped_column(Float)
    natapol165: Mapped[Float] = mapped_column(Float)
    natapol166: Mapped[Float] = mapped_column(Float)
    natapol167: Mapped[Float] = mapped_column(Float)
    natapol168: Mapped[Float] = mapped_column(Float)
    natapol169: Mapped[Float] = mapped_column(Float)
    natapol170: Mapped[Float] = mapped_column(Float)
    natapol171: Mapped[Float] = mapped_column(Float)
    natapol172: Mapped[Float] = mapped_column(Float)
    natapol173: Mapped[Float] = mapped_column(Float)
    natapol174: Mapped[Float] = mapped_column(Float)
    natapol175: Mapped[Float] = mapped_column(Float)


class us_hud(exposome_base):
    __tablename__='US_HUD'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR: Mapped[str] = mapped_column(String(4), primary_key=True)
    QUARTER: Mapped[str] = mapped_column(String(6), primary_key=True)
    VAC: Mapped[Float] = mapped_column(Float)
    AVG_VAC: Mapped[Float] = mapped_column(Float)
    VAC_3: Mapped[Float] = mapped_column(Float)
    VAC_3TO6: Mapped[Float] = mapped_column(Float)
    VAC_6TO12: Mapped[Float] = mapped_column(Float)
    VAC_12TO24: Mapped[Float] = mapped_column(Float)
    VAC_24TO36: Mapped[Float] = mapped_column(Float)
    VAC_36: Mapped[Float] = mapped_column(Float)
    PQV_IS: Mapped[Float] = mapped_column(Float)
    PQV_NOSTAT: Mapped[Float] = mapped_column(Float)
    NOSTAT: Mapped[Float] = mapped_column(Float)
    AVG_NOSTAT: Mapped[Float] = mapped_column(Float)
    NS_3: Mapped[Float] = mapped_column(Float)
    NS_3TO6: Mapped[Float] = mapped_column(Float)
    NS_6TO12: Mapped[Float] = mapped_column(Float)
    NS_12TO24: Mapped[Float] = mapped_column(Float)
    NS_24TO36: Mapped[Float] = mapped_column(Float)
    NS_36: Mapped[Float] = mapped_column(Float)
    PQNS_IS: Mapped[Float] = mapped_column(Float)
    P_VAC: Mapped[Float] = mapped_column(Float)
    P_VAC_3: Mapped[Float] = mapped_column(Float)
    P_VAC_3TO6: Mapped[Float] = mapped_column(Float)
    P_VAC_6TO12: Mapped[Float] = mapped_column(Float)
    P_VAC_12TO24: Mapped[Float] = mapped_column(Float)
    P_VAC_24TO36: Mapped[Float] = mapped_column(Float)
    P_VAC_36: Mapped[Float] = mapped_column(Float)
    P_PQV_IS: Mapped[Float] = mapped_column(Float)
    P_PQV_NOSTAT: Mapped[Float] = mapped_column(Float)
    P_NOSTAT: Mapped[Float] = mapped_column(Float)
    P_NS_3: Mapped[Float] = mapped_column(Float)
    P_NS_3TO6: Mapped[Float] = mapped_column(Float)
    P_NS_6TO12: Mapped[Float] = mapped_column(Float)
    P_NS_12TO24: Mapped[Float] = mapped_column(Float)
    P_NS_24TO36: Mapped[Float] = mapped_column(Float)
    P_NS_36: Mapped[Float] = mapped_column(Float)
    P_PQNS_IS: Mapped[Float] = mapped_column(Float)
    AMS_RES: Mapped[Float] = mapped_column(Float)
    AMS_BUS: Mapped[Float] = mapped_column(Float)
    AMS_OTH: Mapped[Float] = mapped_column(Float)
    RES_VAC: Mapped[Float] = mapped_column(Float)
    BUS_VAC: Mapped[Float] = mapped_column(Float)
    OTH_VAC: Mapped[Float] = mapped_column(Float)
    AVG_VAC_R: Mapped[Float] = mapped_column(Float)
    AVG_VAC_B: Mapped[Float] = mapped_column(Float)
    VAC_3_RES: Mapped[Float] = mapped_column(Float)
    VAC_3_BUS: Mapped[Float] = mapped_column(Float)
    VAC_3_OTH: Mapped[Float] = mapped_column(Float)
    VAC_3_6_R: Mapped[Float] = mapped_column(Float)
    VAC_3_6_B: Mapped[Float] = mapped_column(Float)
    VAC_3_6_O: Mapped[Float] = mapped_column(Float)
    VAC_6_12R: Mapped[Float] = mapped_column(Float)
    VAC_6_12B: Mapped[Float] = mapped_column(Float)
    VAC_6_12O: Mapped[Float] = mapped_column(Float)
    VAC_12_24R: Mapped[Float] = mapped_column(Float)
    VAC_12_24B: Mapped[Float] = mapped_column(Float)
    VAC_12_24O: Mapped[Float] = mapped_column(Float)
    VAC_24_36R: Mapped[Float] = mapped_column(Float)
    VAC_24_36B: Mapped[Float] = mapped_column(Float)
    VAC_24_36O: Mapped[Float] = mapped_column(Float)
    VAC_36_RES: Mapped[Float] = mapped_column(Float)
    VAC_36_BUS: Mapped[Float] = mapped_column(Float)
    VAC_36_OTH: Mapped[Float] = mapped_column(Float)
    PQV_IS_RES: Mapped[Float] = mapped_column(Float)
    PQV_IS_BUS: Mapped[Float] = mapped_column(Float)
    PQV_IS_OTH: Mapped[Float] = mapped_column(Float)
    PQV_NS_RES: Mapped[Float] = mapped_column(Float)
    PQV_NS_BUS: Mapped[Float] = mapped_column(Float)
    PQV_NS_OTH: Mapped[Float] = mapped_column(Float)
    NOSTAT_RES: Mapped[Float] = mapped_column(Float)
    NOSTAT_BUS: Mapped[Float] = mapped_column(Float)
    NOSTAT_OTH: Mapped[Float] = mapped_column(Float)
    AVG_NS_RES: Mapped[Float] = mapped_column(Float)
    AVG_NS_BUS: Mapped[Float] = mapped_column(Float)
    NS_3_RES: Mapped[Float] = mapped_column(Float)
    NS_3_BUS: Mapped[Float] = mapped_column(Float)
    NS_3_OTH: Mapped[Float] = mapped_column(Float)
    NS_3_6_RES: Mapped[Float] = mapped_column(Float)
    NS_3_6_BUS: Mapped[Float] = mapped_column(Float)
    NS_3_6_OTH: Mapped[Float] = mapped_column(Float)
    NS_6_12_R: Mapped[Float] = mapped_column(Float)
    NS_6_12_B: Mapped[Float] = mapped_column(Float)
    NS_6_12_O: Mapped[Float] = mapped_column(Float)
    NS_12_24_R: Mapped[Float] = mapped_column(Float)
    NS_12_24_B: Mapped[Float] = mapped_column(Float)
    NS_12_24_O: Mapped[Float] = mapped_column(Float)
    NS_24_36_R: Mapped[Float] = mapped_column(Float)
    NS_24_36_B: Mapped[Float] = mapped_column(Float)
    NS_24_36_O: Mapped[Float] = mapped_column(Float)
    NS_36_RES: Mapped[Float] = mapped_column(Float)
    NS_36_BUS: Mapped[Float] = mapped_column(Float)
    NS_36_OTH: Mapped[Float] = mapped_column(Float)
    PQNS_IS_R: Mapped[Float] = mapped_column(Float)
    PQNS_IS_B: Mapped[Float] = mapped_column(Float)
    PQNS_IS_O: Mapped[Float] = mapped_column(Float)
    P_AMS_RES: Mapped[Float] = mapped_column(Float)
    P_AMS_BUS: Mapped[Float] = mapped_column(Float)
    P_AMS_OTH: Mapped[Float] = mapped_column(Float)
    P_RES_VAC: Mapped[Float] = mapped_column(Float)
    P_BUS_VAC: Mapped[Float] = mapped_column(Float)
    P_OTH_VAC: Mapped[Float] = mapped_column(Float)
    P2_RES_VAC: Mapped[Float] = mapped_column(Float)
    P2_BUS_VAC: Mapped[Float] = mapped_column(Float)
    P2_OTH_VAC: Mapped[Float] = mapped_column(Float)
    P_VAC_3_RES: Mapped[Float] = mapped_column(Float)
    P_VAC_3_BUS: Mapped[Float] = mapped_column(Float)
    P_VAC_3_OTH: Mapped[Float] = mapped_column(Float)
    P2_VAC_3_RES: Mapped[Float] = mapped_column(Float)
    P2_VAC_3_BUS: Mapped[Float] = mapped_column(Float)
    P2_VAC_3_OTH: Mapped[Float] = mapped_column(Float)
    P_VAC_3_6_R: Mapped[Float] = mapped_column(Float)
    P_VAC_3_6_B: Mapped[Float] = mapped_column(Float)
    P_VAC_3_6_O: Mapped[Float] = mapped_column(Float)
    P2_VAC_3_6_R: Mapped[Float] = mapped_column(Float)
    P2_VAC_3_6_B: Mapped[Float] = mapped_column(Float)
    P2_VAC_3_6_O: Mapped[Float] = mapped_column(Float)
    P_VAC_6_12R: Mapped[Float] = mapped_column(Float)
    P_VAC_6_12B: Mapped[Float] = mapped_column(Float)
    P_VAC_6_12O: Mapped[Float] = mapped_column(Float)
    P2_VAC_6_12R: Mapped[Float] = mapped_column(Float)
    P2_VAC_6_12B: Mapped[Float] = mapped_column(Float)
    P2_VAC_6_12O: Mapped[Float] = mapped_column(Float)
    P_VAC_12_24R: Mapped[Float] = mapped_column(Float)
    P_VAC_12_24B: Mapped[Float] = mapped_column(Float)
    P_VAC_12_24O: Mapped[Float] = mapped_column(Float)
    P2_VAC_12_24R: Mapped[Float] = mapped_column(Float)
    P2_VAC_12_24B: Mapped[Float] = mapped_column(Float)
    P2_VAC_12_24O: Mapped[Float] = mapped_column(Float)
    P_VAC_24_36R: Mapped[Float] = mapped_column(Float)
    P_VAC_24_36B: Mapped[Float] = mapped_column(Float)
    P_VAC_24_36O: Mapped[Float] = mapped_column(Float)
    P2_VAC_24_36R: Mapped[Float] = mapped_column(Float)
    P2_VAC_24_36B: Mapped[Float] = mapped_column(Float)
    P2_VAC_24_36O: Mapped[Float] = mapped_column(Float)
    P_VAC_36_RES: Mapped[Float] = mapped_column(Float)
    P_VAC_36_BUS: Mapped[Float] = mapped_column(Float)
    P_VAC_36_OTH: Mapped[Float] = mapped_column(Float)
    P2_VAC_36_RES: Mapped[Float] = mapped_column(Float)
    P2_VAC_36_BUS: Mapped[Float] = mapped_column(Float)
    P2_VAC_36_OTH: Mapped[Float] = mapped_column(Float)
    P_PQV_IS_RES: Mapped[Float] = mapped_column(Float)
    P_PQV_IS_BUS: Mapped[Float] = mapped_column(Float)
    P_PQV_IS_OTH: Mapped[Float] = mapped_column(Float)
    P2_PQV_IS_RES: Mapped[Float] = mapped_column(Float)
    P2_PQV_IS_BUS: Mapped[Float] = mapped_column(Float)
    P2_PQV_IS_OTH: Mapped[Float] = mapped_column(Float)
    P_PQV_NS_RES: Mapped[Float] = mapped_column(Float)
    P_PQV_NS_BUS: Mapped[Float] = mapped_column(Float)
    P_PQV_NS_OTH: Mapped[Float] = mapped_column(Float)
    P2_PQV_NS_RES: Mapped[Float] = mapped_column(Float)
    P2_PQV_NS_BUS: Mapped[Float] = mapped_column(Float)
    P2_PQV_NS_OTH: Mapped[Float] = mapped_column(Float)
    P_NOSTAT_RES: Mapped[Float] = mapped_column(Float)
    P_NOSTAT_BUS: Mapped[Float] = mapped_column(Float)
    P_NOSTAT_OTH: Mapped[Float] = mapped_column(Float)
    P2_NOSTAT_RES: Mapped[Float] = mapped_column(Float)
    P2_NOSTAT_BUS: Mapped[Float] = mapped_column(Float)
    P2_NOSTAT_OTH: Mapped[Float] = mapped_column(Float)
    P_NS_3_RES: Mapped[Float] = mapped_column(Float)
    P_NS_3_BUS: Mapped[Float] = mapped_column(Float)
    P_NS_3_OTH: Mapped[Float] = mapped_column(Float)
    P2_NS_3_RES: Mapped[Float] = mapped_column(Float)
    P2_NS_3_BUS: Mapped[Float] = mapped_column(Float)
    P2_NS_3_OTH: Mapped[Float] = mapped_column(Float)
    P_NS_3_6_RES: Mapped[Float] = mapped_column(Float)
    P_NS_3_6_BUS: Mapped[Float] = mapped_column(Float)
    P_NS_3_6_OTH: Mapped[Float] = mapped_column(Float)
    P2_NS_3_6_RES: Mapped[Float] = mapped_column(Float)
    P2_NS_3_6_BUS: Mapped[Float] = mapped_column(Float)
    P2_NS_3_6_OTH: Mapped[Float] = mapped_column(Float)
    P_NS_6_12_R: Mapped[Float] = mapped_column(Float)
    P_NS_6_12_B: Mapped[Float] = mapped_column(Float)
    P_NS_6_12_O: Mapped[Float] = mapped_column(Float)
    P2_NS_6_12_R: Mapped[Float] = mapped_column(Float)
    P2_NS_6_12_B: Mapped[Float] = mapped_column(Float)
    P2_NS_6_12_O: Mapped[Float] = mapped_column(Float)
    P_NS_12_24_R: Mapped[Float] = mapped_column(Float)
    P_NS_12_24_B: Mapped[Float] = mapped_column(Float)
    P_NS_12_24_O: Mapped[Float] = mapped_column(Float)
    P2_NS_12_24_R: Mapped[Float] = mapped_column(Float)
    P2_NS_12_24_B: Mapped[Float] = mapped_column(Float)
    P2_NS_12_24_O: Mapped[Float] = mapped_column(Float)
    P_NS_24_36_R: Mapped[Float] = mapped_column(Float)
    P_NS_24_36_B: Mapped[Float] = mapped_column(Float)
    P_NS_24_36_O: Mapped[Float] = mapped_column(Float)
    P2_NS_24_36_R: Mapped[Float] = mapped_column(Float)
    P2_NS_24_36_B: Mapped[Float] = mapped_column(Float)
    P2_NS_24_36_O: Mapped[Float] = mapped_column(Float)
    P_NS_36_RES: Mapped[Float] = mapped_column(Float)
    P_NS_36_BUS: Mapped[Float] = mapped_column(Float)
    P_NS_36_OTH: Mapped[Float] = mapped_column(Float)
    P2_NS_36_RES: Mapped[Float] = mapped_column(Float)
    P2_NS_36_BUS: Mapped[Float] = mapped_column(Float)
    P2_NS_36_OTH: Mapped[Float] = mapped_column(Float)
    P_PQNS_IS_R: Mapped[Float] = mapped_column(Float)
    P_PQNS_IS_B: Mapped[Float] = mapped_column(Float)
    P_PQNS_IS_O: Mapped[Float] = mapped_column(Float)
    P2_PQNS_IS_R: Mapped[Float] = mapped_column(Float)
    P2_PQNS_IS_B: Mapped[Float] = mapped_column(Float)
    P2_PQNS_IS_O: Mapped[Float] = mapped_column(Float)
    AVG_VAC_O: Mapped[Float] = mapped_column(Float)
    AVG_NS_OTH: Mapped[Float] = mapped_column(Float)


class national_walkability_index(exposome_base):
    __tablename__='NATIONAL_WALKABILITY_INDEX'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR: Mapped[str] =  mapped_column(String(4), primary_key=True)
    WALKABILITY: Mapped[Float] = mapped_column(Float)

class usda_fara(exposome_base):
    __tablename__='USDA_FARA'
    ZIP_9:Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR:Mapped[str] = mapped_column(String(4), primary_key=True)
    LA1AND20:Mapped[Float] = mapped_column(Float, nullable=True)
    LASENIORS20:Mapped[Float] = mapped_column(Float, nullable=True)
    LASENIORSHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISP20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LASNAP20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI05_10:Mapped[Float] = mapped_column(Float, nullable=True)
    LATRACTS10:Mapped[Float] = mapped_column(Float, nullable=True)
    LAKIDS10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOP1SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LATRACTS_HALF:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHUNV1SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAAIAN20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LASNAPHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LABLACKHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LASNAP1SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    TRACTAIAN:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOPHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    TRACTASIAN:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISP1SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LABLACK20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LANHOPI1:Mapped[Float] = mapped_column(Float, nullable=True)
    LATRACTS1:Mapped[Float] = mapped_column(Float, nullable=True)
    LAAIAN10:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISP20:Mapped[Float] = mapped_column(Float, nullable=True)
    TRACTNHOPI:Mapped[Float] = mapped_column(Float, nullable=True)
    LATRACTSVEHICLE_20:Mapped[Float] = mapped_column(Float, nullable=True)
    LASNAP10:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOP10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAAIAN1:Mapped[Float] = mapped_column(Float, nullable=True)
    TRACTHISPANIC:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISP10:Mapped[Float] = mapped_column(Float, nullable=True)
    TRACTLOWI:Mapped[Float] = mapped_column(Float, nullable=True)
    LAWHITEHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHUNV20:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOP1_10:Mapped[Float] = mapped_column(Float, nullable=True)
    URBAN:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOP1_20:Mapped[Float] = mapped_column(Float, nullable=True)
    LAWHITE10:Mapped[Float] = mapped_column(Float, nullable=True)
    LAASIAN10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAWHITE20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAOMULTIR1:Mapped[Float] = mapped_column(Float, nullable=True)
    LABLACK1SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LANHOPI10:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISP1:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOP05_10:Mapped[Float] = mapped_column(Float, nullable=True)
    LASENIORS1:Mapped[Float] = mapped_column(Float, nullable=True)
    LAASIAN20:Mapped[Float] = mapped_column(Float, nullable=True)
    PCTGQTRS:Mapped[Float] = mapped_column(Float, nullable=True)
    LAOMULTIR10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAKIDS10:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOPHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWIHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAAIAN10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISPHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAWHITEHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI20:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LANHOPIHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHUNV1:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHUNVHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    LAOMULTIR20SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LABLACK10SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAWHITE1SHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAKIDSHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAASIANHALFSHARE:Mapped[Float] = mapped_column(Float, nullable=True)
    LAPOP1:Mapped[Float] = mapped_column(Float, nullable=True)
    LAASIAN10:Mapped[Float] = mapped_column(Float, nullable=True)
    LASENIORSHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    POVERTYRATE:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI1_20:Mapped[Float] = mapped_column(Float, nullable=True)
    MEDIANFAMILYINCOME:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI1_10:Mapped[Float] = mapped_column(Float, nullable=True)
    OHU2010:Mapped[Float] = mapped_column(Float, nullable=True)
    LABLACK1:Mapped[Float] = mapped_column(Float, nullable=True)
    TRACTSNAP:Mapped[Float] = mapped_column(Float, nullable=True)
    LA1AND10:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWIHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHISPHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    GROUPQUARTERSFLAG:Mapped[Float] = mapped_column(Float, nullable=True)
    LASNAPHALF:Mapped[Float] = mapped_column(Float, nullable=True)
    LAAIAN20:Mapped[Float] = mapped_column(Float, nullable=True)
    LALOWI1:Mapped[Float] = mapped_column(Float, nullable=True)
    LAHALFAND10:Mapped[Float] = mapped_column(Float)
    LANHOPI20:Mapped[Float] = mapped_column(Float)
    LASENIORS1SHARE:Mapped[Float] = mapped_column(Float)
    LAOMULTIR10:Mapped[Float] = mapped_column(Float)
    LAPOP10:Mapped[Float] = mapped_column(Float)
    LAKIDSHALF:Mapped[Float] = mapped_column(Float)
    LILATRACTS_VEHICLE:Mapped[Float] = mapped_column(Float)
    LAPOP20SHARE:Mapped[Float] = mapped_column(Float)
    LILATRACTS_1AND10:Mapped[Float] = mapped_column(Float)
    LANHOPIHALF:Mapped[Float] = mapped_column(Float)
    LAKIDS20SHARE:Mapped[Float] = mapped_column(Float)
    LANHOPI1SHARE:Mapped[Float] = mapped_column(Float)
    LAHUNV20SHARE:Mapped[Float] = mapped_column(Float)
    LAASIAN20SHARE:Mapped[Float] = mapped_column(Float)
    LAHUNV10SHARE:Mapped[Float] = mapped_column(Float)
    LAOMULTIRHALF:Mapped[Float] = mapped_column(Float)
    LAHUNVHALFSHARE:Mapped[Float] = mapped_column(Float)
    LANHOPI10SHARE:Mapped[Float] = mapped_column(Float)
    TRACTKIDS:Mapped[Float] = mapped_column(Float)
    TRACTBLACK:Mapped[Float] = mapped_column(Float)
    LASENIORS10:Mapped[Float] = mapped_column(Float)
    LASENIORS10SHARE:Mapped[Float] = mapped_column(Float)
    LILATRACTS_1AND20:Mapped[Float] = mapped_column(Float)
    TRACTWHITE:Mapped[Float] = mapped_column(Float)
    LAWHITE10SHARE:Mapped[Float] = mapped_column(Float)
    LILATRACTS_HALFAND10:Mapped[Float] = mapped_column(Float)
    LAKIDS1:Mapped[Float] = mapped_column(Float)
    LASNAP1:Mapped[Float] = mapped_column(Float)
    LAASIAN1SHARE:Mapped[Float] = mapped_column(Float)
    LAHUNV10:Mapped[Float] = mapped_column(Float)
    LALOWI1SHARE:Mapped[Float] = mapped_column(Float)
    LALOWI10:Mapped[Float] = mapped_column(Float)
    TRACTOMULTIR:Mapped[Float] = mapped_column(Float)
    LASNAP20:Mapped[Float] = mapped_column(Float)
    LATRACTS20:Mapped[Float] = mapped_column(Float)
    LAAIAN1SHARE:Mapped[Float] = mapped_column(Float)
    TRACTHUNV:Mapped[Float] = mapped_column(Float)
    LAOMULTIR20:Mapped[Float] = mapped_column(Float)
    LAAIANHALFSHARE:Mapped[Float] = mapped_column(Float)
    LOWINCOMETRACTS:Mapped[Float] = mapped_column(Float)
    LAASIAN1:Mapped[Float] = mapped_column(Float)
    NUMGQTRS:Mapped[Float] = mapped_column(Float)
    LAPOP20:Mapped[Float] = mapped_column(Float)
    LAOMULTIR1SHARE:Mapped[Float] = mapped_column(Float)
    LAAIANHALF:Mapped[Float] = mapped_column(Float)
    LAOMULTIRHALFSHARE:Mapped[Float] = mapped_column(Float)
    LAKIDS1SHARE:Mapped[Float] = mapped_column(Float)
    LABLACK10:Mapped[Float] = mapped_column(Float)
    LAKIDS20:Mapped[Float] = mapped_column(Float)
    LASENIORS20SHARE:Mapped[Float] = mapped_column(Float)
    LAHISP10SHARE:Mapped[Float] = mapped_column(Float)
    LAWHITE1:Mapped[Float] = mapped_column(Float)
    TRACTSENIORS:Mapped[Float] = mapped_column(Float)
    HUNVFLAG:Mapped[Float] = mapped_column(Float)
    LANHOPI20SHARE:Mapped[Float] = mapped_column(Float)
    LASNAP10SHARE:Mapped[Float] = mapped_column(Float)
    LAASIANHALF:Mapped[Float] = mapped_column(Float)
    LAWHITE20:Mapped[Float] = mapped_column(Float)
    LABLACK20:Mapped[Float] = mapped_column(Float)
    LABLACKHALF:Mapped[Float] = mapped_column(Float)

class acs(exposome_base):
    __tablename__='ACS'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)

class cbp(exposome_base):
    __tablename__='CBP'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR: Mapped[str] = mapped_column(String(4), primary_key=True)
    religious: Mapped[Float] = mapped_column(Float)
    civic: Mapped[Float] = mapped_column(Float)
    business: Mapped[Float] = mapped_column(Float)
    political: Mapped[Float] = mapped_column(Float)
    professional: Mapped[Float] = mapped_column(Float)
    labor: Mapped[Float] = mapped_column(Float)
    bowling: Mapped[Float] = mapped_column(Float)
    recreational: Mapped[Float] = mapped_column(Float)
    golf: Mapped[Float] = mapped_column(Float)
    sports: Mapped[Float] = mapped_column(Float)


class ucr(exposome_base):
    __tablename__='UCR'
    ZIP_9: Mapped[str] = mapped_column(String(9), primary_key=True)
    YEAR: Mapped[str] = mapped_column(String(4), primary_key=True)
    p_assault: Mapped[Float] = mapped_column(Float, nullable=True)
    p_burglary: Mapped[Float] = mapped_column(Float, nullable=True)
    p_fso: Mapped[Float] = mapped_column(Float, nullable=True)
    p_larceny: Mapped[Float] = mapped_column(Float, nullable=True)
    p_murder: Mapped[Float] = mapped_column(Float, nullable=True)
    p_mvt: Mapped[Float] = mapped_column(Float, nullable=True)
    p_rob: Mapped[Float] = mapped_column(Float, nullable=True)