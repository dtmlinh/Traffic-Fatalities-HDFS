Name: Linh Dinh

'''CREATE GROUND TRUTH HIVE TABLES'''
'''to start hive: beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver'''


'''ACCIDENTS TABLES'''

-- Create external table
create external table ldinh_accidents_csv(
ID string,
Source string,
TMC double,
Severity smallint,
Start_Time timestamp,
End_Time timestamp,
Start_Lat double,
Start_Lng double,
End_Lat double,
End_Lng double,
Distance double,
Description string,
Number double,
Street string,
Side string,
City string,
County string,
State string,
Zipcode string,
Country string,
Timezone string,
Airport_Code string,
Weather_Timestamp timestamp,
Temperature double,
Wind_Chill double,
Humidity double,
Pressure double,
Visibility double,
Wind_Direction string,
Wind_Speed double,
Precipitation double,
Weather_Condition string,
Amenity boolean,
Bump boolean,
Crossing boolean,
Give_Way boolean,
Junction boolean,
No_Exit boolean,
Railway boolean,
Roundabout boolean,
Station boolean,
Stop boolean,
Traffic_Calming boolean,
Traffic_Signal boolean,
Turning_Loop boolean,
Sunrise_Sunset string,
Civil_Twilight string,
Nautical_Twilight string,
Astronomical_Twilight string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/traffic-fatalities-hdfs/accidents/accidents'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select ID, Severity, Start_Time, Start_Lat, Distance, City, State, Zipcode, Temperature, Pressure, Give_Way, Sunrise_Sunset from ldinh_accidents_csv limit 5;

-- Create an ORC table for the same data
create table ldinh_accidents(
ID string,
Source string,
TMC double,
Severity smallint,
Start_Time timestamp,
End_Time timestamp,
Start_Lat double,
Start_Lng double,
End_Lat double,
End_Lng double,
Distance double,
Description string,
Number double,
Street string,
Side string,
City string,
County string,
State string,
Zipcode string,
Country string,
Timezone string,
Airport_Code string,
Weather_Timestamp timestamp,
Temperature double,
Wind_Chill double,
Humidity double,
Pressure double,
Visibility double,
Wind_Direction string,
Wind_Speed double,
Precipitation double,
Weather_Condition string,
Amenity boolean,
Bump boolean,
Crossing boolean,
Give_Way boolean,
Junction boolean,
No_Exit boolean,
Railway boolean,
Roundabout boolean,
Station boolean,
Stop boolean,
Traffic_Calming boolean,
Traffic_Signal boolean,
Turning_Loop boolean,
Sunrise_Sunset string,
Civil_Twilight string,
Nautical_Twilight string,
Astronomical_Twilight string)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ldinh_accidents select * from ldinh_accidents_csv;

-- Test the ORC table
select ID, Severity, Start_Time, Start_Lat, Distance, City, State, Zipcode, Temperature, Pressure, Give_Way, Sunrise_Sunset from ldinh_accidents limit 5;


'''FATALITIES TABLES'''

-- Create external table
create external table ldinh_fatalities_csv(
STATE smallint,
STATENAME string,
ST_CASE bigint, 
VE_TOTAL smallint,
VE_FORMS smallint, 
PVH_INVL smallint,
PEDS smallint,
PERSONS smallint,
PERMVIT smallint,
PERNOTMVIT smallint,
COUNTY int, 
COUNTYNAME string,
CITY int,
CITYNAME string,
DAY smallint,
DAYNAME string, 
MONTH smallint,
MONTHNAME string,
YEAR smallint,
DAY_WEEK smallint,
DAY_WEEKNAME string,
HOUR smallint,
HOURNAME string,
MINUTE smallint,
MINUTENAME string,
NHS smallint,
NHSNAME string,
ROUTE smallint,
ROUTENAME string, 
TWAY_ID string,
TWAY_ID2 string,
RUR_URB smallint,
RUR_URBNAME string,
FUNC_SYS smallint, 
FUNC_SYSNAME string,
RD_OWNER smallint,
RD_OWNERNAME string, 
MILEPT double,
MILEPTNAME string,
LATITUDE double,
LATITUDENAME double,
LONGITUD double, 
LONGITUDNAME double,
SP_JUR smallint,
SP_JURNAME string,
HARM_EV smallint, 
HARM_EVNAME string,
MAN_COLL smallint,
MAN_COLLNAME string,
RELJCT1 smallint,
RELJCT1NAME string,
RELJCT2 smallint,
RELJCT2NAME string,
TYP_INT smallint,
TYP_INTNAME string,
WRK_ZONE smallint,
WRK_ZONENAME string,
REL_ROAD smallint,
REL_ROADNAME string,
LGT_COND smallint, 
LGT_CONDNAME string,
WEATHER1 smallint, 
WEATHER1NAME string,
WEATHER2 smallint, 
WEATHER2NAME string,
WEATHER smallint,
WEATHERNAME string,
SCH_BUS smallint,
SCH_BUSNAME string, 
RAIL string,
RAILNAME string,
NOT_HOUR smallint,
NOT_HOURNAME string,
NOT_MIN smallint, 
NOT_MINNAME string, 
ARR_HOUR smallint,
ARR_HOURNAME string,
ARR_MIN smallint,
ARR_MINNAME string,
HOSP_HR smallint,
HOSP_HRNAME string,
HOSP_MN smallint,
HOSP_MNNAME string,
CF1 smallint,
CF1NAME string,
CF2 smallint, 
CF2NAME string,
CF3 smallint,
CF3NAME string,
FATALS smallint,
DRUNK_DR smallint)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/traffic-fatalities-hdfs/FARS'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select STATE, VE_TOTAL, PERSONS, COUNTY, CITY, DAY, HOUR, MINUTE, MILEPT, LATITUDE, LONGITUD, REL_ROAD, WEATHER1, RAIL, ARR_MIN, HOSP_HR, FATALS from ldinh_fatalities_csv limit 5;

-- Create an ORC table for the same data
create table ldinh_fatalities(
STATE smallint,
STATENAME string,
ST_CASE bigint, 
VE_TOTAL smallint,
VE_FORMS smallint, 
PVH_INVL smallint,
PEDS smallint,
PERSONS smallint,
PERMVIT smallint,
PERNOTMVIT smallint,
COUNTY int, 
COUNTYNAME string,
CITY int,
CITYNAME string,
DAY smallint,
DAYNAME string, 
MONTH smallint,
MONTHNAME string,
YEAR smallint,
DAY_WEEK smallint,
DAY_WEEKNAME string,
HOUR smallint,
HOURNAME string,
MINUTE smallint,
MINUTENAME string,
NHS smallint,
NHSNAME string,
ROUTE smallint,
ROUTENAME string, 
TWAY_ID string,
TWAY_ID2 string,
RUR_URB smallint,
RUR_URBNAME string,
FUNC_SYS smallint, 
FUNC_SYSNAME string,
RD_OWNER smallint,
RD_OWNERNAME string, 
MILEPT double,
MILEPTNAME string,
LATITUDE double,
LATITUDENAME double,
LONGITUD double, 
LONGITUDNAME double,
SP_JUR smallint,
SP_JURNAME string,
HARM_EV smallint, 
HARM_EVNAME string,
MAN_COLL smallint,
MAN_COLLNAME string,
RELJCT1 smallint,
RELJCT1NAME string,
RELJCT2 smallint,
RELJCT2NAME string,
TYP_INT smallint,
TYP_INTNAME string,
WRK_ZONE smallint,
WRK_ZONENAME string,
REL_ROAD smallint,
REL_ROADNAME string,
LGT_COND smallint, 
LGT_CONDNAME string,
WEATHER1 smallint, 
WEATHER1NAME string,
WEATHER2 smallint, 
WEATHER2NAME string,
WEATHER smallint,
WEATHERNAME string,
SCH_BUS smallint,
SCH_BUSNAME string, 
RAIL string,
RAILNAME string,
NOT_HOUR smallint,
NOT_HOURNAME string,
NOT_MIN smallint, 
NOT_MINNAME string, 
ARR_HOUR smallint,
ARR_HOURNAME string,
ARR_MIN smallint,
ARR_MINNAME string,
HOSP_HR smallint,
HOSP_HRNAME string,
HOSP_MN smallint,
HOSP_MNNAME string,
CF1 smallint,
CF1NAME string,
CF2 smallint, 
CF2NAME string,
CF3 smallint,
CF3NAME string,
FATALS smallint,
DRUNK_DR smallint)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ldinh_fatalities select * from ldinh_fatalities_csv;

-- Test the ORC table
select STATE, VE_TOTAL, PERSONS, COUNTY, CITY, DAY, HOUR, MINUTE, MILEPT, LATITUDE, LONGITUD, REL_ROAD, WEATHER1, RAIL, ARR_MIN, HOSP_HR, FATALS from ldinh_fatalities limit 5;


'''NON-FATALITIES TABLES'''

-- Create external table
create external table ldinh_non_fatalities_csv(
CASENUM bigint, 
REGION smallint, 
PSU smallint,
PJ smallint,
PSU_VAR smallint,
URBANICITY smallint,
STRATUM smallint,
VE_TOTAL smallint, 
VE_FORMS smallint, 
PVH_INVL smallint, 
PEDS smallint, 
PERMVIT smallint, 
PERNOTMVIT smallint, 
NUM_INJ smallint, 
MONTH smallint, 
YEAR smallint, 
DAY_WEEK smallint, 
HOUR smallint, 
MINUTE smallint, 
HARM_EV smallint, 
ALCOHOL smallint,
MAX_SEV smallint,
MAN_COLL smallint, 
RELJCT1 smallint, 
RELJCT2 smallint, 
TYP_INT smallint, 
WRK_ZONE smallint, 
REL_ROAD smallint, 
LGT_COND smallint, 
WEATHER1 smallint, 
WEATHER2 smallint, 
WEATHER smallint, 
SCH_BUS smallint, 
INT_HWY smallint, 
CF1 smallint, 
CF2 smallint, 
CF3 smallint, 
WKDY_IM smallint,
HOUR_IM smallint,
MINUTE_IM smallint,
EVENT1_IM smallint,
MANCOL_IM smallint,
RELJCT1_IM smallint, 
RELJCT2_IM smallint, 
LGTCON_IM smallint, 
WEATHR_IM smallint, 
MAXSEV_IM smallint, 
NO_INJ_IM smallint, 
ALCHL_IM smallint,
PSUSTRAT smallint, 
WEIGHT double)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/traffic-fatalities-hdfs/CRSS'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select REGION, VE_TOTAL, HOUR, HOUR_IM, MINUTE, REL_ROAD, WEATHER, WEATHR_IM, CF1, CF2, CF3 from ldinh_non_fatalities_csv limit 5;

-- Create an ORC table for the same data
create table ldinh_non_fatalities(
CASENUM bigint, 
REGION smallint, 
PSU smallint,
PJ smallint,
PSU_VAR smallint,
URBANICITY smallint,
STRATUM smallint,
VE_TOTAL smallint, 
VE_FORMS smallint, 
PVH_INVL smallint, 
PEDS smallint, 
PERMVIT smallint, 
PERNOTMVIT smallint, 
NUM_INJ smallint, 
MONTH smallint, 
YEAR smallint, 
DAY_WEEK smallint, 
HOUR smallint, 
MINUTE smallint, 
HARM_EV smallint, 
ALCOHOL smallint,
MAX_SEV smallint,
MAN_COLL smallint, 
RELJCT1 smallint, 
RELJCT2 smallint, 
TYP_INT smallint, 
WRK_ZONE smallint, 
REL_ROAD smallint, 
LGT_COND smallint, 
WEATHER1 smallint, 
WEATHER2 smallint, 
WEATHER smallint, 
SCH_BUS smallint, 
INT_HWY smallint, 
CF1 smallint, 
CF2 smallint, 
CF3 smallint, 
WKDY_IM smallint,
HOUR_IM smallint,
MINUTE_IM smallint,
EVENT1_IM smallint,
MANCOL_IM smallint,
RELJCT1_IM smallint, 
RELJCT2_IM smallint, 
LGTCON_IM smallint, 
WEATHR_IM smallint, 
MAXSEV_IM smallint, 
NO_INJ_IM smallint, 
ALCHL_IM smallint,
PSUSTRAT smallint, 
WEIGHT double)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ldinh_non_fatalities select * from ldinh_non_fatalities_csv;

-- Test the ORC table
select REGION, VE_TOTAL, HOUR, HOUR_IM, MINUTE, REL_ROAD, WEATHER, WEATHR_IM, CF1, CF2, CF3 from ldinh_non_fatalities limit 5;


'''GEO_MAPPING'''

-- Create external table
create external table ldinh_geo_mapping_csv(
Territory string,
State_Name string,
State_Name_Abr string,
State_Code smallint,
City_Code smallint,
City_Name string,
County_Code int,
County_Name string,
Country_Code smallint,
Old_City_Name string,
Date_Record_Added string,
Duty_Station_Code bigint)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/traffic-fatalities-hdfs/GEO'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select * from ldinh_geo_mapping_csv limit 5;

-- Create an ORC table for the same data
create table ldinh_geo_mapping(
Territory string,
State_Name string,
State_Name_Abr string,
State_Code smallint,
City_Code smallint,
City_Name string,
County_Code int,
County_Name string,
Country_Code smallint,
Old_City_Name string,
Date_Record_Added string,
Duty_Station_Code bigint)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ldinh_geo_mapping select * from ldinh_geo_mapping_csv;

-- Test the ORC table
select * from ldinh_geo_mapping limit 5;


'''HOSPITALS'''

-- Create external table
create external table ldinh_hospitals_csv(
X double,
Y double,
OBJECTID smallint,
ID bigint,
NAME string,
ADDRESS string,
CITY string,
STATE string,
ZIP string,
ZIP4 string,
TELEPHONE string,
TYPE string,
STATUS string,
POPULATION double,
COUNTY string,
COUNTYFIPS int,
COUNTRY string,
LATITUDE double,
LONGITUDE double,
NAICS_CODE int,
NAICS_DESC string,
SOURCE string,
SOURCEDATE timestamp, 
VAL_METHOD string,
VAL_DATE timestamp,
WEBSITE string,
STATE_ID string,
ALT_NAME string,
ST_FIPS smallint,
OWNER string,
TTL_STAFF int,
BEDS int,
TRAUMA string,
HELIPAD string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/traffic-fatalities-hdfs/Hospitals/Hospitals'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select X, Y, NAME, CITY, STATE, ZIP, COUNTYFIPS, LATITUDE, LONGITUDE, ST_FIPS, BEDS from ldinh_hospitals_csv limit 5;

-- Create an ORC table for the same data
create table ldinh_hospitals(
X double,
Y double,
OBJECTID smallint,
ID bigint,
NAME string,
ADDRESS string,
CITY string,
STATE string,
ZIP string,
ZIP4 string,
TELEPHONE string,
TYPE string,
STATUS string,
POPULATION double,
COUNTY string,
COUNTYFIPS int,
COUNTRY string,
LATITUDE double,
LONGITUDE double,
NAICS_CODE int,
NAICS_DESC string,
SOURCE string,
SOURCEDATE timestamp, 
VAL_METHOD string,
VAL_DATE timestamp,
WEBSITE string,
STATE_ID string,
ALT_NAME string,
ST_FIPS smallint,
OWNER string,
TTL_STAFF int,
BEDS int,
TRAUMA string,
HELIPAD string)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ldinh_hospitals select * from ldinh_hospitals_csv;

-- Test the ORC table
select X, Y, NAME, CITY, STATE, ZIP, COUNTYFIPS, LATITUDE, LONGITUDE, ST_FIPS, BEDS from ldinh_hospitals limit 5;


'''GOV SPENDING'''

-- Create external table
create external table ldinh_gov_spending_csv(
SVY_COMP smallint,
SVY_COMP_LABEL string,
GEO_ID string,
NAME string, 
Year smallint,
AGG_DESC string,
AGG_DESC_LABEL string,
GOVTYPE smallint,
GOVTYPE_LABEL string,
AMOUNT double,
AMOUNT_FORMATTED double)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/traffic-fatalities-hdfs/gov_spending'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select * from ldinh_gov_spending_csv limit 5;

-- Create an ORC table for the same data
create table ldinh_gov_spending(
SVY_COMP smallint,
SVY_COMP_LABEL string,
GEO_ID string,
NAME string, 
Year smallint,
AGG_DESC string,
AGG_DESC_LABEL string,
GOVTYPE smallint,
GOVTYPE_LABEL string,
AMOUNT double,
AMOUNT_FORMATTED double)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ldinh_gov_spending select * from ldinh_gov_spending_csv;

-- Test the ORC table
select * from ldinh_gov_spending limit 5;

