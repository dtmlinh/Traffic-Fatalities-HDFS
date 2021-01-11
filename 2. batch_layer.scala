// Name: Linh Dinh

// DROP OLD TABLES IN HIVE (HQL)

beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver

drop table ldinh_fars_crss;
drop table ldinh_hosp_5_mi_state;
drop table ldinh_accidents_clean;
drop table ldinh_fatalities_clean;
drop table ldinh_gov_spend_clean;
drop table ldinh_accidents_master;
drop table ldinh_fatalities_master;



// SPARK QUERIES

spark-shell --conf spark.hadoop.metastore.catalog.default=hive

import org.apache.spark.sql.SaveMode



val fars_crss = spark.sql("""SELECT
VE_TOTAL, VE_FORMS, PVH_INVL, PEDS, PERMVIT, PERNOTMVIT, MONTH, YEAR,
DAY_WEEK, HOUR, HARM_EV, MAN_COLL, RELJCT1, RELJCT2, TYP_INT, WRK_ZONE,
REL_ROAD, LGT_COND, WEATHER, SCH_BUS, CF1, CF2, CF3, 1 as FATAL_FLAG
FROM ldinh_fatalities
UNION
SELECT
VE_TOTAL, VE_FORMS, PVH_INVL, PEDS, PERMVIT, PERNOTMVIT, MONTH, YEAR,
WKDY_IM AS DAY_WEEK, HOUR_IM AS HOUR, EVENT1_IM AS HARM_EV, MANCOL_IM AS MAN_COLL,
RELJCT1_IM AS RELJCT1, RELJCT2_IM AS RELJCT2, TYP_INT, WRK_ZONE,
REL_ROAD, LGTCON_IM AS LGT_COND, WEATHR_IM AS WEATHER, SCH_BUS, CF1, CF2, CF3, 0 as FATAL_FLAG
FROM ldinh_non_fatalities;""")
fars_crss.createOrReplaceTempView("fars_crss")
fars_crss.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_fars_crss")



val hosp_5_mi = spark.sql("""SELECT upper(state) as state, year, avg(hosp_5_mi) as avg_hosp_5_mi
FROM (SELECT a.state, cast(from_unixtime(unix_timestamp(a.start_time), 'yyyy') as smallint) year,
a.id, count(distinct h.id) hosp_5_mi
FROM ldinh_accidents a
LEFT JOIN ldinh_hospitals h 
ON upper(a.state) = upper(h.state)
AND upper(h.status) = 'OPEN'
AND ((a.start_lat - h.latitude) BETWEEN -5/69 AND 5/69)
AND ((a.start_lng - h.longitude) BETWEEN -5/54.6 AND 5/54.6)
GROUP BY a.state, year, a.id) t
GROUP BY state, year;""")
hosp_5_mi.createOrReplaceTempView("hosp_5_mi")
hosp_5_mi.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_hosp_5_mi_state")



val accidents_clean = spark.sql("""SELECT upper(a.state) as state, upper(a.county) as county,
cast(from_unixtime(unix_timestamp(start_time), 'm') as smallint) minute, 
cast(from_unixtime(unix_timestamp(start_time), 'k') as smallint) hour, 
cast(from_unixtime(unix_timestamp(start_time), 'dd') as smallint) day,
from_unixtime(unix_timestamp(start_time), 'EEEE') day_week, 
cast(from_unixtime(unix_timestamp(start_time), 'MM') as smallint) month,
cast(from_unixtime(unix_timestamp(start_time), 'yyyy') as smallint) year,
CASE WHEN sunrise_sunset = '' THEN 'Unknown' 
ELSE sunrise_sunset END as light_condition,
CASE WHEN (lower(weather_condition) like '%clear%') THEN 'Clear'
WHEN (lower(weather_condition) like '%rain%') THEN 'Rain'
WHEN (lower(weather_condition) like '%snow%') THEN 'Snow'
WHEN (lower(weather_condition) like '%cloud%') THEN 'Cloudy'
WHEN (lower(weather_condition) like '%fog%' OR lower(weather_condition) like '%smog%' OR lower(weather_condition) like '%smoke%') THEN 'Fog'
WHEN (lower(weather_condition) like '%hail%' OR lower(weather_condition) like '%sleet%') THEN 'Hail'
WHEN (weather_condition = '' OR lower(weather_condition) = 'unknown') THEN 'Unknown'
ELSE 'Other' END as weather,
CASE WHEN junction = false then 'No' ELSE 'Yes' END as junction,
1 as ct
FROM ldinh_accidents a;""")
accidents_clean.createOrReplaceTempView("accidents_clean")
accidents_clean.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_accidents_clean")



val fatalities_clean = spark.sql("""SELECT upper(g.state_name_abr) as state, upper(g.county_name) as county, minute, hour, day,
CASE WHEN day_week == 1 THEN 'Sunday'
WHEN day_week == 2 THEN 'Monday'
WHEN day_week == 3 THEN 'Tuesday'
WHEN day_week == 4 THEN 'Wednesday'
WHEN day_week == 5 THEN 'Thursday'
WHEN day_week == 6 THEN 'Friday'
WHEN day_week == 7 THEN 'Saturday'
ELSE 'Unknown' END as day_week, month, year,
CASE WHEN lgt_cond IN (1,4) THEN 'Day'
WHEN (lgt_cond) IN (2,3,5,6) THEN 'Night'
ELSE 'Unknown' END as light_condition,
CASE WHEN weather IN (1) THEN 'Clear'
WHEN weather IN (2,12) THEN 'Rain'
WHEN weather IN (4,11) THEN 'Snow'
WHEN weather IN (10) THEN 'Cloudy'
WHEN weather IN (5) THEN 'Fog'
WHEN weather IN (3) THEN 'Hail'
WHEN weather IN (0,98,99) THEN 'Unknown'
ELSE 'Other' END as weather,
CASE WHEN reljct1 = 0 then 'No' 
WHEN reljct1 = 1 then 'Yes'
ELSE 'Unknown' END as junction,
CASE WHEN (hosp_hr > hour) AND (hosp_hr BETWEEN 0 and 24) THEN round(((hosp_hr + hosp_mn/60) - (hour + minute/60)) * 60, 0)
WHEN (hosp_hr = hour AND hosp_mn >= minute) AND (hosp_hr BETWEEN 0 and 24) THEN round((hosp_mn - minute), 0)
ELSE NULL END as hosp_arr_mn,
1 as ct
FROM ldinh_fatalities f
LEFT JOIN 
(select distinct county_code, county_name, state_code, state_name_abr FROM ldinh_geo_mapping) g
ON f.county = g.county_code
AND f.state = g.state_code;""")
fatalities_clean.createOrReplaceTempView("fatalities_clean")
fatalities_clean.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_fatalities_clean")



val gov_spend_clean = spark.sql("""SELECT upper(g.state_name_abr) as state, t1.category, t1.year, t1.cat_spending, t2.tot_spending 
FROM (SELECT upper(name) as name, year, 
CASE WHEN lower(agg_desc_label) = lower('General Expenditure, by Function: - Hospitals') THEN 'Hospitals Spending'
WHEN lower(agg_desc_label) = lower('General Expenditure, by Function: - Health') THEN 'Health Spending'
WHEN lower(agg_desc_label) = lower('General Expenditure, by Function: - Highways') THEN 'Highways Spending'
END AS category, sum(amount) as cat_spending
from ldinh_gov_spending
WHERE lower(agg_desc_label) IN (lower('General Expenditure, by Function: - Hospitals'),
lower('General Expenditure, by Function: - Health'), lower('General Expenditure, by Function: - Highways'))
GROUP BY name, year, agg_desc_label) t1
JOIN
(SELECT upper(name) as name, year, lower(agg_desc_label) AS category, sum(amount) as tot_spending
from ldinh_gov_spending
WHERE lower(agg_desc_label) IN (lower('Total Expenditure'))
GROUP BY name, year, agg_desc_label) t2 
ON t1.name = t2.name
AND t1.year = t2.year
JOIN (SELECT DISTINCT upper(state_name) as state_name,
upper(state_name_abr) as state_name_abr
FROM ldinh_geo_mapping) g
ON t1.name = g.state_name
ORDER BY state, category, year;""")
gov_spend_clean.createOrReplaceTempView("gov_spend_clean")
gov_spend_clean.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_gov_spend_clean")



val accidents_master = spark.sql("""SELECT t1.*, t2.avg_hosp_arr_mn, t3.avg_hosp_5_mi, t4.tot, t4.hea, t4.hos, t4.hig
FROM (SELECT state, year, count(1) tot_accidents, 
count(if(light_condition = 'Day', 1, null)) daytime, 
count(if(light_condition = 'Night', 1, null)) nightime,
count(if(weather = 'Clear', 1, null)) clear, 
count(if(weather = 'Rain', 1, null)) rain, 
count(if(weather = 'Snow', 1, null)) snow, 
count(if(weather = 'Cloudy', 1, null)) cloudy, 
count(if(weather = 'Fog', 1, null)) fog, 
count(if(weather = 'Hail', 1, null)) hail, 
count(if(junction = 'No', 1, null)) junction_no, 
count(if(junction = 'Yes', 1, null)) junction_yes,
count(if(day_week = 'Monday', 1, null)) monday,
count(if(day_week = 'Tuesday', 1, null)) tuesday,
count(if(day_week = 'Wednesday', 1, null)) wednesday,
count(if(day_week = 'Thursday', 1, null)) thursday,
count(if(day_week = 'Friday', 1, null)) friday,
count(if(day_week = 'Saturday', 1, null)) saturday,
count(if(day_week = 'Sunday', 1, null)) sunday,
count(if(hour = 1, 1, null)) hour_1,
count(if(hour = 2, 1, null)) hour_2,
count(if(hour = 3, 1, null)) hour_3,
count(if(hour = 4, 1, null)) hour_4,
count(if(hour = 5, 1, null)) hour_5,
count(if(hour = 6, 1, null)) hour_6,
count(if(hour = 7, 1, null)) hour_7,
count(if(hour = 8, 1, null)) hour_8,
count(if(hour = 9, 1, null)) hour_9,
count(if(hour = 10, 1, null)) hour_10,
count(if(hour = 11, 1, null)) hour_11,
count(if(hour = 12, 1, null)) hour_12,
count(if(hour = 13, 1, null)) hour_13,
count(if(hour = 14, 1, null)) hour_14,
count(if(hour = 15, 1, null)) hour_15,
count(if(hour = 16, 1, null)) hour_16,
count(if(hour = 17, 1, null)) hour_17,
count(if(hour = 18, 1, null)) hour_18,
count(if(hour = 19, 1, null)) hour_19,
count(if(hour = 20, 1, null)) hour_20,
count(if(hour = 21, 1, null)) hour_21,
count(if(hour = 22, 1, null)) hour_22,
count(if(hour = 23, 1, null)) hour_23,
count(if(hour = 24, 1, null)) hour_24,
count(if(month = 1, 1, null)) month_1,
count(if(month = 2, 1, null)) month_2,
count(if(month = 3, 1, null)) month_3,
count(if(month = 4, 1, null)) month_4,
count(if(month = 5, 1, null)) month_5,
count(if(month = 6, 1, null)) month_6,
count(if(month = 7, 1, null)) month_7,
count(if(month = 8, 1, null)) month_8,
count(if(month = 9, 1, null)) month_9,
count(if(month = 10, 1, null)) month_10,
count(if(month = 11, 1, null)) month_11,
count(if(month = 12, 1, null)) month_12
FROM ldinh_accidents_clean
WHERE year != 2016 OR (year = 2016 AND month IN (7,8,9,10,11,12))
GROUP BY state, year) t1
LEFT JOIN 
(SELECT state, year, avg(hosp_arr_mn) avg_hosp_arr_mn
FROM ldinh_fatalities_clean
WHERE year != 2016 OR (year = 2016 AND month IN (7,8,9,10,11,12))
GROUP BY state, year) t2
ON t1.state = t2.state
AND t1.year = t2.year
LEFT JOIN ldinh_hosp_5_mi_state t3
ON t1.state = t3.state 
AND t1.year = t3.year
LEFT JOIN 
(SELECT state, year,
avg(tot_spending) tot, 
sum(if(category = 'Health Spending', cat_spending, 0)) hea, 
sum(if(category = 'Hospitals Spending', cat_spending, 0)) hos, 
sum(if(category = 'Highways Spending', cat_spending, 0)) hig
FROM ldinh_gov_spend_clean 
GROUP BY state, year) t4
ON t1.state = t4.state
AND t1.year = t4.year;""")
accidents_master.createOrReplaceTempView("accidents_master")
accidents_master.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_accidents_master")



val fatalities_master = spark.sql("""SELECT t1.*, t2.avg_hosp_5_mi, t3.tot, t3.hea, t3.hos, t3.hig
FROM (SELECT state, year, count(1) tot_accidents, 
count(if(light_condition = 'Day', 1, null)) daytime, 
count(if(light_condition = 'Night', 1, null)) nightime,
count(if(weather = 'Clear', 1, null)) clear, 
count(if(weather = 'Rain', 1, null)) rain, 
count(if(weather = 'Snow', 1, null)) snow, 
count(if(weather = 'Cloudy', 1, null)) cloudy, 
count(if(weather = 'Fog', 1, null)) fog, 
count(if(weather = 'Hail', 1, null)) hail, 
count(if(junction = 'No', 1, null)) junction_no, 
count(if(junction = 'Yes', 1, null)) junction_yes,
count(if(day_week = 'Monday', 1, null)) monday,
count(if(day_week = 'Tuesday', 1, null)) tuesday,
count(if(day_week = 'Wednesday', 1, null)) wednesday,
count(if(day_week = 'Thursday', 1, null)) thursday,
count(if(day_week = 'Friday', 1, null)) friday,
count(if(day_week = 'Saturday', 1, null)) saturday,
count(if(day_week = 'Sunday', 1, null)) sunday,
count(if(hour = 1, 1, null)) hour_1,
count(if(hour = 2, 1, null)) hour_2,
count(if(hour = 3, 1, null)) hour_3,
count(if(hour = 4, 1, null)) hour_4,
count(if(hour = 5, 1, null)) hour_5,
count(if(hour = 6, 1, null)) hour_6,
count(if(hour = 7, 1, null)) hour_7,
count(if(hour = 8, 1, null)) hour_8,
count(if(hour = 9, 1, null)) hour_9,
count(if(hour = 10, 1, null)) hour_10,
count(if(hour = 11, 1, null)) hour_11,
count(if(hour = 12, 1, null)) hour_12,
count(if(hour = 13, 1, null)) hour_13,
count(if(hour = 14, 1, null)) hour_14,
count(if(hour = 15, 1, null)) hour_15,
count(if(hour = 16, 1, null)) hour_16,
count(if(hour = 17, 1, null)) hour_17,
count(if(hour = 18, 1, null)) hour_18,
count(if(hour = 19, 1, null)) hour_19,
count(if(hour = 20, 1, null)) hour_20,
count(if(hour = 21, 1, null)) hour_21,
count(if(hour = 22, 1, null)) hour_22,
count(if(hour = 23, 1, null)) hour_23,
count(if(hour = 24, 1, null)) hour_24,
count(if(month = 1, 1, null)) month_1,
count(if(month = 2, 1, null)) month_2,
count(if(month = 3, 1, null)) month_3,
count(if(month = 4, 1, null)) month_4,
count(if(month = 5, 1, null)) month_5,
count(if(month = 6, 1, null)) month_6,
count(if(month = 7, 1, null)) month_7,
count(if(month = 8, 1, null)) month_8,
count(if(month = 9, 1, null)) month_9,
count(if(month = 10, 1, null)) month_10,
count(if(month = 11, 1, null)) month_11,
count(if(month = 12, 1, null)) month_12,
avg(hosp_arr_mn) avg_hosp_arr_mn
FROM ldinh_fatalities_clean
WHERE year != 2016 OR (year = 2016 AND month IN (7,8,9,10,11,12))
GROUP BY state, year) t1
LEFT JOIN ldinh_hosp_5_mi_state t2
ON t1.state = t2.state 
AND t1.year = t2.year
LEFT JOIN 
(SELECT state, year,
avg(tot_spending) tot, 
sum(if(category = 'Health Spending', cat_spending, 0)) hea, 
sum(if(category = 'Hospitals Spending', cat_spending, 0)) hos, 
sum(if(category = 'Highways Spending', cat_spending, 0)) hig
FROM ldinh_gov_spend_clean 
GROUP BY state, year) t3
ON t1.state = t3.state
AND t1.year = t3.year;""")
fatalities_master.createOrReplaceTempView("fatalities_master")
fatalities_master.write.mode(SaveMode.Overwrite).saveAsTable("ldinh_fatalities_master")


