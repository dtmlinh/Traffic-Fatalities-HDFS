-- Name: Linh Dinh

-- Hive queries to drop existing tables

beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver
drop table ldinh_accidents_conditions_by_state;
drop table ldinh_state;



-- Hbase queries to drop existing table

hbase shell
disable 'ldinh_accidents_conditions_by_state'
drop 'ldinh_accidents_conditions_by_state'
disable 'ldinh_state'
drop 'ldinh_state'



-- Create new tables

-- Hbase queries

create 'ldinh_accidents_conditions_by_state', 'acc'
create 'ldinh_state', 'st'



-- Hive queries

beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver

create external table ldinh_state (state string, state_dup string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,st:state_dup')
TBLPROPERTIES ('hbase.table.name' = 'ldinh_state');

insert overwrite table ldinh_state
select distinct state, state from ldinh_accidents_master; 



create external table ldinh_accidents_conditions_by_state (
  location string, tot_acc bigint, tot_fat bigint,
  daytime_acc bigint, daytime_fat bigint,
  nightime_acc bigint, nightime_fat bigint,
  clear_acc bigint, clear_fat bigint,
  rain_acc bigint, rain_fat bigint,
  snow_acc bigint, snow_fat bigint,
  cloudy_acc bigint, cloudy_fat bigint,
  fog_acc bigint, fog_fat bigint,
  hail_acc bigint, hail_fat bigint,
  junction_acc bigint, junction_fat bigint,
  mon_acc bigint, mon_fat bigint,
  tue_acc bigint, tue_fat bigint,
  wed_acc bigint, wed_fat bigint,
  thu_acc bigint, thu_fat bigint,
  fri_acc bigint, fri_fat bigint,
  sat_acc bigint, sat_fat bigint,
  sun_acc bigint, sun_fat bigint,
  hour_1_acc bigint, hour_1_fat bigint,
  hour_2_acc bigint, hour_2_fat bigint,
  hour_3_acc bigint, hour_3_fat bigint,
  hour_4_acc bigint, hour_4_fat bigint,
  hour_5_acc bigint, hour_5_fat bigint,
  hour_6_acc bigint, hour_6_fat bigint,
  hour_7_acc bigint, hour_7_fat bigint,
  hour_8_acc bigint, hour_8_fat bigint,
  hour_9_acc bigint, hour_9_fat bigint,
  hour_10_acc bigint, hour_10_fat bigint,
  hour_11_acc bigint, hour_11_fat bigint,
  hour_12_acc bigint, hour_12_fat bigint,
  hour_13_acc bigint, hour_13_fat bigint,
  hour_14_acc bigint, hour_14_fat bigint,
  hour_15_acc bigint, hour_15_fat bigint,
  hour_16_acc bigint, hour_16_fat bigint,
  hour_17_acc bigint, hour_17_fat bigint,
  hour_18_acc bigint, hour_18_fat bigint,
  hour_19_acc bigint, hour_19_fat bigint,
  hour_20_acc bigint, hour_20_fat bigint,
  hour_21_acc bigint, hour_21_fat bigint,
  hour_22_acc bigint, hour_22_fat bigint,
  hour_23_acc bigint, hour_23_fat bigint,
  hour_24_acc bigint, hour_24_fat bigint,
  month_1_acc bigint, month_1_fat bigint,
  month_2_acc bigint, month_2_fat bigint,
  month_3_acc bigint, month_3_fat bigint,
  month_4_acc bigint, month_4_fat bigint,
  month_5_acc bigint, month_5_fat bigint,
  month_6_acc bigint, month_6_fat bigint,
  month_7_acc bigint, month_7_fat bigint,
  month_8_acc bigint, month_8_fat bigint,
  month_9_acc bigint, month_9_fat bigint,
  month_10_acc bigint, month_10_fat bigint,
  month_11_acc bigint, month_11_fat bigint,
  month_12_acc bigint, month_12_fat bigint,
  avg_hosp_arr_mn decimal, avg_hosp_5_mi decimal,
  tot_sp decimal, hea_sp decimal, hos_sp decimal, hig_sp decimal)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,acc:tot_acc#b,acc:tot_fat#b,acc:daytime_acc#b,acc:daytime_fat#b,acc:nightime_acc#b,acc:nightime_fat#b,acc:clear_acc#b,acc:clear_fat#b,acc:rain_acc#b,acc:rain_fat#b,acc:snow_acc#b,acc:snow_fat#b,acc:cloudy_acc#b,acc:cloudy_fat#b,acc:fog_acc#b,acc:fog_fat#b,acc:hail_acc#b,acc:hail_fat#b,acc:junction_acc#b,acc:junction_fat#b,acc:mon_acc#b,acc:mon_fat#b,acc:tue_acc#b,acc:tue_fat#b,acc:wed_acc#b,acc:wed_fat#b,acc:thu_acc#b,acc:thu_fat#b,acc:fri_acc#b,acc:fri_fat#b,acc:sat_acc#b,acc:sat_fat#b,acc:sun_acc#b,acc:sun_fat#b,acc:hour_1_acc#b,acc:hour_1_fat#b,acc:hour_2_acc#b,acc:hour_2_fat#b,acc:hour_3_acc#b,acc:hour_3_fat#b,acc:hour_4_acc#b,acc:hour_4_fat#b,acc:hour_5_acc#b,acc:hour_5_fat#b,acc:hour_6_acc#b,acc:hour_6_fat#b,acc:hour_7_acc#b,acc:hour_7_fat#b,acc:hour_8_acc#b,acc:hour_8_fat#b,acc:hour_9_acc#b,acc:hour_9_fat#b,acc:hour_10_acc#b,acc:hour_10_fat#b,acc:hour_11_acc#b,acc:hour_11_fat#b,acc:hour_12_acc#b,acc:hour_12_fat#b,acc:hour_13_acc#b,acc:hour_13_fat#b,acc:hour_14_acc#b,acc:hour_14_fat#b,acc:hour_15_acc#b,acc:hour_15_fat#b,acc:hour_16_acc#b,acc:hour_16_fat#b,acc:hour_17_acc#b,acc:hour_17_fat#b,acc:hour_18_acc#b,acc:hour_18_fat#b,acc:hour_19_acc#b,acc:hour_19_fat#b,acc:hour_20_acc#b,acc:hour_20_fat#b,acc:hour_21_acc#b,acc:hour_21_fat#b,acc:hour_22_acc#b,acc:hour_22_fat#b,acc:hour_23_acc#b,acc:hour_23_fat#b,acc:hour_24_acc#b,acc:hour_24_fat#b,acc:month_1_acc#b,acc:month_1_fat#b,acc:month_2_acc#b,acc:month_2_fat#b,acc:month_3_acc#b,acc:month_3_fat#b,acc:month_4_acc#b,acc:month_4_fat#b,acc:month_5_acc#b,acc:month_5_fat#b,acc:month_6_acc#b,acc:month_6_fat#b,acc:month_7_acc#b,acc:month_7_fat#b,acc:month_8_acc#b,acc:month_8_fat#b,acc:month_9_acc#b,acc:month_9_fat#b,acc:month_10_acc#b,acc:month_10_fat#b,acc:month_11_acc#b,acc:month_11_fat#b,acc:month_12_acc#b,acc:month_12_fat#b,acc:avg_hosp_arr_mn,acc:avg_hosp_5_mi,acc:tot_sp,acc:hea_sp,acc:hos_sp,acc:hig_sp')
TBLPROPERTIES ('hbase.table.name' = 'ldinh_accidents_conditions_by_state');

insert overwrite table ldinh_accidents_conditions_by_state
select concat(a.state, a.year),
  a.tot_accidents, f.tot_accidents,
  a.daytime, f.daytime, 
  a.nightime, f.nightime,
  a.clear, f.clear,
  a.rain, f.rain,
  a.snow, f.snow,
  a.cloudy, f.cloudy,
  a.fog, f.fog,
  a.hail, f.hail,
  a.junction, f.junction,
  a.monday, f.monday,
  a.tuesday, f.tuesday,
  a.wednesday, f.wednesday,
  a.thursday, f.thursday,
  a.friday, f.friday,
  a.saturday, f.saturday,
  a.sunday, f.sunday,
  a.hour_1, f.hour_1,
  a.hour_2, f.hour_2,
  a.hour_3, f.hour_3,
  a.hour_4, f.hour_4,
  a.hour_5, f.hour_5,
  a.hour_6, f.hour_6,
  a.hour_7, f.hour_7,
  a.hour_8, f.hour_8,
  a.hour_9, f.hour_9,
  a.hour_10, f.hour_10,
  a.hour_11, f.hour_11,
  a.hour_12, f.hour_12,
  a.hour_13, f.hour_13,
  a.hour_14, f.hour_14,
  a.hour_15, f.hour_15,
  a.hour_16, f.hour_16,
  a.hour_17, f.hour_17,
  a.hour_18, f.hour_18,
  a.hour_19, f.hour_19,
  a.hour_20, f.hour_20,
  a.hour_21, f.hour_21,
  a.hour_22, f.hour_22,
  a.hour_23, f.hour_23,
  a.hour_24, f.hour_24,
  a.month_1, f.month_1,
  a.month_2, f.month_2,
  a.month_3, f.month_3,
  a.month_4, f.month_4,
  a.month_5, f.month_5,
  a.month_6, f.month_6,
  a.month_7, f.month_7,
  a.month_8, f.month_8,
  a.month_9, f.month_9,
  a.month_10, f.month_10,
  a.month_11, f.month_11,
  a.month_12, f.month_12,
  a.avg_hosp_arr_mn, a.avg_hosp_5_mi,
  a.tot, a.hea, a.hos, a.hig
from
(select state, year,
  tot_accidents, 
  daytime, nightime, 
  clear, rain, 
  snow, cloudy, 
  fog, hail, junction_yes junction,
  monday, tuesday,
  wednesday, thursday,
  friday, saturday, sunday,
  hour_1, hour_2, hour_3, hour_4,
  hour_5, hour_6, hour_7, hour_8,
  hour_9, hour_10, hour_11, hour_12, 
  hour_13, hour_14, hour_15, hour_16,
  hour_17, hour_18, hour_19, hour_20,
  hour_21, hour_22, hour_23, hour_24, 
  month_1, month_2, month_3, month_4, 
  month_5, month_6, month_7, month_8, 
  month_9, month_10, month_11, month_12,
  avg_hosp_arr_mn, avg_hosp_5_mi,
  tot, hea, hos, hig
from ldinh_accidents_master) a
left join
(select state, year,
  tot_accidents, 
  daytime, nightime, 
  clear, rain, 
  snow, cloudy, 
  fog, hail, junction_yes junction,
  monday, tuesday,
  wednesday, thursday,
  friday, saturday, sunday,
  hour_1, hour_2, hour_3, hour_4,
  hour_5, hour_6, hour_7, hour_8,
  hour_9, hour_10, hour_11, hour_12, 
  hour_13, hour_14, hour_15, hour_16,
  hour_17, hour_18, hour_19, hour_20,
  hour_21, hour_22, hour_23, hour_24, 
  month_1, month_2, month_3, month_4, 
  month_5, month_6, month_7, month_8, 
  month_9, month_10, month_11, month_12
from ldinh_fatalities_master) f
on a.state = f.state and a.year = f.year;


