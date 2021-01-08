Name: Linh Dinh

Hbase query: create 'ldinh_accidents_conditions_by_state', 'acc'

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
  avg_hosp_arr_mn decimal, avg_hosp_5_mi decimal,
  tot_sp decimal, hea_sp decimal, hos_sp decimal, hig_sp decimal)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,acc:tot_acc#b,acc:tot_fat#b,acc:daytime_acc#b,acc:daytime_fat#b,acc:nightime_acc#b,acc:nightime_fat#b,acc:clear_acc#b,acc:clear_fat#b,acc:rain_acc#b,acc:rain_fat#b,acc:snow_acc#b,acc:snow_fat#b,acc:cloudy_acc#b,acc:cloudy_fat#b,acc:fog_acc#b,acc:fog_fat#b,acc:hail_acc#b,acc:hail_fat#b,acc:junction_acc#b,acc:junction_fat#b,acc:mon_acc#b,acc:mon_fat#b,acc:tue_acc#b,acc:tue_fat#b,acc:wed_acc#b,acc:wed_fat#b,acc:thu_acc#b,acc:thu_fat#b,acc:fri_acc#b,acc:fri_fat#b,acc:sat_acc#b,acc:sat_fat#b,acc:sun_acc#b,acc:sun_fat#b,acc:avg_hosp_arr_mn,acc:avg_hosp_5_mi,acc:tot_sp,acc:hea_sp,acc:hos_sp,acc:hig_sp')
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
  a.avg_hosp_arr_mn/60, a.avg_hosp_5_mi,
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
  friday, saturday, sunday
from ldinh_fatalities_master) f
on a.state = f.state and a.year = f.year
where a.year in (2016,2017,2018);


Hbase query: create 'ldinh_state', 'st'

create external table ldinh_state (state string, state_dup string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,st:state_dup')
TBLPROPERTIES ('hbase.table.name' = 'ldinh_state');

insert overwrite table ldinh_state
select distinct state, state from ldinh_accidents_master; 











