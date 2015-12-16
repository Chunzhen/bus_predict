#! /usr/bin/env python
# -*- coding:utf-8 -*-
#  根据时间生成sql

import sys
import os
from datetime import datetime
import time


class Sql_creation(object):
	def __init__(self,base_day):
		self.__base_day=base_day
		self.__recent_day=[1,2,3,4,5,6,7,14,21,28,365]
		self.__roads=['21','13','16','19','6','9','11']


	def split_road_data(self):
		roads=self.__roads
		f=file(u'new_sql/split_road_data.txt','wb')
		#过滤掉出行1次的用户
		sql="drop table if exists gd_train_data; create table gd_train_data as select t.* from (select *,count(card_id) over(partition by card_id) as c from tianchi_gd.p2_gd_train_data)t where t.c>1;\n"
		for road in roads:
			sql_str=u"drop table if exists gd_train_data_"+road+"; create table gd_train_data_"+road+" as select * from gd_train_data where line_name='线路"+road+"';\n"		
			sql+=sql_str
		f.write(sql+'\n')
		return sql

	def split_weather_day(self):
		roads=self.__roads
		f=file(u'new_sql/split_weather_day.txt','wb')
		weather_type=['晴','雷阵雨','多云','大雨','中到大雨','大到暴雨','阵雨','中雨','阴','小雨','小到中雨','霾']
		sql=""
		index=0
		for w in weather_type:
			sql_str=u"drop table if exists weather_days_"+str(index)+"; create table weather_days_"+str(index)+" as select to_date(concat(split_part(date_time,'/',1),cast(cast(cast(split_part(date_time,'/',2) as int)/10 as int) as string),cast(cast(cast(split_part(date_time,'/',2) as int)%10 as int) as string),cast(cast(cast(split_part(date_time,'/',3) as int)/10 as int) as string),cast(cast(cast(split_part(date_time,'/',3) as int)%10 as int) as string) ), 'yyyymmdd') as date_time from tianchi_gd.gd_weather_report where weather like '%"+w+"%';\n"
			index+=1
			sql+=sql_str

		f.write(sql)
		return sql


		#时间特征
	def weather_feature(self):
		roads=self.__roads
		f=file(u'new_sql/weather_feature.txt','wb')
		sql="drop table if exists weather_feature; create table weather_feature as "
		sql+="select w.date_time2"
		for i in range(7):
			sql+=", case when weekday(w.date_time2)="+str(i)+" then 1 else 0 end as weekday_col"+str(i)+"\n"
		weather_type=['晴','雷阵雨','多云','大雨','中到大雨','大到暴雨','阵雨','中雨','阴','小雨','小到中雨','霾']
		i=0
		for w in weather_type:
			sql+=",case when w.weather like '%"+w+"%' then 1 else 0 end as weather_col"+str(i)+" \n"
			i+=1
		sql+=",case when weekday(w.date_time2)>=5 or (w.date_time>='2015/1/1' and w.date_time<='2015/1/3') then 1 else 0 end as weekend_col \n"
		sql+=",case when weekday(w.date_time2)<5 or (w.date_time='2015/1/4') then 1 else 0 end as workday_col \n"
		sql+=" from (select *,to_date(concat(split_part(date_time,'/',1),cast(cast(cast(split_part(date_time,'/',2) as int)/10 as int) as string),cast(cast(cast(split_part(date_time,'/',2) as int)%10 as int) as string),cast(cast(cast(split_part(date_time,'/',3) as int)/10 as int) as string),cast(cast(cast(split_part(date_time,'/',3) as int)%10 as int) as string) ), 'yyyymmdd') as date_time2 from tianchi_gd.gd_weather_report)w;\n"
		f.write(sql)

	#用户与线路特征
	#count型特征
	#前k天有多少天乘坐公交车，（用户对公交车依赖程度）
	def user_count_predayk(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())

		sql=""
		sql+="drop table if exists user_count_predayk_road_"+road+"_"+type+"; create table user_count_predayk_road_"+road+"_"+type+" as select \n"
		inner_sql=""
		inner_select="a.card_id"
		inner_sql+="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a \n"
		for x in recent_day:
			r_timestamp=timestamp-x*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			sql_str=u"left outer join (select Card_id, count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as day_count_"+str(x)+" from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) table_day_count_"+str(x)+" on a.card_id=table_day_count_"+str(x)+".card_id \n"
			inner_select+=", day_count_"+str(x)
			if x<=28:
				inner_select+=", (cast(day_count_"+str(x)+" as double) / cast(day_count_200 as double)) as day_count_percent_"+str(x)
			inner_sql+=sql_str
		
		sql+=inner_select+"\n from \n"+inner_sql+";\n"
		return sql+'\n'

	#
	#用户在不同天气下乘坐一条线路的次数 比例
	def user_count_preweather(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())

		sql=""
		sql+="drop table if exists user_count_preweather_road_"+road+"_"+type+"; create table user_count_preweather_road_"+road+"_"+type+" as select \n"
		inner_sql=""
		inner_sql+="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a \n"
		inner_select="a.card_id "
		weather_type=['晴','雷阵雨','多云','大雨','中到大雨','大到暴雨','阵雨','中雨','阴','小雨','小到中雨','霾']
		w=0
		for i in weather_type:
			inner_sql+=" left outer join (select card_id,count(*) as oneroad_weather_"+str(w)+" from gd_train_data_"+road+" where deal_time<'"+base_day+"' and datetrunc(to_date(deal_time,'yyyymmddhh'),'DD') in \n";
			inner_sql+="(select date_time from weather_days_"+str(w)+") group by card_id) t_"+str(w)+" on a.card_id=t_"+str(w)+".card_id \n"
			inner_select+=", oneroad_weather_"+str(w)+" , oneroad_weather_"+str(w)+"/oneroad_weather_all as oneroad_weather_"+str(w)+"_percent"
			w+=1

		inner_sql+=" left outer join (select card_id,count(*) as oneroad_weather_all from gd_train_data_"+road+" where deal_time<'"+base_day+"' group by card_id) ta on ta.card_id=a.card_id"
		sql+=inner_select+"\n from \n"+inner_sql+";\n"
		return sql+"\n"


	def user_count_preweekly(self,road,type):
		level='7'
		r=15
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())

		sql=""
		sql+="drop table if exists user_count_preweekly_road_"+road+"_"+level+"_"+type+"; create table user_count_preweekly_road_"+road+"_"+level+"_"+type+" as select \n"
		inner_sql=""
		inner_sql+="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a \n"
		inner_select="a.card_id "
		for i in range(r):
			index=i*int(level);
			timestamp1=timestamp-86400*index;
			timestamp2=timestamp1-86400*int(level);
			d=datetime.fromtimestamp(float(timestamp1))
			dStr1=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			d=datetime.fromtimestamp(float(timestamp2))
			dStr2=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			inner_sql+="left outer join (select card_id, count(*) as level_"+level+"_"+str(i)+" ,cast(sign(count(*)) as int) as level_"+level+"_"+str(i)+"_bool from gd_train_data_"+road+" where deal_time>='"+dStr2+"' and deal_time<'"+dStr1+"' group by card_id) table_level_"+level+"_"+str(i)+" on a.card_id=table_level_"+level+"_"+str(i)+".card_id \n"
			inner_select+=", level_"+level+"_"+str(i)
			if i>0:
				inner_select+=",case when level_"+level+"_"+str(i)+"=1 and level_"+level+"_"+str(i-1)+"=1 then 1 else 0 end as continue_2week_"+str(i)

		sql+=inner_select+"\n from \n"+inner_sql+";\n"
		return sql+"\n"

	def create_city(self):
		f=file(u'new_sql/create_city.txt','wb')
		sql="drop table if exists create_city_table; "
		sql+="create table create_city_table as select t2.create_city,t2.create_city_key "
		for i in range(20):
			sql+=", case when t2.create_city_key="+str(i+1)+" then 1 else 0 end as create_city_col_"+str(i+1)+"\n"
		sql+="from (select create_city,row_number() over(partition by num order by num desc) as create_city_key from (select create_city,length('abc') as num from tianchi_gd.p2_gd_train_data group by create_city)t)t2;\n"
		f.write(sql)
		return sql

	def card_type(self):
		f=file(u'new_sql/card_type.txt','wb')
		sql="drop table if exists card_type_table;"
		sql+="create table card_type_table as select t2.card_type,t2.card_type_key "
		for i in range(7):
			sql+=", case when t2.card_type_key="+str(i+1)+" then 1 else 0 end as card_type_key"+str(i+1)+"\n"
		sql+="from (select card_type,row_number() over(partition by num order by num desc) as card_type_key from (select card_type,length('abc') as num from tianchi_gd.p2_gd_train_data group by card_type)t)t2;\n"
		f.write(sql)
		return sql

	#用户第一次与最后一次乘车时间，第一次与最后一次乘车时间的差值，最后一次与最后第二次的差值
	def user_first_last_sql(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())

		sql=""
		sql+="drop table if exists user_first_last_road_"+road+"_"+type+"; create table user_first_last_road_"+road+"_"+type+" as select \n"
		inner_sql=""
		inner_sql+="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a \n"
		inner_select=" a.card_id,last_visit,first_visit,(first_visit-last_visit) as visit_range \n"
		inner_select+=",(first_visit-last_visit)%2 as first_end_col_0 \n"
		inner_select+=",case when (first_visit-last_visit) in (2, 3, 6, 7, 10, 11, 14, 15, 18, 19, 22, 23, 26, 27, 30, 31, 34, 35, 38, 39, 42, 43, 46, 47, 50, 51, 54, 55, 58, 59, 62, 63, 66, 67, 70, 71, 74, 75, 78, 79, 82, 83, 86, 87, 90, 91, 94, 95, 98, 99, 102, 103, 106, 107, 110, 111, 114, 115, 118, 119, 122, 123, 126, 127) then 1 else 0 end as first_end_col_1 \n"
		inner_select+=",case when (first_visit-last_visit) in (4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31, 36, 37, 38, 39, 44, 45, 46, 47, 52, 53, 54, 55, 60, 61, 62, 63, 68, 69, 70, 71, 76, 77, 78, 79, 84, 85, 86, 87, 92, 93, 94, 95, 100, 101, 102, 103, 108, 109, 110, 111, 116, 117, 118, 119, 124, 125, 126, 127) then 1 else 0 end as first_end_col_2 \n"
		inner_select+=",case when (first_visit-last_visit) in (8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31, 40, 41, 42, 43, 44, 45, 46, 47, 56, 57, 58, 59, 60, 61, 62, 63, 72, 73, 74, 75, 76, 77, 78, 79, 88, 89, 90, 91, 92, 93, 94, 95, 104, 105, 106, 107, 108, 109, 110, 111, 120, 121, 122, 123, 124, 125, 126, 127) then 1 else 0 end as first_end_col_3 \n"
		inner_select+=",case when (first_visit-last_visit) in (16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127) then 1 else 0 end as first_end_col_4 \n"
		inner_select+=",case when (first_visit-last_visit) in (32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127) then 1 else 0 end as first_end_col_5 \n"
		inner_select+=",case when (first_visit-last_visit)>63 then 1 else 0 end as first_end_col_6"

		inner_select+=",(last_2_visit-last_visit) as visit_range2 \n"
		inner_select+=",(last_2_visit-last_visit)%2 as pre_end_col_0 \n"
		inner_select+=",case when (last_2_visit-last_visit) in (2, 3, 6, 7, 10, 11, 14, 15, 18, 19, 22, 23, 26, 27, 30, 31, 34, 35, 38, 39, 42, 43, 46, 47, 50, 51, 54, 55, 58, 59, 62, 63, 66, 67, 70, 71, 74, 75, 78, 79, 82, 83, 86, 87, 90, 91, 94, 95, 98, 99, 102, 103, 106, 107, 110, 111, 114, 115, 118, 119, 122, 123, 126, 127) then 1 else 0 end as pre_end_col_1 \n"
		inner_select+=",case when (last_2_visit-last_visit) in (4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31, 36, 37, 38, 39, 44, 45, 46, 47, 52, 53, 54, 55, 60, 61, 62, 63, 68, 69, 70, 71, 76, 77, 78, 79, 84, 85, 86, 87, 92, 93, 94, 95, 100, 101, 102, 103, 108, 109, 110, 111, 116, 117, 118, 119, 124, 125, 126, 127) then 1 else 0 end as pre_end_col_2 \n"
		inner_select+=",case when (last_2_visit-last_visit) in (8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31, 40, 41, 42, 43, 44, 45, 46, 47, 56, 57, 58, 59, 60, 61, 62, 63, 72, 73, 74, 75, 76, 77, 78, 79, 88, 89, 90, 91, 92, 93, 94, 95, 104, 105, 106, 107, 108, 109, 110, 111, 120, 121, 122, 123, 124, 125, 126, 127) then 1 else 0 end as pre_end_col_3 \n"
		inner_select+=",case when (last_2_visit-last_visit) in (16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127) then 1 else 0 end as pre_end_col_4 \n"
		inner_select+=",case when (last_2_visit-last_visit) in (32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127) then 1 else 0 end as pre_end_col_5 \n"
		inner_select+=",case when (last_2_visit-last_visit)>63 then 1 else 0 end as pre_end_col_6"

		inner_sql+="left outer join (select t1.card_id,cast(("+str(int(timestamp))+" - unix_timestamp(to_date(t1.deal_time,'yyyymmddhh')))/86400 as int) as last_visit from (select card_id,deal_time,row_number() over(partition by card_id order by deal_time desc) as rank from gd_train_data_"+road+" where deal_time<'"+base_day+"') t1 where t1.rank=1 ) table_last on a.card_id=table_last.card_id \n"
		inner_sql+="left outer join (select t2.card_id,cast(("+str(int(timestamp))+" - unix_timestamp(to_date(t2.deal_time,'yyyymmddhh')))/86400 as int) as first_visit  from (select card_id,deal_time,row_number() over(partition by card_id order by deal_time asc) as rank from gd_train_data_"+road+" where deal_time<'"+base_day+"') t2 where t2.rank=1 ) table_first on a.card_id=table_first.card_id \n"
		inner_sql+="left outer join (select t3.card_id,cast(("+str(int(timestamp))+" - unix_timestamp(to_date(t3.deal_time,'yyyymmddhh')))/86400 as int) as last_2_visit from (select card_id,deal_time,row_number() over(partition by card_id order by deal_time desc) as rank from gd_train_data_"+road+" where deal_time<'"+base_day+"') t3 where t3.rank=2 ) table_last_2 on a.card_id=table_last_2.card_id \n"
		sql+=inner_select+"\n from \n"+inner_sql+";\n"
		#f.write(sql)
		return sql+"\n"

	#发卡地与卡类型
	def user_type_sql(self,road,type):
		base_day=self.__base_day

		sql="drop table if exists user_type_road_"+road+"_"+type+"; create table user_type_road_"+road+"_"+type+" as select t.* from \n"
		sql+="(select a.card_id,b.*,c.*,row_number() over(partition by a.card_id order by a.card_type desc) as rank from \n"
		sql+="(select card_id,card_type,create_city from gd_train_data_"+road+" where deal_time<'"+base_day+"05') a left outer join \n"
		sql+=" (select * from card_type_table) b on a.card_type=b.card_type left outer join \n"
		sql+=" (select * from create_city_table) c on a.create_city=c.create_city ) t where t.rank=1; \n"
		return sql

	#合并特征项
	def concat_feature(self,road,type):
		level='7'
		r=15
		#将重名的card_id改掉
		tables=[]
		tables.append("user_count_predayk_road_"+road+"_"+type)
		tables.append("user_count_preweather_road_"+road+"_"+type)
		tables.append("user_count_preweekly_road_"+road+"_"+level+"_"+type)
		tables.append("user_first_last_road_"+road+"_"+type)
		tables.append("user_type_road_"+road+"_"+type)
		#alter column name
		sql=""
		i=0
		for table in tables:
			if i==0:
				i+=1
				continue
			sql+="alter table "+table+" change column card_id rename to card_id"+str(i)+";\n"
			i+=1

		sql+="drop table if exists "+type+"_"+road+"; create table "+type+"_"+road+" as select \n"
		i=0
		inner_select=""
		inner_sql=""
		for table in tables:
			if i==0:
				inner_select+=table+".* "
				inner_sql+=table+" "
			else:
				inner_select+=","+table+".* "
				inner_sql+="left outer join "+table+" on "+tables[0]+".card_id="+table+".card_id"+str(i)+"\n"
			i+=1
		sql+=inner_select+"\n from \n"+inner_sql+";\n"

		return sql

	def concat_label(self,road,type):
		sql="drop table if exists local_data_"+road+"; create table local_data_"+road+" as select label_road_"+road+"_"+type+".label,"+type+"_"+road+".* \n"
		sql+=" from "
		sql+="label_road_"+road+"_"+type+" left outer join "+type+"_"+road+" on "+type+"_"+road+".card_id=label_road_"+road+"_"+type+".card_id;"
		return sql

	def new_concat_all(self):
		#将重名的card_id改掉
		#new table
		roads=self.__roads
		sql=""
		for road in roads:
			sql+="drop table if exists final_data_"+road+";"
			sql+="create table final_data_"+road+" as select old_test_data_road_"+road+".*"
			for i in range(14):
				sql+=",local_data_road_"+road+".continue_2week_"+str(i+1)
			for i in range(6):			
				sql+=",local_data_road_"+road+".first_end_col_"+str(i)
			for i in range(6):			
				sql+=",local_data_road_"+road+".pre_end_col_"+str(i)
			for i in range(7):
				sql+=",local_data_road_"+road+".card_type_key"+str(i+1)
			for i in range(20):
				sql+=",local_data_road_"+road+".create_city_col_"+str(i+1)

			sql+="\n from \n"
			sql+="old_test_data_road_"+road+" left outer join local_data_road_"+road+" on local_data_road_"+road+".card_id=old_test_data_road_"+road+".card_id;\n"
		f=file(u'new_sql/new_concat_all.txt','wb')
		f.write(sql)

	def new_concat_all_p(self):
		#将重名的card_id改掉
		#new table
		roads=self.__roads
		sql=""
		for road in roads:
			sql+="drop table if exists final_predict_"+road+";"
			sql+="create table final_predict_"+road+" as select old_predict_data_road_"+road+"_predict.*"
			for i in range(14):
				sql+=",new_predict_"+road+".continue_2week_"+str(i+1)
			for i in range(6):			
				sql+=",new_predict_"+road+".first_end_col_"+str(i)
			for i in range(6):			
				sql+=",new_predict_"+road+".pre_end_col_"+str(i)
			for i in range(7):
				sql+=",new_predict_"+road+".card_type_key"+str(i+1)
			for i in range(20):
				sql+=",new_predict_"+road+".create_city_col_"+str(i+1)

			sql+="\n from \n"
			sql+="old_predict_data_road_"+road+"_predict left outer join new_predict_"+road+" on new_predict_"+road+".card_id=old_predict_data_road_"+road+"_predict.card_id;\n"
		f=file(u'new_sql/new_concat_all_p.txt','wb')
		f.write(sql)

	def extend_feature(self):
		roads=self.__roads
		sql=""
		for road in roads:
			sql+="drop table if exists extend_data_"+road+";"
			sql+="create table extend_data_"+road+" as select *"
			recent_day=[1,2,3,4,5,6,7,14,21,28,365]
			for day in recent_day:
				day=str(day)
				sql+=",pow(day_count_"+day+",2) as e1_day_count_"+day
				sql+=",exp(0-day_count_"+day+") as e2_day_count_"+day
				if day<200:
					sql+=",pow(day_count_percent_"+day+",2) as e1_day_count_percent_"+day
					sql+=",exp(0-day_count_percent_"+day+") as e2_day_count_percent_"+day

			sql+="\n"
			for i in range(4):
				sql+=",pow(weekday_count_"+str(i+1)+",2) as e1_weekday_count_"+str(i+1)
				sql+=",exp(0-weekday_count_"+str(i+1)+") as e2_weekday_count_"+str(i+1)

				sql+=",pow(weekend_count_"+str(i+1)+",2) as e1_weekend_count_"+str(i+1)
				sql+=",exp(0-weekend_count_"+str(i+1)+") as e2_weekend_count_"+str(i+1)

				sql+=",pow(traffic_count_"+str(i+1)+",2) as e1_traffic_count_"+str(i+1)
				sql+=",exp(0-traffic_count_"+str(i+1)+") as e2_traffic_count_"+str(i+1)

				sql+=",pow(weekday_percent_"+str(i+1)+",2) as e1_weekday_percent_"+str(i+1)
				sql+=",exp(0-weekday_percent_"+str(i+1)+") as e2_weekday_percent_"+str(i+1)

				sql+=",pow(traffic_percent_"+str(i+1)+",2) as e1_traffic_percent_"+str(i+1)
				sql+=",exp(0-traffic_percent_"+str(i+1)+") as e2_traffic_percent_"+str(i+1)

			sql+="\n"
			levels_dict={'2':14,'3':15,'7':10,'14':6,'21':4,'28':3}
			for level,v in levels_dict.items():
				for i in range(v):
					sql+=",pow(level_"+level+"_"+str(i)+",2) as e1_level_"+level+"_"+str(i)
					sql+=",exp(0-level_"+level+"_"+str(i)+") as e2_level_"+level+"_"+str(i)

			sql+="\n"
			sql+=",pow(first_visit,2) as e1_first_visit"
			sql+=",exp(0-first_visit) as e2_first_visit"

			sql+=",pow(last_visit,2) as e1_last_visit"
			sql+=",exp(0-last_visit) as e2_last_visit"

			sql+=",pow(visit_range,2) as e1_visit_range"
			sql+=",exp(0-visit_range) as e2_visit_range"

			sql+=",pow(visit_range2,2) as e1_visit_range2"
			sql+=",exp(0-visit_range2) as e2_visit_range2"

			sql+=",pow(allroad_first_visit,2) as e1_allroad_first_visit"
			sql+=",exp(0-allroad_first_visit) as e2_allroad_first_visit"

			sql+=",pow(allroad_last_visit,2) as e1_allroad_last_visit"
			sql+=",exp(0-allroad_last_visit) as e2_allroad_last_visit"

			sql+=",pow(allroad_visit_range,2) as e1_allroad_visit_range"
			sql+=",exp(0-allroad_visit_range) as e2_allroad_visit_range"

			sql+=",pow(allroad_visit_range2,2) as e1_allroad_visit_range2"
			sql+=",exp(0-allroad_visit_range2) as e2_allroad_visit_range2"

			sql+=",pow(all_count,2) as e1_all_count"
			sql+=",exp(0-all_count) as e2_all_count"
			sql+="\n"

			sql+=" from final_data_"+road+"_2;\n\n"

			f=file(u'new_sql/exetend_feature.txt','wb')
			f.write(sql)



	#训练集&&正例集=所有用户的标签
	def classify_label_sql(self,base_day1,base_day2,base_day3,base_day4,road,type):
		base_day=self.__base_day

		#f=file(u'sql/'+base_day+'_road_'+road+'_classify_label.txt','wb')
		sql="drop table if exists label_road_"+road+"_"+type+"; \ncreate table label_road_"+road+"_"+type+" as select \n"
		sql+=" card_id,regexp_replace(cast(length(card_id2) as string),'32','1') as label  from \n "
		d_base1=datetime.strptime(str(base_day3),'%Y%m%d')
		dStr1=str(d_base1.year)+self.add_zero(d_base1.month)+self.add_zero(d_base1.day)+'05'
		d_base2=datetime.strptime(str(base_day4),'%Y%m%d')
		timestamp=time.mktime(d_base2.timetuple())
		d_base2=datetime.fromtimestamp(float(timestamp))
		dStr2=str(d_base2.year)+self.add_zero(d_base2.month)+self.add_zero(d_base2.day)+'05'
		sql+="(select card_id as card_id from gd_train_data_"+road+" where deal_time<'"+dStr2+"' group by card_id) a\n left outer join \n"
		d_base1=datetime.strptime(str(base_day1),'%Y%m%d')
		dStr1=str(d_base1.year)+self.add_zero(d_base1.month)+self.add_zero(d_base1.day)+'05'
		d_base2=datetime.strptime(str(base_day2),'%Y%m%d')
		timestamp=time.mktime(d_base2.timetuple())+86400.0
		d_base2=datetime.fromtimestamp(float(timestamp))
		dStr2=str(d_base2.year)+self.add_zero(d_base2.month)+self.add_zero(d_base2.day)+'05'
		sql+="(select card_id as card_id2 from gd_train_data_"+road+" where deal_time>='"+dStr1+"' and deal_time<'"+dStr2+"' group by card_id) b on a.card_id=b.card_id2;\n"
		#f.write(sql)
		return sql+'\n'

	def add_zero(self,s):
		t=str(s)
		if s<10:
			t='0'+t
		return t

def get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,type):
	sql_instance=Sql_creation(base_day)
	sql_arr=[]
	#
	sql_arr.append(sql_instance.user_count_predayk(road,type))
	#
	sql_arr.append(sql_instance.user_count_preweather(road,type))
	#
	sql_arr.append(sql_instance.user_count_preweekly(road,type))
	#
	sql_arr.append(sql_instance.user_first_last_sql(road,type))
	
	sql_arr.append(sql_instance.user_type_sql(road,type))

	sql_arr.append(sql_instance.concat_feature(road,type))
	if type=='local':
		sql_arr.append(sql_instance.classify_label_sql(base_day1,base_day2,base_day3,base_day4,road,type))
		sql_arr.append(sql_instance.concat_label(road,type))

	

	f=file(u'new_sql/road_'+road+'_'+type+'_data_sql.txt','wb')
	for sql in sql_arr:
		f.write(sql+'\n')

def get_local_data():
	#roads=['12','15','2','8','10','4','7']
	roads=['21','13','16','19','6','9','11']#
	#截点类型1
	base_day='20141225'
	#正例集
	base_day1='20141225'
	base_day2='20141231'
	#所有用户集
	base_day3='20140801'
	base_day4='20141225'

	sql_instance=Sql_creation(base_day)
	start_day='2014080105'
	end_day='2015010105'
	base_len=120
	sql_instance.split_road_data()

	for road in roads:
		get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,'local')

def change_label_type():
	roads=['21','13','16','19','6','9','11']
	sql=""
	for road in roads:
		#sql+="drop table if exists local_data_road_"+road+";"
		sql+="create table local_data_road_"+road+" as select *,cast(label as int) as label2 from local_data_"+road+";\n"
	f=file(u'new_sql/change_column_type.txt','wb')
	f.write(sql)

def get_predict_data():
	roads=['21','13','16','19','6','9','11']
	sql_instance=Sql_creation('20150101')
	for x in roads:
		_get_predict_data('20150101',x,'new_predict')

def _get_predict_data(base_day,road,type):
	sql_instance=Sql_creation(base_day)
	sql_arr=[]
	#
	sql_arr.append(sql_instance.user_count_predayk(road,type))
	#
	sql_arr.append(sql_instance.user_count_preweather(road,type))
	#
	sql_arr.append(sql_instance.user_count_preweekly(road,type))
	#
	sql_arr.append(sql_instance.user_first_last_sql(road,type))
	
	sql_arr.append(sql_instance.user_type_sql(road,type))

	sql_arr.append(sql_instance.concat_feature(road,type))

	

	f=file(u'new_sql/road_'+road+'_'+type+'_data_sql.txt','wb')
	for sql in sql_arr:
		f.write(sql+'\n')


def week_passanger_count():
	roads=['21','13','16','19','6','9','11']
	sql="select line_name, count(distinct(card_id)) from tianchi_gd.p2_gd_train_data where deal_time>='2015090605' and deal_time<'2015091305' group by line_name;"

base = [str(x) for x in range(10)] + [ chr(x) for x in range(ord('A'),ord('A')+6)]
def dec2bin(string_num,col):
    num = int(string_num)
    mid = []
    while True:
        if num == 0: break
        num,rem = divmod(num, 2)
        mid.append(base[rem])
    #print ''.join([str(x) for x in mid[::-1]])
    if col>=len(mid):
    	return 0
    else:
    	return mid[col]

def f(n1,p,r,n):
	print "origin:"+str(2*p*r/(p+r))
	r2=r
	if n1<n:
		r2=n1*r/n
	else:
		p=n*p/n1
	print "final:"+str(2*p*r2/(p+r2))

def main():
	base_day='20141225'
	get_local_data()
	get_predict_data()
	change_label_type()
	sql_instance=Sql_creation(base_day)
	sql_instance.new_concat_all_p()
	pass

if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	#main()
	# l=[]
	# for i in range(128):
	# 	if dec2bin(i,6)=='1':
	# 		l.append(i)
	# print l
	# print dec2bin(60,6)
	# print dec2bin(60,5)
	# print dec2bin(60,4)
	# print dec2bin(60,3)
	# print dec2bin(60,2)
	# print dec2bin(60,1)
	# print dec2bin(60,0)

	real=[174758,22049,64276,46218,77314,120270,128929]
	# #predict=[191796,25754,70501,52791,68794,115215,122426]
	predict=[178513,22969,63881,48672,75221,109938,125869]
	#predict=[134812,20008,43381,34801,48939,106818,82696]
	#predict=[180370,20008,65736,44219,76204,122463,119710]

	# p=0.41
	# r=0.35

	# predict_1=[]
	# for a in predict:
	# 	predict_1.append(a*r)

	# predict_recall=[]
	# for i in range(7):
	# 	predict_recall.append(float(predict_1[i])/float(real[i]))

	# recall=float(sum(predict_1))/sum(real)
	# print "origin:"+str(2.0*p*r/(p+r))
	# print predict_recall
	# print recall
	# p=0.43
	# print 2.0*p*recall/(p+recall)

	# a=17424
	# b=174758

	# recall=0.42

	# p1=0.42
	# p2=0.46

	# n_recall=(b*0.46-17424*1)/b
	# print n_recall

	# print "origin F1:"+str(2*p1*recall/(p1+recall))
	# print "new    F1:"+str(2*p2*n_recall/(p2+n_recall))

	# print 0.37*0.36/0.4

	# p=0.6373
	# r=0.6373
	# #road11
	# n=227071#
	# n1=174758#
	# #road13
	# # n=32571#22207
	# # n1=22049#15033
	# #road 16 72759
	# n=81843
	# n1=64376*8/9

	# #road 19
	# n=62970
	# n1=46218
	


	# #road11
	# p=0.42081*0.88
	# r=0.4648
	# #road13
	# # p=0.57341
	# # r=0.6372
	# #n2=25153
	# #road16
	# p=0.45543*0.85
	# r=0.5119
	# #road19
	# p=0.4873*0.85
	# r=0.56459

	# f(n1,p,r,n)
	# print p
	# n2=n1*r/p
	# print n2
	# #n2=22207
	# n2=54275
	# f(n2,p,r,n)

	n1=177348.0
	n2=239406.0-15000
	f=76980.0

	p=f/n1
	r=f/n2

	print "precision"+str(p)
	print "recall:"+str(r)
	print "f1:"+str(2*p*r/(r+p))






