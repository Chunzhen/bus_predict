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
		self.__recent_day=[1,2,3,4,5,6,7,10,15,30,60,365]
		self.__roads=['21','13','16','19','6','9','11'] #
		#self.__roads=['12','15','2','8','10','4','7']

	#拆分不同路线
	def split_road_data(self):
		roads=self.__roads
		f=file(u'sql/split_road_data.txt','wb')
		sql=''
		for road in roads:
			sql_str=u"drop table if exists gd_train_data_"+road+"; create table gd_train_data_"+road+" as select * from tianchi_gd.p2_gd_train_data where line_name='线路"+road+"';"
			f.write(sql_str+'\n')
			sql+=sql_str
		return sql

	#用户距离截止时间的前（1/2/3/4/5/6/7/10/15/30/60/全部 ）乘坐车的次数
	#用户在前1天，前5天，前7天，前30天乘车次数占全部乘车次数的比例
	def last_recent_day_count_sql(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_recent_day_count.txt','wb')
		#print timestamp
		sql=""
		sql+="drop table if exists recent_day_count_road_"+road+"_"+type+"; create table recent_day_count_road_"+road+"_"+type+" as select \n"
		inner_sql=""
		inner_select="a.card_id"
		inner_sql+="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a \n"
		for x in recent_day:
			r_timestamp=timestamp-x*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			sql_str=u"left outer join (select Card_id, count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as day_count_"+str(x)+" from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) table_day_count_"+str(x)+" on a.card_id=table_day_count_"+str(x)+".card_id \n"
			inner_select+=", day_count_"+str(x)
			if x<=60:
				inner_select+=", (cast(day_count_"+str(x)+" as double) / cast(day_count_365 as double)) as day_count_percent_"+str(x)
			inner_sql+=sql_str
		
		sql+=inner_select+"\n from \n"+inner_sql+";\n"
		f.write(sql)
		return sql+'\n'


	#用户距离截止时间前1个月内每天乘车次数超过两次以上的次数，1周，1月内换一种计算方法，多次计算减去一次计算的次数
	def count_beyond_two_times_sql(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		t=[3,7,14,21]
		f=file(u'sql/'+base_day+'_road_'+road+'_count_beyond_two_times.txt','wb')
		sql=""
		sql+="drop table if exists count_beyond_two_times_road_"+road+"_"+type+"; \n"
		sql+="create table count_beyond_two_times_road_"+road+"_"+type+" as select c.card_id,";

		for temp_t in t:
			sql+="a_"+str(temp_t)+".beyond_two_count_"+str(temp_t)+",";
		sql+="b.beyond_two_count_30 from \n"
		sql+="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) c left outer join\n"
		for temp_t in t:
			r_timestamp=timestamp-temp_t*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			sql+="(select Card_id, (count(*)-count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD')))) as beyond_two_count_"+str(temp_t)+" from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) a_"+str(temp_t)+" on c.card_id=a_"+str(temp_t)+".card_id left outer join \n"
		r_timestamp=timestamp-30*86400
		d=datetime.fromtimestamp(float(r_timestamp))
		dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
		sql+="(select Card_id, (count(*)-count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD')))) as beyond_two_count_30 from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) b on c.card_id=b.card_id;\n"
		
		f.write(sql)
		return sql

	#用户距离截止时间1，2，3，4个星期内，周一到周五乘车的次数，周六到周日乘车的次数，周一到周五占一周内乘车的比例。
	def weekday_weekend_count_sql(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_weekday_weekend_count.txt','wb')
		sql="drop table if exists weekday_weekend_count_road_"+road+"_"+type+";\n create table weekday_weekend_count_road_"+road+"_"+type+" as select \n"
		inner_sql="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a left outer join \n"
		inner_select=""
		for i in range(4):
			j=(i+1)*7
			index=str(i+1)
			r_timestamp=timestamp-j*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			if i==0:
				inner_select+="a.card_id"
				inner_select+=", weekday_"+index+".weekday_count_"+index
				inner_select+=", weekend_"+index+".weekend_count_"+index
				inner_select+=", cast(weekday_"+index+".weekday_count_"+index+" as double)/cast((weekday_"+index+".weekday_count_"+index+")+(weekend_"+index+".weekend_count_"+index+") as double) as weekday_percent_"+index
				inner_select+=", traffic_"+index+".traffic_count_"+index
				inner_select+=", cast(traffic_"+index+".traffic_count_"+index+" as double)/cast((weekday_"+index+".weekday_count_"+index+")+(weekend_"+index+".weekend_count_"+index+") as double) as traffic_percent_"+index
				inner_select+=" \n"
			else:
				inner_select+=", weekday_"+index+".weekday_count_"+index
				inner_select+=", weekend_"+index+".weekend_count_"+index
				inner_select+=", cast(weekday_"+index+".weekday_count_"+index+" as double)/cast((weekday_"+index+".weekday_count_"+index+")+(weekend_"+index+".weekend_count_"+index+") as double) as weekday_percent_"+index
				inner_select+=", traffic_"+index+".traffic_count_"+index
				inner_select+=", cast(traffic_"+index+".traffic_count_"+index+" as double)/cast((weekday_"+index+".weekday_count_"+index+")+(weekend_"+index+".weekend_count_"+index+") as double) as traffic_percent_"+index
				inner_select+=" \n"

			if i==3:
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekday_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 group by card_id) weekday_"+index+" on a.card_id=weekday_"+index+".card_id left outer join \n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekend_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))>=5 group by card_id) weekend_"+index+" on a.card_id=weekend_"+index+".card_id left outer join\n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as traffic_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 and ((datepart(to_date(deal_time,'yyyymmddhh'),'hh')>6 and datepart(to_date(deal_time,'yyyymmddhh'),'hh')<11) or (datepart(to_date(deal_time,'yyyymmddhh'),'hh')>15 and datepart(to_date(deal_time,'yyyymmddhh'),'hh')<19))  group by card_id) traffic_"+index+" on a.card_id=traffic_"+index+".card_id; \n"
			else:
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekday_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 group by card_id) weekday_"+index+" on a.card_id=weekday_"+index+".card_id left outer join \n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekend_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))>=5 group by card_id) weekend_"+index+" on a.card_id=weekend_"+index+".card_id left outer join \n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as traffic_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 and ((datepart(to_date(deal_time,'yyyymmddhh'),'hh')>6 and datepart(to_date(deal_time,'yyyymmddhh'),'hh')<11) or (datepart(to_date(deal_time,'yyyymmddhh'),'hh')>15 and datepart(to_date(deal_time,'yyyymmddhh'),'hh')<19))  group by card_id) traffic_"+index+" on a.card_id=traffic_"+index+".card_id left outer join \n"

		sql+=inner_select+" from\n "+inner_sql
		f.write(sql)
		return sql

	#用户前1，2，3，4周，每个小时段乘车的次数
	def hour_count_sql(self,road,type):
		base_day=self.__base_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_hour_count.txt','wb')
		sql="drop table if exists hour_count_road_"+road+"_"+type+";\n create table hour_count_road_"+road+"_"+type+" as select \n"
		inner_sql="(select card_id from gd_train_data_"+road+" where deal_time<'"+base_day+"05' group by card_id) a left outer join \n"
		inner_select="a.card_id"
		for i in range(4):
			if i>0:
				continue
			j=(i+1)*7
			index=str(i+1)
			r_timestamp=timestamp-j*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			for k in range(15):
				hour_index=str(k+6)
				inner_select+=" , week_"+index+"_hour_"+hour_index+".week_"+index+"_hour_count_"+hour_index

			if i==0:
				for k in range(15):
					hour_index=str(k+6)
					if k==14:
						inner_sql+="(select card_id, count(*) as week_"+index+"_hour_count_"+hour_index+" from  gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and  datepart(to_date(deal_time,'yyyymmddhh'),'hh')="+hour_index+"  group by card_id) week_"+index+"_hour_"+hour_index+" on a.card_id=week_"+index+"_hour_"+hour_index+".card_id; \n"
					else:
						inner_sql+="(select card_id, count(*) as week_"+index+"_hour_count_"+hour_index+" from  gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and  datepart(to_date(deal_time,'yyyymmddhh'),'hh')="+hour_index+"  group by card_id) week_"+index+"_hour_"+hour_index+" on a.card_id=week_"+index+"_hour_"+hour_index+".card_id left outer join \n"
			else:
				for k in range(15):
					hour_index=str(k+6)
					inner_sql+="(select card_id, count(*) as week_"+index+"_hour_count_"+hour_index+" from  gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and  datepart(to_date(deal_time,'yyyymmddhh'),'hh')="+hour_index+"  group by card_id) week_"+index+"_hour_"+hour_index+" on a.card_id=week_"+index+"_hour_"+hour_index+".card_id left outer join \n"



		sql+=inner_select+" from\n "+inner_sql
		f .write(sql)
		return sql

	#训练集&&正例集=所有用户的标签
	def classify_label_sql(self,base_day1,base_day2,base_day3,base_day4,road,type):
		base_day=self.__base_day

		f=file(u'sql/'+base_day+'_road_'+road+'_classify_label.txt','wb')
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
		f.write(sql)
		return sql+'\n'

	#发卡地与卡类型
	def type_sql(self,road,type):
		base_day=self.__base_day

		sql="drop table if exists type_road_"+road+"_"+type+"; create table type_road_"+road+"_"+type+" as select t.card_id,t.card_type_key,t.create_city_key from \n"
		sql+="(select a.card_id,b.card_type_key,c.create_city_key,row_number() over(partition by a.card_id order by a.card_type desc) as rank from \n"
		sql+="(select card_id,card_type,create_city from gd_train_data_"+road+" where deal_time<'"+base_day+"05') a left outer join \n"
		sql+=" (select card_type_key,card_type from card_type_table) b on a.card_type=b.card_type left outer join \n"
		sql+=" (select create_city_key,create_city from create_city_table) c on a.create_city=c.create_city ) t where t.rank=1; \n"
		return sql

	#将所有属性列与标签列合并到一个表
	def column_concat_sql(self,road,type):
		base_day=self.__base_day
		recent_day=self.__recent_day
		recent_count_select=""
		for x in recent_day:
			recent_count_select+=", recent_day_count_road_"+road+"_"+type+".day_count_"+str(x)
			if x<=60:
				recent_count_select+=", recent_day_count_road_"+road+"_"+type+".day_count_percent_"+str(x)

		f=file(u'sql/'+base_day+'_road_'+road+'column_concat.txt','wb')
		sql="drop table if exists "+type+"_data_road_"+road+"; \n"
		sql+="create table "+type+"_data_road_"+road+" as \n"
		sql+="select recent_day_count_road_"+road+"_"+type+".card_id"+recent_count_select
		sql+=", count_beyond_two_times_road_"+road+"_"+type+".beyond_two_count_30"
		t=[3,7,14,21]
		for temp_t in t:
			sql+=", count_beyond_two_times_road_"+road+"_"+type+".beyond_two_count_"+str(temp_t);

		weekday_weekend_select=""
		for i in range(4):
			index=str(i+1)
			weekday_weekend_select+=",weekday_weekend_count_road_"+road+"_"+type+".weekday_count_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".weekend_count_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".weekday_percent_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".traffic_count_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".traffic_percent_"+index
			weekday_weekend_select+=" \n"

		hour_select=""
		for i in range(4):
			if i>0:
				continue
			index=str(i+1)
			for k in range(15):
				hour_index=str(k+6)
				hour_select+=" , hour_count_road_"+road+"_"+type+".week_"+index+"_hour_count_"+hour_index


		sql+=weekday_weekend_select
		sql+=hour_select
		sql+=",type_road_"+road+"_"+type+".create_city_key,type_road_"+road+"_"+type+".card_type_key \n"
		sql+=",cast(label_road_"+road+"_"+type+".label as int) as label \n"
		sql+="from label_road_"+road+"_"+type+"\n"
		sql+=" left outer join recent_day_count_road_"+road+"_"+type+" on label_road_"+road+"_"+type+".card_id=recent_day_count_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join count_beyond_two_times_road_"+road+"_"+type+" on label_road_"+road+"_"+type+".card_id=count_beyond_two_times_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join weekday_weekend_count_road_"+road+"_"+type+" on label_road_"+road+"_"+type+".card_id=weekday_weekend_count_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join hour_count_road_"+road+"_"+type+" on label_road_"+road+"_"+type+".card_id=hour_count_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join type_road_"+road+"_"+type+" on label_road_"+road+"_"+type+".card_id=type_road_"+road+"_"+type+".card_id;"
		
		f.write(sql)
		return sql+'\n'

	def coumun_concat_sql_predict(self,road,type):
		# base_day=self.__base_day
		# f=file(u'sql/'+base_day+'_road_'+road+'column_concat_predict.txt','wb')
		# sql="drop table if exists "+type+"_data_road_"+road+"_predict; \n"
		# sql+="create table "+type+"_data_road_"+road+"_predict as \n"
		# sql+="select recent_day_count_road_"+road+".*, count_beyond_two_times_road_"+road+".*, weekday_weekend_count_road_"+road+".* \n"
		# sql+="from recent_day_count_road_"+road
		# sql+=" join count_beyond_two_times_road_"+road+" on recent_day_count_road_"+road+".card_id=count_beyond_two_times_road_"+road+".card_id"
		# sql+=" join weekday_weekend_count_road_"+road+" on recent_day_count_road_"+road+".card_id=weekday_weekend_count_road_"+road+".card_id;"
		# f.write(sql)
		# return sql+'\n'
		base_day=self.__base_day
		recent_day=self.__recent_day
		recent_count_select=""
		for x in recent_day:
			recent_count_select+=", recent_day_count_road_"+road+"_"+type+".day_count_"+str(x)
			if x<=60:
				recent_count_select+=", recent_day_count_road_"+road+"_"+type+".day_count_percent_"+str(x)

		f=file(u'sql/'+base_day+'_road_'+road+'column_concat_predict.txt','wb')
		sql="drop table if exists "+type+"_data_road_"+road+"_predict; \n"
		sql+="create table "+type+"_data_road_"+road+"_predict as \n"
		sql+="select recent_day_count_road_"+road+"_"+type+".card_id"+recent_count_select
		sql+=", count_beyond_two_times_road_"+road+"_"+type+".beyond_two_count_30"
		t=[3,7,14,21]
		for temp_t in t:
			sql+=", count_beyond_two_times_road_"+road+"_"+type+".beyond_two_count_"+str(temp_t);

		weekday_weekend_select=""
		for i in range(4):
			index=str(i+1)
			weekday_weekend_select+=",weekday_weekend_count_road_"+road+"_"+type+".weekday_count_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".weekend_count_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".weekday_percent_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".traffic_count_"+index
			weekday_weekend_select+=", weekday_weekend_count_road_"+road+"_"+type+".traffic_percent_"+index
			weekday_weekend_select+=" \n"

		hour_select=""
		for i in range(4):
			if i>0:
				continue
			index=str(i+1)
			for k in range(15):
				hour_index=str(k+6)
				hour_select+=" , hour_count_road_"+road+"_"+type+".week_"+index+"_hour_count_"+hour_index


		sql+=weekday_weekend_select
		sql+=hour_select
		sql+=",type_road_"+road+"_"+type+".create_city_key,type_road_"+road+"_"+type+".card_type_key \n"
		sql+="from recent_day_count_road_"+road+"_"+type
		sql+=" left outer join count_beyond_two_times_road_"+road+"_"+type+" on recent_day_count_road_"+road+"_"+type+".card_id=count_beyond_two_times_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join weekday_weekend_count_road_"+road+"_"+type+" on recent_day_count_road_"+road+"_"+type+".card_id=weekday_weekend_count_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join hour_count_road_"+road+"_"+type+" on recent_day_count_road_"+road+"_"+type+".card_id=hour_count_road_"+road+"_"+type+".card_id\n"
		sql+=" left outer join type_road_"+road+"_"+type+" on recent_day_count_road_"+road+"_"+type+".card_id=type_road_"+road+"_"+type+".card_id;"
		f.write(sql)
		return sql+'\n'

	#预测为正例的所有card_id
	def get_predict_card_id(self,road):
		f=file(u'sql/final_road.txt','a+')
		sql="drop table if exists final_road_"+road+"; create table final_road_"+road+" as select card_id,regexp_replace('abc','abc','线路"+road+"') as line_name from predict_road_"+road+" where prediction_result=1;"
		f.write(sql)
		return sql+'\n'

	#合并所有路的结果
	def concat_predict_data(self):
		roads=self.__roads
		f=file(u'sql/concat_predict_data.txt','wb')
		sql="drop table if exists gd_predict as select * from("
		_len=len(roads)
		i=0
		for road in roads:
			i+=1
			if i==_len:
				sql+="select * from final_road_"+road+")"
			else:
				sql+="select * from final_road_"+road+" union all "

		f.write(sql)

	def add_zero(self,s):
		t=str(s)
		if s<10:
			t='0'+t
		return t

	def create_city(self):
		sql="drop table if exists create_city_table; "
		sql+="create table create_city_table as select create_city,row_number() over(partition by num order by num desc) as create_city_key from (select create_city,length(create_city) as num from tianchi_gd.gd_train_data group by create_city)t"

	def card_type(self):
		sql="drop table if exists card_type_table;"
		sql+="create table card_type_table as select card_type,row_number() over(partition by num order by num desc) as card_type_key from (select card_type,length(card_type) as num from tianchi_gd.gd_train_data group by card_type)t"

import MySQLdb
def run_sql(sql):
	try:
		conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='tianchi_gd',port=3306,charset="utf8")
		cur=conn.cursor()
		cur.execute(sql)
		cur.close()  
		conn.commit() 
		conn.close()
	except MySQLdb.Error,e:
		print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	pass

def get_train_data():
	base_day='20141217'
	road='10'
	#正例集
	base_day1='20141218'
	base_day2='20141224'
	#所有用户集
	base_day3='20140801'
	base_day4='20141217'
	get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,'train')

def get_test_data():
	roads=['12','15','2','8','10','4','7']
	#roads=['21','13','16','19','6','9','11']#
	# base_day='20141225'
	# #正例集
	# base_day1='20141225'
	# base_day2='20141231'
	# #所有用户集
	# base_day3='20140801'
	# base_day4='20141225'
	roads=['4']

	# base_day='20141218'
	# #正例集
	# base_day1='20141218'
	# base_day2='20141224'
	# #所有用户集
	# base_day3='20140801'
	# base_day4='20141218'

	base_day='20141211'
	#正例集
	base_day1='20141211'
	base_day2='20141217'
	#所有用户集
	base_day3='20140801'
	base_day4='20141211'
	for road in roads:
		get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,'test3')

def get_predict_data():
	#roads=['12','15','2','8','10','4','7'] 
	roads=['21','13','16','19','6','9','11']
	sql_instance=Sql_creation('20141231')
	for x in roads:
		_get_predict_data('20141231',x)

def _get_predict_data(base_day,road):
	#base_day='20141231'
	#road='10'
	sql_instance=Sql_creation(base_day)
	sql_arr=[]
	#计算不同路的用户最近1-60天的乘坐次数
	type='predict'
	sql_arr.append(sql_instance.last_recent_day_count_sql(road,type))
	#two times
	sql_arr.append(sql_instance.count_beyond_two_times_sql(road,type))
	#weekday weeken
	sql_arr.append(sql_instance.weekday_weekend_count_sql(road,type))
	#hour
	sql_arr.append(sql_instance.hour_count_sql(road,type))	
	#type
	sql_arr.append(sql_instance.type_sql(road,type))
	#合并为一个表
	sql_arr.append(sql_instance.coumun_concat_sql_predict(road,'predict'))
	f=file(u'sql/road_'+road+'_predict_data_sql.txt','wb')
	for sql in sql_arr:
		f.write(sql+'\n')

def get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,type):
	sql_instance=Sql_creation(base_day)
	sql_arr=[]
	
	#计算不同路的用户最近1-60天的乘坐次数
	sql_arr.append(sql_instance.last_recent_day_count_sql(road,type))
	#two times
	sql_arr.append(sql_instance.count_beyond_two_times_sql(road,type))
	#weekday weeken
	sql_arr.append(sql_instance.weekday_weekend_count_sql(road,type))	
	#hour
	sql_arr.append(sql_instance.hour_count_sql(road,type))	
	#获取label
	sql_arr.append(sql_instance.classify_label_sql(base_day1,base_day2,base_day3,base_day4,road,type))
	#type
	sql_arr.append(sql_instance.type_sql(road,type))
	#合并
	sql_arr.append(sql_instance.column_concat_sql(road,type))

	f=file(u'sql/road_'+road+'_'+type+'_data_sql.txt','wb')
	for sql in sql_arr:
		f.write(sql+'\n')
		#run_sql(sql)

def main():
	get_test_data()
	get_predict_data()
	#sql_instance=Sql_creation("20141231")
	#sql_instance.get_predict_card_id('10')

	# try:
	# 	os.remove(u'sql/all_road_predict_data_sql.txt') 
	# 	#os.remove(u'sql/final_road.txt')
	# except WindowsError:
	# 	pass

	# roads=['12','15','2','8','10','4','7'] #10
	# sql_instance=Sql_creation('20141231')
	# for x in roads:
	# 	get_predict_data('20141231',x)
		
	# 	#sql_instance.get_predict_card_id(x)
	# pass

	# sql_instance.concat_predict_data()

	# base_day1='20141225'
	# base_day2='20141231'
	# #所有用户集
	# base_day3='20140801'
	# base_day4='20141224'
	# sql_instance=Sql_creation("20141224")
	# sql_instance.hour_count_sql('4','test')


if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()


	base_day='20141217'
	#roads=['12','15','2','8','10','4','7']
	sql_instance=Sql_creation(base_day)
	#分开不同线路的交通数据
	#sql=sql_instance.split_road_data()
	# #计算不同路的用户最近1-60天的乘坐次数
	# #sql=sql_instance.last_recent_day_count_sql(roads[4])
	# #合并为一个表
	# #sql=sql_instance.concat_recent_day_count_sql(roads[4])
	# #正例集
	# # base_day1='20141218'
	# # base_day2='20141224'
	# # sql=sql_instance.positive_set_sql(base_day1,base_day2,roads[4])
	# #所有用户集
	# # base_day1='20140801'
	# # base_day2='20141217'
	# # sql=sql_instance.train_alluser_set_sql(base_day1,base_day2,roads[4])

	# sql=sql_instance.classify_label_sql(roads[4])
	# run_sql(sql)
	# sql=sql_instance.train_data_sql(roads[4])
	# #print sql
	# run_sql(sql)

	# st=""
	# for i in range(15):
	# 	st+=",hour_count_road_4_test.week_4_hour_count_"+str(int(i)+6)

	# print st