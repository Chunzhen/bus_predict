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
		self.__roads=['12','15','2','8','10','4','7']

	#拆分不同路线
	def split_road_data(self):
		roads=self.__roads
		f=file(u'sql/split_road_data.txt','wb')
		sql=''
		for road in roads:
			sql_str=u"drop table if exists gd_train_data_"+road+"; create table gd_train_data_"+road+" as select * from tianchi_gd.gd_train_data where line_name='线路"+road+"';"
			f.write(sql_str+'\n')
			sql+=sql_str
		return sql

	#用户距离截止时间的前（1/2/3/4/5/6/7/10/15/30/60/全部 ）乘坐车的次数
	#用户在前1天，前5天，前7天，前30天乘车次数占全部乘车次数的比例
	def last_recent_day_count_sql(self,road):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_recent_day_count.txt','wb')
		#print timestamp
		sql=""
		sql+="drop table if exists recent_day_count_road_"+road+"; create table recent_day_count_road_"+road+" as select \n"
		inner_sql=""
		inner_select="a.card_id"
		inner_sql+="(select distinct(card_id) from gd_train_data_"+road+") a \n"
		for x in recent_day:
			r_timestamp=timestamp-x*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			sql_str=u"join (select Card_id, count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as day_count_"+str(x)+" from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) table_day_count_"+str(x)+" on a.card_id=table_day_count_"+str(x)+".card_id \n"
			inner_select+=", day_count_"+str(x)
			if x==1 or x==5 or x==7 or x==30:
				inner_select+=", (cast(day_count_"+str(x)+" as double) / cast(day_count_365 as double)) as day_count_percent_"+str(x)
			inner_sql+=sql_str
		
		sql+=inner_select+"\n from \n"+inner_sql+";"
		f.write(sql)
		return sql+'\n'


	#用户在前1天，前5天，前7天，前30天乘车次数占全部乘车次数的比例
	def last_recent_day_count_percent_sql(self,road):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_recent_day_count_percent.txt','wb')
		sql=""
		# count_1/count_365 count_5/count_365 count_7/count_365 count_30/count_365
		sql+="drop table if exists recent_day_count_percent_road_"+road+"; \n"
		sql+="create table recent_day_count_percent_road_"+road+" as \n"
		sql+="select recent_day_count_road_"+road+"_365.card_id, \n"
		sql+="(cast(recent_day_count_road_"+road+"_1.day_count_1 as double)/cast(recent_day_count_road_"+road+"_365.day_count_365 as double)) as day_count_percent_1, \n"
		sql+="(cast(recent_day_count_road_"+road+"_5.day_count_5 as double)/cast(recent_day_count_road_"+road+"_365.day_count_365 as double)) as day_count_percent_5, \n"
		sql+="(cast(recent_day_count_road_"+road+"_7.day_count_7 as double)/cast(recent_day_count_road_"+road+"_365.day_count_365 as double)) as day_count_percent_7, \n"
		sql+="(cast(recent_day_count_road_"+road+"_30.day_count_30 as double)/cast(recent_day_count_road_"+road+"_365.day_count_365 as double)) as day_count_percent_30 \n"
		sql+=" from recent_day_count_road_"+road+"_365"
		sql+=" left outer join recent_day_count_road_"+road+"_1 on recent_day_count_road_"+road+"_365.card_id=recent_day_count_road_"+road+"_1.card_id\n"
		sql+=" left outer join recent_day_count_road_"+road+"_5 on recent_day_count_road_"+road+"_365.card_id=recent_day_count_road_"+road+"_5.card_id\n"
		sql+=" left outer join recent_day_count_road_"+road+"_7 on recent_day_count_road_"+road+"_365.card_id=recent_day_count_road_"+road+"_7.card_id\n"
		sql+=" left outer join recent_day_count_road_"+road+"_30 on recent_day_count_road_"+road+"_365.card_id=recent_day_count_road_"+road+"_30.card_id;\n"

		f.write(sql)
		return sql

	#用户距离截止时间前1个月内每天乘车次数超过两次以上的次数，1周，1月内换一种计算方法，多次计算减去一次计算的次数
	def count_beyond_two_times_sql(self,road):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_count_beyond_two_times.txt','wb')
		sql=""
		sql+="drop table if exists count_beyond_two_times_road_"+road+"; "
		sql+="create table count_beyond_two_count_road_"+road+" as select a.card_id,a.beyond_two_count_7,b.beyond_two_count_30 from "
		r_timestamp=timestamp-7*86400
		d=datetime.fromtimestamp(float(r_timestamp))
		dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
		sql+="(select Card_id, (count(*)-count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD')))) as beyond_two_count_7 from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) a join "
		r_timestamp=timestamp-30*86400
		d=datetime.fromtimestamp(float(r_timestamp))
		dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
		sql+="(select Card_id, (count(*)-count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD')))) as beyond_two_count_30 from gd_train_data_"+road+" where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' group by Card_id) b on a.card_id=b.card_id;"
		
		f.write(sql)
		return sql

	def weekday_weekend_count_sql(self,road):
		base_day=self.__base_day
		recent_day=self.__recent_day
		d_base=datetime.strptime(str(base_day),'%Y%m%d')
		timestamp=time.mktime(d_base.timetuple())
		f=file(u'sql/'+base_day+'_road_'+road+'_weekday_weekend_count.txt','wb')
		time_data={'1_weekday':[],'2_weekday':[],'3_weekday':[],'4_weekday':[],'1_weekend':[],'2_weekend':[],'3_weekend':[],'4_weekend':[]}
		sql="drop table if exists weekday_weekend_count_road_"+road+";\n create table weekday_weekend_count_road_"+road+" as select \n"
		inner_sql=""
		inner_select=""
		for i in range(4):
			j=(i+1)*7
			index=str(i+1)
			r_timestamp=timestamp-j*86400
			d=datetime.fromtimestamp(float(r_timestamp))
			dStr=str(d.year)+self.add_zero(d.month)+self.add_zero(d.day)+'05'
			if i==0:
				inner_select+="weekday_1.card_id, weekday_"+index+".weekday_count_"+index+", weekend_"+index+".weekend_count_"+index+", cast(weekday_"+index+".weekday_count_"+index+" as double)/cast((weekday_"+index+".weekday_count_"+index+")+(weekend_"+index+".weekend_count_"+index+") as double) as weekday_percent_"+index+" \n"
			else:
				inner_select+=" ,weekday_"+index+".weekday_count_"+index+", weekend_"+index+".weekend_count_"+index+", cast(weekday_"+index+".weekday_count_"+index+" as double)/cast((weekday_"+index+".weekday_count_"+index+")+(weekend_"+index+".weekend_count_"+index+") as double) as weekday_percent_"+index+" \n"

			if i==0:
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekday_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 group by card_id) weekday_"+index+" join \n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekend_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))>=5 group by card_id) weekend_"+index+" on weekday_1.card_id=weekend_"+index+".card_id join \n"
			elif i==3:
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekday_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 group by card_id) weekday_"+index+" on weekday_1.card_id=weekday_"+index+".card_id join \n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekend_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))>=5 group by card_id) weekend_"+index+" on weekday_1.card_id=weekend_"+index+".card_id;\n"
			else:
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekday_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))<5 group by card_id) weekday_"+index+" on weekday_1.card_id=weekday_"+index+".card_id join \n"
				inner_sql+="(select card_id,count(distinct(datetrunc(to_date(deal_time,'yyyymmddhh'),'DD'))) as weekend_count_"+index+" from gd_train_data_"+road+"\n where deal_time>='"+dStr+"' and deal_time<'"+base_day+"05' and weekday(to_date(deal_time,'yyyymmddhh'))>=5 group by card_id) weekend_"+index+" on weekday_1.card_id=weekend_"+index+".card_id join \n"

		sql+=inner_select+" from\n "+inner_sql
		f.write(sql)
		return sql

	#正例集
	def positive_set_sql(self,base_day1,base_day2,road):
		base_day=self.__base_day
		f=file(u'sql/'+base_day+'_road_'+road+'_positive_set.txt','wb')
		d_base1=datetime.strptime(str(base_day1),'%Y%m%d')
		dStr1=str(d_base1.year)+self.add_zero(d_base1.month)+self.add_zero(d_base1.day)+'05'
		d_base2=datetime.strptime(str(base_day2),'%Y%m%d')
		timestamp=time.mktime(d_base2.timetuple())+86400.0
		d_base2=datetime.fromtimestamp(float(timestamp))
		dStr2=str(d_base2.year)+self.add_zero(d_base2.month)+self.add_zero(d_base2.day)+'05'
		sql="drop table if exists positive_set_road_"+road+"; create table positive_set_road_"+road+" as select distinct(card_id) from gd_train_data_"+road+" where deal_time>='"+dStr1+"' and deal_time<'"+dStr2+"';"
		f.write(sql)
		return sql+'\n'

	#训练集所有用户
	def train_alluser_set_sql(self,base_day1,base_day2,road):
		base_day=self.__base_day
		f=file(u'sql/'+base_day+'_road_'+road+'_train_alluser_set.txt','wb')
		d_base1=datetime.strptime(str(base_day1),'%Y%m%d')
		dStr1=str(d_base1.year)+self.add_zero(d_base1.month)+self.add_zero(d_base1.day)+'05'
		d_base2=datetime.strptime(str(base_day2),'%Y%m%d')
		timestamp=time.mktime(d_base2.timetuple())+86400.0
		d_base2=datetime.fromtimestamp(float(timestamp))
		dStr2=str(d_base2.year)+self.add_zero(d_base2.month)+self.add_zero(d_base2.day)+'05'
		sql="drop table if exists train_alluser_set_road_"+road+"; create table train_alluser_set_road_"+road+" as select distinct(card_id) from gd_train_data_"+road+" where deal_time>='"+dStr1+"' and deal_time<'"+dStr2+"';"
		f.write(sql)
		return sql+'\n'

	#训练集&&正例集=所有用户的标签
	def classify_label_sql(self,base_day1,base_day2,base_day3,base_day4,road):
		base_day=self.__base_day

		f=file(u'sql/'+base_day+'_road_'+road+'_classify_label.txt','wb')
		sql="drop table if exists label_road_"+road+"; \ncreate table label_road_"+road+" as select \n"
		sql+=" card_id,regexp_replace(cast(length(card_id2) as string),'32','1') as label  from \n "
		d_base1=datetime.strptime(str(base_day3),'%Y%m%d')
		dStr1=str(d_base1.year)+self.add_zero(d_base1.month)+self.add_zero(d_base1.day)+'05'
		d_base2=datetime.strptime(str(base_day4),'%Y%m%d')
		timestamp=time.mktime(d_base2.timetuple())+86400.0
		d_base2=datetime.fromtimestamp(float(timestamp))
		dStr2=str(d_base2.year)+self.add_zero(d_base2.month)+self.add_zero(d_base2.day)+'05'
		sql+="(select distinct(card_id) as card_id from gd_train_data_"+road+" where deal_time>='"+dStr1+"' and deal_time<'"+dStr2+"') a\n left outer join \n"
		d_base1=datetime.strptime(str(base_day1),'%Y%m%d')
		dStr1=str(d_base1.year)+self.add_zero(d_base1.month)+self.add_zero(d_base1.day)+'05'
		d_base2=datetime.strptime(str(base_day2),'%Y%m%d')
		timestamp=time.mktime(d_base2.timetuple())+86400.0
		d_base2=datetime.fromtimestamp(float(timestamp))
		dStr2=str(d_base2.year)+self.add_zero(d_base2.month)+self.add_zero(d_base2.day)+'05'
		sql+="(select distinct(card_id) as card_id2 from gd_train_data_"+road+" where deal_time>='"+dStr1+"' and deal_time<'"+dStr2+"') b on a.card_id=b.card_id2;\n"
		f.write(sql)
		return sql+'\n'

	#将所有属性列与标签列合并到一个表
	def column_concat_sql(self,road,type):
		base_day=self.__base_day
		f=file(u'sql/'+base_day+'_road_'+road+'column_concat.txt','wb')
		sql="drop table if exists "+type+"_data_road_"+road+"; \n"
		sql+="create table "+type+"_data_road_"+road+" as \n"
		sql+="select recent_day_count_road_"+road+".*, recent_day_count_percent_road_"+road+".*, count_beyond_two_times_road_"+road+".*, weekday_weekend_count_road_"+road+".*, label_road_"+road+".label \n"
		sql+="from label_road_"+road+"\n"
		sql+=" join recent_day_count_road_"+road+" on label_road_"+road+".card_id=recent_day_count_road_"+road+".card_id\n"
		sql+=" join recent_day_count_percent_road_"+road+"on label_road_"+road+".card_id=recent_day_count_percent_road_"+road+".card_id\n"
		sql+=" join count_beyond_two_times_road_"+road+"on label_road_"+road+".card_id=count_beyond_two_times_road_"+road+".card_id\n"
		sql+=" join weekday_weekend_count_road_"+road+"on label_road_"+road+".card_id=weekday_weekend_count_road_"+road+".card_id;\n"
		f.write(sql)
		return sql+'\n'

	def coumun_concat_sql_predict(self,road,type):
		base_day=self.__base_day
		f=file(u'sql/'+base_day+'_road_'+road+'column_concat_predict.txt','wb')
		sql="drop table if exists "+type+"_data_road_"+road+"_predict; \n"
		sql+="create table "+type+"_data_road_"+road+"_predict as \n"
		sql+="select recent_day_count_road_"+road+".*, recent_day_count_percent_road_"+road+".*, count_beyond_two_times_road_"+road+".*, weekday_weekend_count_road_"+road+".* \n"
		sql+="from recent_day_count_road_"+road
		sql+=" join recent_day_count_percent_road_"+road+"on recent_day_count_road_"+road+".card_id=recent_day_count_percent_road_"+road+".card_id"
		sql+=" join count_beyond_two_times_road_"+road+"on recent_day_count_road_"+road+".card_id=count_beyond_two_times_road_"+road+".card_id"
		sql+=" join weekday_weekend_count_road_"+road+"on recent_day_count_road_"+road+".card_id=weekday_weekend_count_road_"+road+".card_id;"
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
	base_day='20141224'
	for road in roads:
	#road='10'
	#正例集
		base_day1='20141225'
		base_day2='20141231'
		#所有用户集
		base_day3='20140801'
		base_day4='20141224'
		get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,'test')

def get_predict_data(base_day,road):
	#base_day='20141231'
	#road='10'
	sql_instance=Sql_creation(base_day)
	sql_arr=[]
	#计算不同路的用户最近1-60天的乘坐次数
	sql_arr.append(sql_instance.last_recent_day_count_sql(road))
	#合并为一个表
	sql_arr.append(sql_instance.concat_recent_day_count_sql_predict(road))
	f=file(u'sql/all_road_predict_data_sql.txt','a+')
	for sql in sql_arr:
		f.write(sql+'\n')

def get_data(base_day,road,base_day1,base_day2,base_day3,base_day4,type):
	sql_instance=Sql_creation(base_day)
	sql_arr=[]
	
	#计算不同路的用户最近1-60天的乘坐次数
	sql_arr.append(sql_instance.last_recent_day_count_sql(road))
	#percent
	sql_arr.append(sql_instance.last_recent_day_count_percent_sql(road))
	#two times
	sql_arr.append(sql_instance.count_beyond_two_times_sql(road))
	#weekday weeken
	sql_arr.append(sql_instance.weekday_weekend_count_sql(road))
	#获取label
	sql_arr.append(sql_instance.classify_label_sql(base_day1,base_day2,base_day3,base_day4,road))

	sql_arr.append(sql_instance.column_concat_sql(road,type))

	f=file(u'sql/road_'+road+'_'+type+'_data_sql.txt','wb')
	for sql in sql_arr:
		f.write(sql+'\n')
		#run_sql(sql)

def main():
	get_test_data()
	#get_predict_data()
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
	# sql_instance.classify_label_sql(base_day1,base_day2,base_day3,base_day4,'10')


if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()


	# base_day='20141217'
	# roads=['12','15','2','8','10','4','7']
	# sql_instance=Sql_creation(base_day)
	# #分开不同线路的交通数据
	# #sql=sql_instance.split_road_data()
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