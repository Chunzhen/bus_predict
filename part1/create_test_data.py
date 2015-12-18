#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 生成测试数据

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class Create_test_data(object):
	def __init__(self):
		pass

	def handle_data(self):
		self.output_test_data()

	def weather_data(self):
		weather={}
		data=np.loadtxt('files/weather.txt',delimiter=',')
		m,n=data.shape
		for i in range(m):
			d=datetime.fromtimestamp(float(data[i,0]))
			if d.month==8 and (d.day==11 or d.day==13 or d.day==15 or d.day==18):
				continue
			day=str(d.year)+'-'+self.add_zero(d.month)+'-'+self.add_zero(d.day)
			row=data[i,1:].astype('int').tolist()
			row.append(int(abs(data[i,3]-data[i,4])))
			weather[day]=row
		return weather

	def add_zero(self,s):
		t=str(s)
		if s<10:
			t='0'+t
		return t

	def output_test_data(self):
		holiday={'2014-09-06':1,'2014-09-07':1,'2014-09-08':1,'2014-10-01':1,'2014-10-02':1,'2014-10-03':1,'2014-10-04':1,'2014-10-05':1,'2014-10-06':1,'2014-10-07':1,'2015-01-01':1,'2015-01-02':1,'2015-01-03':1}
		overtime={'2014-09-28':1,'2014-10-11':1,'2015-01-04':1}
		weather=self.weather_data()
		start=time.mktime(datetime.strptime('2015-1-1','%Y-%m-%d').timetuple())
		end=time.mktime(datetime.strptime('2015-1-7','%Y-%m-%d').timetuple())

		r_data=[]
		holiday_data=[]
		overtime_data=[]
		continueHoliday=0
		for i in range(7):
			d=datetime.strptime('2015-1-'+str(i+1),'%Y-%m-%d')
			dStr=str(d.year)+'-'+self.add_zero(d.month)+'-'+self.add_zero(d.day)
			temestamp=int(time.mktime(d.timetuple()))
			year_week=int(d.strftime("%W"))+1 #52
			weekday=d.weekday()+1
			
			isHoliday=holiday.get(dStr,0)
			isOverTime=overtime.get(dStr,0)
			if isHoliday:
				continueHoliday+=1
				if continueHoliday>32:
					continueHoliday=32
			else:
				continueHoliday=0

			# if isHoliday and (weekday!=6 and weekday!=7):
			# 	if continueHoliday:
			# 		weekday=7
			# 	else:
			# 		weekday=6
			# 	pass
			
			if isOverTime:
				weekday=4

			holiday_len=0
			if (d.month==1 and d.day>0 and d.day<4):
				holiday_len=3

			hourWeather=weather[dStr]
			for j in range(16):		
				hour=j+6
				
				row=[temestamp,year_week,hour,weekday,continueHoliday/16,isOverTime,temestamp,isHoliday,holiday_len]

				row.extend(hourWeather)
				r_data.append(row)
				# if isHoliday:
				# 	#print "holiday"
				# 	holiday_data.append(row)
				# elif isOverTime:
				# 	overtime_data.append(row)
				# else:
				# 	r_data.append(row)

		df=pd.DataFrame(r_data)
		df.to_csv('files/test_data'+'.txt',sep=',',mode='w',header=None,index=False)

		df=pd.DataFrame(overtime_data)
		df.to_csv('files/test_data_overtime.txt',sep=',',mode='w',header=None,index=False)

		df=pd.DataFrame(holiday_data)
		df.to_csv('files/test_data_holiday.txt',sep=',',mode='w',header=None,index=False)