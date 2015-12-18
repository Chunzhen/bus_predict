#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 回归模型输入变量，以每个小时数据为一行
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class To_regression(object):
	def __init__(self):
		self.__holiday={'2014-09-06':1,'2014-09-07':1,'2014-09-08':1,'2014-10-01':1,'2014-10-02':1,'2014-10-03':1,'2014-10-04':1,'2014-10-05':1,'2014-10-06':1,'2014-10-07':1}
		self.__overtime={'2014-09-28':1,'2014-10-11':1}

	def handle_data(self):
		roadList=self._get_road_list()
		for x in roadList:
			self._hour_data(str(x))

	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def _hour_data(self,road):
		holiday=self.__holiday
		overtime=self.__overtime
		weather=self._weather_data()
		data=np.loadtxt('files/statistic_data_'+road+'.txt',delimiter=',',usecols=(1,2,3,5,7,8,9,10,11))
		m,n=data.shape

		r_data=[]
		holiday_data=[]
		overtime_data=[]

		holiday_count=0
		overtime_count=0
		normal_count=0
		for i in range(m):
			d=datetime.fromtimestamp(float(data[i,0]))
			dStr=str(d.year)+'-'+self.add_zero(d.month)+'-'+self.add_zero(d.day)
			if d.month==8 and (d.day==11 or d.day==13 or d.day==15 or d.day==18):
				continue
			isHoliday=holiday.get(dStr,0)
			isOverTime=overtime.get(dStr,0)
			if isHoliday:
				holiday_count+=1
			elif isOverTime:
				overtime_count+=1
			else:
				normal_count+=1

		print "holiday_count:"+str(holiday_count)
		print "overtime_count:"+str(overtime_count)
		print "normal_count:"+str(normal_count)

		holiday_add_sample=int(normal_count/holiday_count)
		overtime_add_sample=int(normal_count/overtime_count)
		holiday_add_sample=1
		overtime_add_sample=1

		continueHoliday=0
		for i in range(m):
			d=datetime.fromtimestamp(float(data[i,0]))
			dStr=str(d.year)+'-'+self.add_zero(d.month)+'-'+self.add_zero(d.day)
			if (d.month==8 and (d.day==11 or d.day==13 or d.day==15 or d.day==18)) or (d.month==9 and d.day==23):
				continue
			
			hour=d.hour
			year_week=d.strftime("%W")
			weekday=d.weekday()+1
			
			isHoliday=holiday.get(dStr,0)
			isOverTime=overtime.get(dStr,0)
			# if isHoliday:
			# 	continue
			if isHoliday:
				continueHoliday+=1
			else:
				continueHoliday=0

			# if isHoliday and (weekday!=6 and weekday!=7):
			# 	if continueHoliday>1:
			# 		weekday=7
			# 	else:
			# 		weekday=6
			# 	pass
			
			if isOverTime:
				weekday=4

			holiday_len=0
			if (d.month==9 and d.day>5 and d.day<9):
				holiday_len=3
			elif (d.month==10 and d.day>0 and d.day<8):
				holiday_len=7
			row=[year_week,hour,weekday,continueHoliday/16,isOverTime,data[i,0],isHoliday,holiday_len] #data[i,0]

			hourWeather=weather[dStr]
			d2=datetime.fromtimestamp(float(data[i,0])-86400.0)
			dStr2=str(d2.year)+'-'+self.add_zero(d2.month)+'-'+self.add_zero(d2.day)

			row.extend(hourWeather)

			row.append(int(data[i,1]))
			if isHoliday:
				for j in range(holiday_add_sample):
					r_data.append(row)
			elif isOverTime:
				for j in range(overtime_add_sample):
					r_data.append(row)
			else:
				r_data.append(row)

		df=pd.DataFrame(r_data)
		df.to_csv('files/regression_'+road+'.txt',sep=',',mode='w',header=None,index=False)


	def add_zero(self,s):
		t=str(s)
		if s<10:
			t='0'+t
		return t

	def _weather_data(self):
		weather={}
		data=np.loadtxt('files/weather.txt',delimiter=',')
		m,n=data.shape
		for i in range(m):
			d=datetime.fromtimestamp(float(data[i,0]))
			if d.month==8 and (d.day==11 or d.day==13 or d.day==15 or d.day==18):
				continue
			day=str(d.year)+'-'+self.add_zero(d.month)+'-'+self.add_zero(d.day)
			row=data[i,1:].astype('int').tolist()
			#row.append(int(abs(data[i,1]-data[i,2])))
			row.append(int(abs(data[i,3]-data[i,4])))
			weather[day]=row
		return weather