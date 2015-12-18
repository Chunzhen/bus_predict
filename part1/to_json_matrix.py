#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 生成像素矩阵 json格式的数据
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time
import json

class To_json_matrix(object):
	def __init__(self):
		pass

	def handle_data(self):
		roadList=self._get_road_list()
		for x in roadList:
			self.matrix_data(str(x))

	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def matrix_data(self,road):
		holiday={'2014-09-06':1,'2014-09-07':1,'2014-09-08':1,'2014-10-01':1,'2014-10-02':1,'2014-10-03':1,'2014-10-04':1,'2014-10-05':1,'2014-10-06':1,'2014-10-07':1,'2015-01-01':1,'2015-01-02':1,'2015-01-03':1}
		overtime={'2014-09-28':1,'2014-10-11':1,'2015-01-04':1}
		data=np.loadtxt('files/statistic_data_'+road+'.txt',delimiter=',',usecols=(1,2))
		m,n=data.shape
		day_max=0
		for i in range(n-1):
			t_max=np.max(data[:,(i+1)])
			day_max=int(t_max)

		day_min=0
		for i in range(n-1):
			t_min=np.min(data[:,(i+1)])
			day_min=int(t_min)

		day_data=[]
		all_year_week=set()

		for i in range(m):
			d=datetime.fromtimestamp(float(data[i,0]))
			dStr=str(d.year)+'-'+self.add_zero(d.month)+'-'+self.add_zero(d.day)
			hour=str(d.hour)
			week=d.weekday()+1
			year_week=d.strftime("%W")
			all_year_week.add(int(year_week))
			isHoliday=holiday.get(dStr,0)
			isOverTime=overtime.get(dStr,0)
			passanger_count=int(data[i,1])
			day_timestamp=int(data[i,0])
			row=[dStr,int(year_week),week,int(hour),isHoliday,isOverTime,passanger_count]
			day_data.append(row)

		sorted(all_year_week)
		all_year_week=list(all_year_week)
		dict_all_year_week=dict()
		index=0
		for i in all_year_week:
			dict_all_year_week[str(i)]=index
			index+=1
		json_data={'all_year_week':dict_all_year_week,'max':day_max,'min':day_min,'data':day_data}
		with open('F:/wamp/www/bus_predict_contest/json/matrix_data_'+road+'.json','w') as f:
		 	f.write(json.dumps(json_data))

	def add_zero(self,s):
		t=str(s)
		if s<10:
			t='0'+t
		return t

def main():
	to_json_matrix_instance=To_json_matrix()
	to_json_matrix_instance.handle_data()
	pass

if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()





