#! usr/bin/env python
# -*- coding: utf-8 -*-
# coding:utf-8

import sys
import os
import numpy as np
import csv
from datetime import datetime
import time

class Weather(object):
	def __init__(self):
		self.__weatherType={'晴':0,'多云':1,'阴':2,'雷阵雨':3,'阵雨':4,'小雨':5,'小到中雨':5,'中雨':6,'中雨到大雨':7,'大雨':8,'大到暴雨':9,'霾':10}

	def _read_data(self,filePath):
		weatherType=self.__weatherType
		file=open(filePath)
		w_type=[]
		l=[]
		for line in file.readlines():
			currentLine=line.strip().split(',')
			#print len(currentLine)
			if len(currentLine)>1:
				row=[]
				for i,c_v in enumerate(currentLine):
					if i==0:
						date=datetime.strptime(c_v,'%Y/%m/%d')
						timestamp=time.mktime(date.timetuple())
						row.append(int(timestamp))
					elif i==1:
						w=str(c_v).split('/')
						for w_i in w:	
							if w_i not in w_type:
								print unicode(w_i)
								w_type.append(w_i)
							v=0				
							for d,x in weatherType.items():
								if d in w_i:
									v=x
							row.append(v)
					elif i==2:
						t=c_v.split('/')
						for t_v in t:
							t_v=t_v.replace('℃','')
							row.append(int(t_v))
				l.append(row)
		return l,w_type

	def _save_weather_data(self,data):
		writer=csv.writer(file('files/weather.txt','wb'))
		for x in data:
			writer.writerow(x)

	def _save_weather_type(self,data):
		f=file('files/weather_type.txt','wb')
		for x in data:
			f.write(x+'\n')
		f.close()

	def handle_data(self):
		filePath='../gd_weather_report.txt'
		l,w=self._read_data(filePath)
		self._save_weather_data(l)
		self._save_weather_type(w)

