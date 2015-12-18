#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 只选择6-21时的数据
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class Time_select(object):
	def __init__(self):
		pass

	def handle_data(self):
		roadList=self._get_road_list()
		for x in roadList:
			self._load_count_data(str(x))

	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def _load_count_data(self,road):
		try:
			os.remove('files/selected_data_'+road+'.txt')
		except WindowsError:
			pass
		loop=True
		chunkSize=1000
		i=0
		reader=pd.read_csv('files/countData_'+road+'.txt',iterator=True,delimiter=',',encoding='utf-8',skiprows=-1,header=None)
		while loop:
			try:
				df=reader.get_chunk(chunkSize)
				df[(df[df.columns[1]]).apply(self._time_select)].to_csv('files/selected_data_'+road+'.txt',sep=',',mode='a+',header=None,index=False)

			except StopIteration:
				loop=False
				print "Iteration is stopped"

	def _time_select(self,s):
		d=datetime.fromtimestamp(float(s))
		if d.hour>=6 and d.hour<=21:
			return True
		else:
			return False