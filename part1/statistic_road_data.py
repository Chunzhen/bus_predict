#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 统计属性值

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class Statistic_road_data(object):
	def __init__(self):
		pass

	def handle_data(self):
		roadList=self._get_road_list()
		for x in roadList:
			self._load_selected_data(str(x))

	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def _load_selected_data(self,road):
		try:
			os.remove('files/statistic_data_'+road+'.txt')
		except WindowsError:
			pass
		loop=True
		chunSize=1000
		i=0
		reader=pd.read_csv('files/selected_data_'+road+'.txt',iterator=True,delimiter=',',encoding='utf-8',skiprows=-1,header=None)
		while loop:
			try:
				df=reader.get_chunk(chunSize)
				df_bus=(df[df.columns[4]]).apply(self._statistic_bus)
				df_bus=pd.DataFrame(df_bus)

				df_cardType=(df[df.columns[8]]).apply(self._statistic_card_type)
				df_cardType=pd.DataFrame(df_cardType)

				df=df.drop([df.columns[4],df.columns[5],df.columns[6],df.columns[8]],axis=1)

				new_df=pd.concat([df,df_bus,df_cardType],ignore_index=True,axis=1)
				new_df.to_csv('files/statistic_data_'+road+'.txt',sep=',',mode='a+',header=None,index=False)
				#break
			except StopIteration:
				loop=False
				print "Iteration is stopped"

	#统计最多，最小，平均，中位载客量，最大值与最小值的差值，方差
	def _statistic_bus(self,s):
		arr=str(s).split(' ')
		arr=np.array(arr,dtype='int')
		arr_max=np.max(arr)
		arr_min=np.min(arr)
		arr_ptp=arr_max-arr_min
		arr_median=np.median(arr)
		arr_mean=np.mean(arr)
		arr_var=np.var(arr)
		l=[arr_max,arr_min,arr_ptp,arr_median,arr_mean,arr_var]
		new_arr=np.array(l,dtype='int')
		return pd.Series(new_arr) 

	def _statistic_card_type(self,s):
		#print s
		arr=str(s).split(' ')
		arr=np.array(arr,dtype='int')
		arr_max=np.max(arr)
		l=[arr_max]
		new_arr=np.array(l,dtype='int')
		return pd.Series(new_arr) 