#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 区分两条线路的数据
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class Split_road_data(object):
	def __init__(self):
		self.__roadList=self._get_road_list()
		#print self.__roadList

	def handle_data(self):
		self._read_data_by_pandas()
		pass

	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def _read_data_by_pandas(self):
		try:
			for x in self.__roadList:
				os.remove('data_road_'+str(x)+'.txt')
		except WindowsError:
			pass

		loop=True
		chunkSize=1000000
		i=0
		reader=pd.read_csv('files/data.txt',iterator=True,delimiter=',',encoding='utf-8',skiprows=-1,header=None)
		while loop:	
			try:
				df=reader.get_chunk(chunkSize)
				
				for x  in self.__roadList:
					df[df[df.columns[0]]==x].to_csv('files/data_road_'+str(x)+'.txt',sep=',',mode='a+',header=None,index=False)
				i+=1
				print unicode(str(i*100)+"万")	

			except StopIteration:
				loop=False
				print "Iteration is stopped"