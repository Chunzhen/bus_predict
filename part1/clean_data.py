#! /usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class Clean_data(object):
	def __init__(self):
		self.__busList={}
		self.__userList={}
		self.__useCity={}
		self.__cardType={}
		self.__roadList={}

	def _read_data_by_pandas(self):
		try:
			os.remove('files/data.txt')
		except WindowsError:
			pass
		loop=True
		chunkSize=1000000
		i=0
		reader=pd.read_csv('../gd_train_data.txt',iterator=True,delimiter=',',encoding='utf-8',usecols=(1,2,3,4,5,6),skiprows=-1,header=None)
		while loop:	
			try:
				start=datetime.now()
				df=reader.get_chunk(chunkSize)

				df[df.columns[0]]=(df[df.columns[0]]).apply(self._check_road)
				df[df.columns[1]]=(df[df.columns[1]]).apply(self._check_bus)
				df[df.columns[2]]=(df[df.columns[2]]).apply(self._check_user)
				df[df.columns[3]]=(df[df.columns[3]]).apply(self._check_city)
				df[df.columns[4]]=(df[df.columns[4]]).apply(self._check_date)
				df[df.columns[5]]=(df[df.columns[5]]).apply(self._check_card)

				df.to_csv('files/data.txt',sep=',',mode='a+',header=None,index=False)
				i+=1
				#if i>2:
					#break
				end=datetime.now()
				print unicode(str(i*100)+"万")			
				print "Run time:"+str(float((end-start).seconds)/60.0)+"min / "+str(float((end-start).seconds))+"s"
				#break
			except StopIteration:
				loop=False
				print "Iteration is stopped"

	def handle_data(self):
		start=datetime.now()
		self._read_data_by_pandas()
		self._save_roadList()
		self._save_busList()
		self._save_userList()
		self._save_useCity()
		self._save_cardType()
		end=datetime.now()
		print "All Run time:"+str(float((end-start).seconds)/60.0)+"min / "+str(float((end-start).seconds))+"s"

	def _check_road(self,s):
		s=s.strip()
		s=s.replace(u'线路','')
		index=self.__roadList.get(s,-1)
		if index==-1:
			index=1
			self.__roadList[s]=index
		return s

	def _check_bus(self,s):
		s=s.strip()
		index=self.__busList.get(s,-1)
		if index==-1:
			index=len(self.__busList)+1
			self.__busList[s]=index
		return index

	def _check_user(self,s):
		s=s.strip()
		index=self.__userList.get(s,-1)
		if index==-1:
			index=len(self.__userList)+1
			self.__userList[s]=index
		return index

	def _check_city(self,s):
		s=s.strip()
		index=self.__useCity.get(s,-1)
		if index==-1:
			index=len(self.__useCity)+1
			self.__useCity[s]=index
		return index

	def _check_date(self,s):
		date=datetime.strptime(str(s),'%Y%m%d%H')
		timestamp=time.mktime(date.timetuple())
		return int(timestamp)

	def _check_card(self,s):
		s=s.strip()
		index=self.__cardType.get(s,-1)
		if index==-1:
			index=len(self.__cardType)+1
			self.__cardType[s]=index
		return index

	def _save_roadList(self):
		f=file('files/road_list.txt','wb')
		for v,k in self.__roadList.items():
			f.write(str(k)+','+str(v)+'\n')
		f.close()

	def _save_busList(self):
		f=file('files/bus_list.txt','wb')
		for v,k in self.__busList.items():
			f.write(str(k)+','+str(v)+'\n')
		f.close()

	def _save_userList(self):
		f=file('files/user_list.txt','wb')
		for v,k in self.__userList.items():
			f.write(str(k)+','+str(v)+'\n')
		f.close()

	def _save_useCity(self):
		f=file('files/use_city.txt','wb')
		for v,k in self.__useCity.items():
			f.write(str(k)+','+str(v)+'\n')
		f.close()

	def _save_cardType(self):
		f=file('files/card_type.txt','wb')
		for v,k in self.__cardType.items():
			f.write(str(k)+','+str(v)+'\n')
		f.close()

