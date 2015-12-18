#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 计算每个小时的客流

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

class Count_road_data(object):
	def __init__(self):
		pass

	def handle_data(self):
		roadList=self._get_road_list()
		for x in roadList:
			hourData=self._load_road_data(str(x))
			self._handle_count_data(str(x),hourData)


	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def _load_road_data(self,road):
		hourData={}
		loop=True
		chunkSize=1000000
		i=0
		reader=pd.read_csv('files/data_road_'+road+'.txt',iterator=True,delimiter=',',encoding='utf-8',usecols=(1,2,3,4,5),skiprows=-1,header=None)
		while loop:
			try:
				start=datetime.now()
				df=reader.get_chunk(chunkSize)
				#print df
						
				for ai,group in df.iterrows():
					name=group[df.columns[3]]		
					rowData=hourData.get(str(name),{})
					#print rowData
					if len(rowData)==0:
						rowData={'count':1,'bus':{},'useCity':{},'cardType':{}}
						rowData['bus'][str(group[df.columns[0]])]=1
						rowData['useCity'][str(group[df.columns[2]])]=1
						rowData['cardType'][str(group[df.columns[4]])]=1

					else:
						rowData['count']=rowData.get('count',0)+1
						temp=rowData['bus'].get(str(group[df.columns[0]]),-1)
						if temp == -1:
							rowData['bus'][str(group[df.columns[0]])]=1
						else:
							rowData['bus'][str(group[df.columns[0]])]=temp+1

						temp=rowData['useCity'].get(str(group[df.columns[2]]),-1)
						if temp == -1:
							rowData['useCity'][str(group[df.columns[2]])]=1
						else:
							rowData['useCity'][str(group[df.columns[2]])]=temp+1

						temp=rowData['cardType'].get(str(group[df.columns[4]]),-1)
						if temp == -1:
							rowData['cardType'][str(group[df.columns[4]])]=1
						else:
							rowData['cardType'][str(group[df.columns[4]])]=temp+1

					hourData[str(name)]=rowData
				i+=1
				print unicode(str(i*100)+"万")	
				end=datetime.now()					
				print "Run time:"+str(float((end-start).seconds)/60.0)+"min / "+str(float((end-start).seconds))+"s"					

			except StopIteration:
				loop=False
				print "Iteration is stopped"

		return hourData

	def _handle_count_data(self,road,hourData):
		filePath='files/countData_'+road+'.txt'
		f=file(filePath,'wb')
		data=sorted(hourData.items(), key=lambda hourData:hourData[0])

		for row in data:
			date=row[0]	
			rowData=row[1]
			#print rowData

			d=datetime.fromtimestamp(float(date))
			busCount=len(rowData['bus'])
			#passangerCount=len(rowData['passangerCount'])
			useCityCount=len(rowData['useCity'])
			cardTypeCount=len(rowData['cardType'])
			#每辆车的运载客数目
			busStr=''
			for k,v in rowData['bus'].items():
				busStr+=str(v)+' '
			busStr=busStr.strip()
			#每个客人上车的次数
			#passangerStr=''
			#for k,v in rowData['passangerCount'].items():
			#	passangerStr+=str(v)+' '
			#passangerStr=passangerStr.strip()
			#发卡城市人上车的数目
			useCityStr=''
			for k,v in rowData['useCity'].items():
				useCityStr+=str(v)+' '
			useCityStr=useCityStr.strip()
			#不同类型卡的乘客数目
			cardTypeStr=''
			for k,v in rowData['cardType'].items():
				cardTypeStr+=str(v)+' '	
			cardTypeStr=cardTypeStr.strip()
			
			dStr=str(d.year)+'-'+str(d.month)+'-'+str(d.day)+':'+str(d.hour)

			#s=dStr+','+str(date)+','+str(rowData['count'])+','+str(busCount)+','+busStr+','+str(passangerCount)+','+passangerStr+','+str(useCityCount)+','+useCityStr+','+str(cardTypeCount)+','+cardTypeStr+'\n'
			s=dStr+','+str(date)+','+str(rowData['count'])+','+str(busCount)+','+busStr+','+str(useCityCount)+','+useCityStr+','+str(cardTypeCount)+','+cardTypeStr+'\n'
			f.write(s)
		f.close()