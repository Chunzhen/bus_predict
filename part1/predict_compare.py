#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 将预测的数据转为json格式以便d3分析
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time
import matplotlib.pyplot as plt
def compare_predict_data(paths,index=0):
	data=[]
	i=0
	for x in paths:
		temp=np.loadtxt('output/'+x,delimiter=',',converters={0:checkRoad,1:checkDate,2:checkHour},dtype='int')
		data.append(temp)
		data[i]=data[i][index:]
		i+=1

	m,n=data[0].shape



	for i in range(m):
		road=str(data[0][i,0])
		d=datetime.fromtimestamp(float(data[0][i,1]))
		hour=str(data[0][i,2])
		day=str(d.year)+'-'+add_zero(d.month)+'-'+add_zero(d.day)
		#print u''+road+'路：'+day+' '+add_zero(hour)+'时 path1:'+str(data[0][i,3])+' path2:'+str(data[1][i,3])+' ='+str(data[0][i,3]-data[1][i,3])
	
	#fig=plt.figure()
	#ax=fig.add_subplot(111)
	colors=['red','blue','green','purple','black']
	i=0
	for x in paths:
		plt.plot(range(m)[:48],data[i][:48,3],label=x,color=colors[i])	
		i+=1

	plt.legend()
	plt.grid()
	plt.show()


def main():
	paths=['output_2015-11-27.txt','output_2015-11-29_atree.txt','output_2015-11-28_atree2.txt','output_2015-11-29_gb.txt','output_2015-11-29_final.txt']
	compare_predict_data(paths,112)
	pass

def checkRoad(s):
	s=s.strip()
	s=s.replace(u'线路','')
	return s

def checkDate(s):
	date=datetime.strptime(str(s),'%Y%m%d')
	timestamp=time.mktime(date.timetuple())
	return int(timestamp)

def checkHour(s):
	return int(s)

def add_zero(s):
	t=str(s)
	if s<10:
		t='0'+t
	return t


if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()