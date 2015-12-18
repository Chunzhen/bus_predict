#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 首次尝试预测，线性回归模型

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

from sklearn.ensemble.forest import RandomForestRegressor
from sklearn.ensemble import BaggingRegressor
from sklearn import ensemble

from sklearn.cross_validation import ShuffleSplit
from sklearn.cross_validation import train_test_split
from sklearn import metrics
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn import svm
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import ExtraTreesRegressor

class Regression(object):
	def __init__(self):
		pass

	def _get_road_list(self):
		return (np.loadtxt('files/road_list.txt',delimiter=',',usecols=(0,1),dtype='int'))[:,1].tolist()

	def load_data(self,road):
		week_data={}
		week_arr=set()
		data=np.loadtxt('files/regression_'+road+'.txt',delimiter=',',dtype='float')
		m,n=data.shape
		X=data[:,0:n-1]
		y=data[:,n-1]
		return X,y

	def handle_regression(self,regression,road):
		X,y=self.load_data(road)
		cv=ShuffleSplit(n=len(X),n_iter=10,test_size=0.1,indices=True,random_state=5)

		AllMAE=[]
		AllMSE=[]
		AllRMSE=[]

		naiveMAE=[]
		naiveMSE=[]
		naiveRMSE=[]

		m,n=X.shape

		for train_index,test_index in cv:
			X_train,y_train=X[train_index],y[train_index]
			X_test,y_test=X[test_index],y[test_index]

			regression.fit(X_train,y_train)

			y_pred = regression.predict(X_test)
			m=len(y_test)

			hour_error={}
			for i in range(m):
				hour=str(X_test[i,1])
				week=str(X_test[i,2])
				#print u"星期："+str(X_test[i,2])+' '+str(X_test[i,1])+' 点 '+str(y_test[i])+' '+str(y_pred[i])+' '+str(y_pred[i]-y_test[i])
				temp_error=hour_error.get(hour,[])
				temp_error.append(int(abs(y_pred[i]-y_test[i])))
				hour_error[hour]=temp_error

			print "*************Predict model*************"	
			mae=metrics.mean_absolute_error(y_test, y_pred)
			mse=metrics.mean_squared_error(y_test, y_pred)
			rmse=np.sqrt(metrics.mean_squared_error(y_test, y_pred))
			print "MAE:",mae
			#print "MSE:",mse
			#print "RMSE:",rmse
			AllMAE.append(mae)
			AllMSE.append(mse)
			AllRMSE.append(rmse)
				
			print '\n'

		print "*************Predict model*************"	
		AverageMAE=np.median(AllMAE)
		AverageMSE=np.median(AllMSE)
		AverageRMSE=np.median(AllRMSE)
		print "Average MAE:",AverageMAE

	def linear_regression(self):
		regression=LinearRegression()
		return regression
	
	def svr_regression(self):
		regression=svm.SVR()
		return regression

	def tree_regression(self):
		return DecisionTreeRegressor(max_depth=10)
	
	def extra_tree(self):
		return ExtraTreesRegressor(n_estimators=100, max_features=10,
                                       random_state=0)

	def adaboost_tree_regression(self):
		rng = np.random.RandomState(7)
		return AdaBoostRegressor(DecisionTreeRegressor(max_depth=20), #
	                          n_estimators=100, random_state=rng)

	def adaboost_gradient_regression(self):
		params = {'n_estimators': 500, 'max_depth': 7, 'min_samples_split': 10,
	          'learning_rate': 0.01, 'loss': 'ls'}
		return ensemble.GradientBoostingRegressor(**params)

	
	def random_forest_regression(self):
		return RandomForestRegressor(n_estimators=500,min_samples_split=5)

	
	def bagging_regression(self):
		rng = np.random.RandomState(7)
		return BaggingRegressor(self.adaboost_tree_regression(),     
	                          n_estimators=200, random_state=rng)

	def handle_train(self):
		self.handle_regression(self.random_forest_regression(),'6')
		pass

	def handle_output(self):
		d='2015-11-29_atree'
		try:
			os.remove('output/output_'+d+'.txt')
		except WindowsError:
			pass

		roadList=self._get_road_list()
		for x in roadList:
			self.output_test(self.bagging_regression(),str(x),d)

	def add_zero(self,s):
		t=str(s)
		if s<10:
			t='0'+t
		return t

	def output_test(self,regression,road,d):
		X,y=self.load_data(road)
		test_data=np.loadtxt('files/test_data.txt',delimiter=',',dtype='int')

		regression.fit(X,y)
		y_pred = regression.predict(test_data[:,1:])

		m=len(y_pred)
		f=file('output/output_'+d+'.txt','a+')
		
		for i in range(m):
			d=datetime.fromtimestamp(float(test_data[i,0]))
			dStr=str(d.year)+''+self.add_zero(d.month)+''+self.add_zero(d.day)
			hour=self.add_zero(test_data[i,2])
			row='线路'+road+','+dStr+','+hour+','+str(int(y_pred[i]))
			f.write(row+'\n')
		f.close()


def main():
	regression_instance=Regression()
	regression_instance.handle_output()

if __name__ == '__main__':
	main()