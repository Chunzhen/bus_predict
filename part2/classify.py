#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 线下简单测试

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

def load_data():
	data=np.loadtxt('tempFile/train_data_road_10.txt',delimiter=',',dtype='int')
	m,n=data.shape
	X=data[:,0:n-1]
	label=data[:,n-1]
	return X,label

from sklearn.cross_validation import ShuffleSplit
from sklearn.metrics import classification_report
def handle_classify(clf):
	X,y=load_data()
	cv=ShuffleSplit(n=len(X),n_iter=10,test_size=0.25,indices=True,random_state=1)

	for train_index,test_index in cv:
		X_train,y_train=X[train_index],y[train_index]
		X_test,y_test=X[test_index],y[test_index]

		clf.fit(X_train,y_train)

		predict=clf.predict(X_test)
		m=len(y_test)
		correctNum=0
		for i in range(m):
			if predict[i]==y_test[i]:
				correctNum+=1

		print "accuracy:"+str(correctNum/float(m))
		print classification_report(y_test,predict)

from sklearn.neighbors import KNeighborsClassifier
def knn_classify():
	return KNeighborsClassifier(n_neighbors=5)

def main():
	handle_classify(knn_classify())
	pass

if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()