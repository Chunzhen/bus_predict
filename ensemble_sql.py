#! /usr/bin/env python
# -*- coding:utf-8 -*-
#  根ensemble sql

import sys
import os
from datetime import datetime
import time


def ensemble_sql(input_tables,output_table_name,file_name):
	f=file(u'ensemble/'+file_name+'.txt','wb')
	sql=""
	sql+="drop table if exists "+output_table_name+"; create table "+output_table_name+" as select "
	inner_select=""
	inner_select=""
	inner_sql=""
	i=0
	for table in input_tables:
		if i==0:
			inner_select+=table+".* "
			inner_sql+=" "+table
			i+=1
			continue
		inner_select+=", "+table+".prediction_score as score"+str(i)+","+table+".prediction_result as result"+str(i)+" \n"
		inner_sql+=" left outer join "+table+" on "+input_tables[0]+".card_id="+table+".card_id \n"
		i+=1
	sql+=inner_select+" from "+inner_sql+";\n"
	sql+="alter table "+output_table_name+" change column prediction_score rename to score0;\n"
	sql+="alter table "+output_table_name+" change column prediction_detail rename to detail0;\n"
	sql+="alter table "+output_table_name+" change column prediction_result rename to result0;\n"

	f.write(sql)

def vote_sql():
	f=file(u'ensemble/voting.txt','wb')
	sql="select *,"
	inner_select="cast(("
	for i in range(16):
		if i==0:
			inner_select+="result"+str(i)
		else:
			inner_select+="+result"+str(i)

	inner_select+=")/8 as int) as prediction_result"

	sql+=inner_select+" from ensemble_output_21_16"
	f.write(sql)
	pass


def main():
	#train
	# input_tables=["lr_output_21","svm_output_21","rf_output_21","gbdt_output_21"]
	# output_table_name="ensemble_output_21"
	# file_name="ensemble_21_2month"
	#test
	# input_tables=["lr_predict_21","svm_predict_21","rf_predict_21","gbdt_predict_21"]
	# output_table_name="ensemble_predict_21"
	# file_name="ensemble_21_2month_predict"

	#train
	input_tables=[]
	for i in range(8):
		j=i+1
		input_tables.append("rf_output_21_"+str(j))
		input_tables.append("gbdt_output_21_"+str(j))


	output_table_name="ensemble_output_21_16"
	file_name="ensemble_output_21_16"
	ensemble_sql(input_tables, output_table_name, file_name)

	#test
	input_tables=[]
	for i in range(8):
		j=i+1
		input_tables.append("rf_predict_21_"+str(j))
		input_tables.append("gbdt_predict_21_"+str(j))


	output_table_name="ensemble_predict_21_16"
	file_name="ensemble_predict_21_16"
	ensemble_sql(input_tables, output_table_name, file_name)

	pass

if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()
	vote_sql()