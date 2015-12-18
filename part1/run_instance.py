#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 生成测试数据

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import time

from weather import Weather
from clean_data import Clean_data
from split_road_data import Split_road_data
from count_road_data import Count_road_data
from time_select import Time_select
from statistic_road_data import Statistic_road_data
from to_regression import To_regression
from create_test_data import Create_test_data
from regression import Regression
def main():
	#weather_instance=Weather()
	#weather_instance.handle_data()

	#clean_data_instance=Clean_data()
	#clean_data_instance.handle_data()

	#split_road_data_instance=Split_road_data()
	#split_road_data_instance.handle_data()

	#count_road_data_instance=Count_road_data()
	#count_road_data_instance.handle_data()

	#time_select_instance=Time_select()
	#time_select_instance.handle_data()

	#statistic_road_data_instance=Statistic_road_data()
	#statistic_road_data_instance.handle_data()

	to_regression_instance=To_regression()
	to_regression_instance.handle_data()

	create_test_data_instance=Create_test_data()
	create_test_data_instance.handle_data()

	#regression_instance=Regression()
	#regression_instance.handle_output()

if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf8')
	main()