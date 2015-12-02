#! /usr/bin/env python
# -*- coding:utf-8 -*-
# 统计属性值

import sys
import os
import numpy as np
from datetime import datetime,date
import time
import math
import matplotlib.pyplot as plt

def main():
	x=range(10)
	y=[]
	for i in x:
		y.append(1/(1+math.exp(i))*10)

	print y
	plt.plot(x,y)
	plt.show()

if __name__ == '__main__':
	main()