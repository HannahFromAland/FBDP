# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 11:03:29 2019
@author: lenovo
"""
import re
import matplotlib.pyplot as plt

fname = input("输入文件路径：")

file=open(fname, 'r')
lines=list(file.readlines())
file.close()
all_points = {}
for line in lines:
    values = re.split('[\t, \n]',line)
    if values[0] not in all_points.keys():
        all_points[values[0]] = {'x':[], 'y': []}
    all_points[values[0]]['x'].append(float(values[1]))
    all_points[values[0]]['y'].append(float(values[2]))


colors = ['lightblue','mediumseagreen','lightcoral',
'lightgrey','lightpink','khaki','thistle','cornflowerblue','mediumaquamarine','lightsalmon']
for id in all_points:
    plt.scatter(all_points[id]['x'],all_points[id]['y'],color=colors[int(id)],marker='o',s=50)
plt.title("KMeans:k=" + str(len(all_points)))
plt.savefig(fname)
