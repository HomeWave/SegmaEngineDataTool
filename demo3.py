# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 16:42:26 2019

@author: jerry
"""
'''
# demo--工业数据处理
'''

import dataInterface, dataCleaning, dataTransfer, dataBasicOpera # 导入组件
import time
time_start = time.time()


# 创建一个Hive连接
hi = dataInterface.HiveInterface()
data = hi.linkHiveTable(databaseName='test', tableName='14a11034616000________00001_data')
print("原始数据")
data.show()
#选取五列分析：入口设定速度，入口实际速度，开卷机实际张力，清洗段入口实际张力，清洗段出口实际张力
data = dataBasicOpera.selectCol(data,data.columns[1:6])
#更改列名\
newname = ['入口设定速度','入口实际速度','开卷机实际张力','清洗段入口实际张力','清洗段出口实际张力']
for i,name in enumerate(data.columns):
    data = dataBasicOpera.changeFieldName(data,name,newname[i])
#检查无效行并删除
invalidnum, invalidtable = dataCleaning.checkInvalidRow(data, threshold=0.1)
print("无效行数量%s"%invalidnum)
data = dataBasicOpera.deleteRow(data, invalidtable.select(["isInvalid"]))
print('除去无效行后')
data.show()
# 新建字段
data = dataTransfer.createField(dataDF=data, exp="(data.入口设定速度-data.入口实际速度)", filedName="入口速度差值")
data = dataBasicOpera.deleteCol(data, fieldNameList=['入口设定速度','入口实际速度'])
#检查连续重复行并删除除第一次出现的其他行
DuplicateRow= dataCleaning.checkDuplicateRow(data)
data = dataBasicOpera.deleteRow(data, DuplicateRow)
#检查开卷机实际张力的离群值并删去
tagDF, nums = dataCleaning.checkOutlier(data, ColName=data.columns[0])
data = dataBasicOpera.deleteRow(data, tagDF)
print("离群值数量%s"%nums)
print("删除离群值后")
data.show()
#归一化
data = dataTransfer.minMaxNormal(data,data.columns)
print('归一化后')
data.show()
# 粒度划分
data = dataTransfer.granularityPartition(data, N=3, aggMode='mean')
print("粒度划分后")
data.show()
#数据采样
demo = dataTransfer.sampling(data)
print('数据采样后')
data.show()
#添加ID列，保存到Hive
hi.saveAsHiveTable(demo,databaseName="test", tableName="14a11034616000________00001_data_sample")
print("成功！")
time_end = time.time()
t1 = time_end-time_start
print('运行时间为%fs'%t1)


