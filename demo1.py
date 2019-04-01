# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 20:42:26 2019

@author: bob
"""
'''
# demo--生产工艺数据处理
'''

import dataInterface, dataCleaning, dataTransfer, dataBasicOpera # 导入组件

# 创建一个Hive连接
hi = dataInterface.HiveInterface()
data = hi.linkHiveTable(databaseName='test', tableName='vcalsegment1712_txt')
print("原始数据")
data.show(10)
# 检查无效列并删除
invalidnum, invalidname = dataCleaning.checkInvalidCol(data, threshold=0.1)
data = dataBasicOpera.deleteCol(data, fieldNameList=invalidname)
# 检查离群值并删除
# tagDF, nums = dataCleaning.checkOutlier(data, ColName=data.columns[10])
# data = dataBasicOpera.deleteRow(data, tagDF)
# print("离群值数量%s"%nums)
# print("删除离群值后")
# data.show(10)
# 标准化
data = dataTransfer.zScoreNormal(data, fieldNameList=data.columns[:10])
# 粒度划分
data = dataTransfer.granularityPartition(data, N=3, aggMode='mean')
print("粒度划分后")
data.show(2)
# 新建字段
data = dataTransfer.createField(dataDF=data, exp="(data.avg_firstctemp_upper_+data.avg_firstctemp_lower_*2)*0.5-data.avg_oatemp_upper_", filedName="new_field1")
# 字段拆分
data = dataTransfer.mapSingleField2Multi(data, origFieldName="avg_firstctemp_upper_", mapFun=lambda x:[x-3, x+3])
print("处理结果")
data.show(10)
# 保存到Hive
hi.saveAsHiveTable(data,databaseName="test", tableName="vcalsegment1712_txt_processed")
print("成功！")

