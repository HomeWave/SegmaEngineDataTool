# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 20:42:26 2019

@author: bob
"""
'''
# demo--产业聚集地分析
'''

import dataInterface, dataCleaning, dataTransfer, dataBasicOpera

# 创建一个Hive连接
hi = dataInterface.HiveInterface()
data = hi.linkHiveTable(databaseName='test', tableName='base_comp_main_orig')
print("原始数据")
data.show()
# 选择指定列
data = dataBasicOpera.selectCol(data, ['industry_type', 'city'])
print("选择的数据")
data.show()
# 检查无效行并删除
invalidnum, invalidtable = dataCleaning.checkInvalidRow(data, threshold=0.1)
data = dataBasicOpera.deleteRow(data, invalidtable.select(["isInvalid"]))
# 检查重复行并删除
tagDF = dataCleaning.checkDuplicateRow(data)
data = dataBasicOpera.deleteRow(data, tagDF)
print("删除无效行重复行后")
data.show()
# 根据'industry_type'和'city'多字段聚合样本，聚合方式为count
data = dataTransfer.multiFieldPartition(data, fieldNameList=['industry_type', 'city'], aggMode='count')
print("聚类后")
data.show()
# 选择每个indus中count最大的记录
maxcounttable = dataTransfer.multiFieldPartition(data.select(['industry_type', 'countN']), fieldNameList=['industry_type'], aggMode='max')
# 将maxcounttable中的max(count)字段名改为count
maxcounttable.show()
print(maxcounttable.columns)
maxcounttable = dataBasicOpera.changeFieldName(maxcounttable, 'max(countN)', 'countN')
# 根据maxcounttable获得最终结果
data = dataBasicOpera.selectRowByPartField(data, maxcounttable)
data.show(1000)
hi.saveAsHiveTable(data,databaseName="test", tableName="industry_cluster_city")


