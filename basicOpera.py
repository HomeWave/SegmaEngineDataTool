# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 16:12:06 2019

@author: bob
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.types import StructType, StructField, LongType, StringType  # 导入类型

from dataInterface import HiveInterface

spark=SparkSession \
.builder \
.appName('bob_app') \
.getOrCreate()



#class CustomError(Exception): 
#    def __init__(self,ErrorInfo): 
#        super().__init__(self) #初始化父类 
#        self.errorinfo=ErrorInfo 
#    def __str__(self): 
#        return self.errorinfo

def addIdCol(dataDF, idColName="continuousID"):
    '''
    添加连续递增id列
    '''
    numParitions = dataDF.rdd.getNumPartitions() # 获取分区数
    data_withindex = dataDF.withColumn("increasing_id_temp", monotonically_increasing_id()) # 添加递增列
    data_withindex = data_withindex.withColumn(idColName, row_number().over(Window.orderBy("increasing_id_temp"))-1) # 生成连续递增id列，此时分区数为1
    data_withindex = data_withindex.repartition(numParitions) # 按原分区数重新分区，所以不用紧张前面分区数变为1时报出的性能降低警告
    data_withindex = data_withindex.sort("increasing_id_temp") # 由于重分区后会打乱顺序，需重新排序
    data_withindex = data_withindex.drop("increasing_id_temp") # 删除临时生成的递增列
    return data_withindex

def selectRow(dataDF, rowIndex=[1,2,3,4]):
    '''
    选择数据的指定行
    Input
        rowIndex:暂时用[]，以后用spark dataframe
    '''
    # 异常输入检测
    if rowIndex==[]:
        return dataDF
    rowN = dataDF.count()
    # 边界检测，行标是否符合规定,raise IndexError("行标越界")
    if len(rowIndex)>rowN:
        raise IndexError("指定筛选的行标数超过数据表总行数！")
    if min(rowIndex)<0 or max(rowIndex)>rowN-1:
        raise IndexError("行标越界！")
    # 对dataDf添加索引列
    data_withindex = addIdCol(dataDF,idColName="id_left_temp_bob")

    # 生成行选择标签表
    select_flag = [[0]]*rowN
    for each in rowIndex:
        select_flag[each] = [1]
    selectRow_flag_bob = spark.createDataFrame(spark.sparkContext.parallelize(select_flag), 
                                        StructType([StructField("selectRow_flag_bob", LongType(), True)])) # 标记列
    # selectRow_flag_bob.show()
    select_flag_withindex = addIdCol(selectRow_flag_bob, idColName="id_right_temp_bob")
    # 以索引为主键拼接dataDF和标记表
    # print(select_flag_withindex.count())
    data_withindex2 = data_withindex.join(select_flag_withindex, data_withindex.id_left_temp_bob==select_flag_withindex.id_right_temp_bob)
    # 按照选择标记筛选行
    # print(data_withindex2.show())
    data_filtered = data_withindex2.filter(data_withindex2.selectRow_flag_bob>0)
    # print(data_filtered.count())
    # 删除id_temp和标识行
    data_res = data_filtered.drop("id_left_temp_bob").drop("id_right_temp_bob")
    # data_res.show()
    # print(data_res.show(1))
    return data_res


def filterRow(dataDF, cond):
    '''
    自定义条件筛选行
    条件表达式：一个Bool类型的Column，或者SQL字符串表达式
    '''
    return dataDF.filter(cond)
    

'''
单元测试
在hive中必须有测试用表格test.base_comp_main_orig
'''
class TestBasicOpera1(unittest.TestCase):
    def setUp(self):
        '''
        初始化测试环境
        '''
        print('连接测试用数据')
        hif = HiveInterface()
        self.dataDF = hif.linkHiveTable(limitN=30)
        
    def tearDown(self):
        '''
        退出函数
        '''
#        print('测试结束')
        pass
        
    def test_selectRow_correctness(self):
        '''
        测试行选择，正确性测试
        '''
        data_res = selectRow(self.dataDF)
        data_res = data_res.select(data_res.company_id)

        print(data_res.show())
        print("======================")
        print(data_res.count())
        print("======================")
        self.assertTrue(True)
        
    def test_selectRow_boundary(self):
        '''
        测试行选择，行标边界测试，异常测试
        '''
        # 行标数超过行数
        with self.assertRaises(IndexError):
            selectRow(self.dataDF, rowIndex=list(range(100000)))
        # 行标为负
        with self.assertRaises(IndexError):
            selectRow(self.dataDF, rowIndex=[0,1,-2,3])
        # 行标过大
        with self.assertRaises(IndexError):
            selectRow(self.dataDF, rowIndex=[0,1,2,3, 1000000000])
            
    def test_filterRow(self):
        '''
        测试条件筛选行
        '''
        # 无需测试
        pass
            
if __name__=="__main__":
    from pyspark.sql import SparkSession
    spark=SparkSession \
    .builder \
    .appName('bob_app') \
    .getOrCreate()
    spark.sparkContext.addPyFile("/usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip") # 导入依赖的模块
    spark.sparkContext.addPyFile("/home/hdfs/bob/packages/dataInterface.py") # 导入依赖的模块
    unittest.main()
     
    
    
    

    
