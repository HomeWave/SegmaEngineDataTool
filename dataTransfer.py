# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 22:39:00 2019

@author: bob
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, floor, row_number
from pyspark.sql.types import StructType, StructField, LongType, FloatType, StringType  # 导入类型
from pyspark.sql import Window
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
    Input
        dataDF:待处理数据表，spark dataframe
        idColName:id列名
    '''
    numParitions = dataDF.rdd.getNumPartitions() # 获取分区数
    data_withindex = dataDF.withColumn("increasing_id_temp", monotonically_increasing_id()) # 添加递增列
    data_withindex = data_withindex.withColumn(idColName, row_number().over(Window.orderBy("increasing_id_temp"))-1) # 生成连续递增id列，此时分区数为1
    data_withindex = data_withindex.repartition(numParitions) # 按原分区数重新分区，所以不用紧张前面分区数变为1时报出的性能降低警告
    data_withindex = data_withindex.sort("increasing_id_temp") # 由于重分区后会打乱顺序，需重新排序
    data_withindex = data_withindex.drop("increasing_id_temp") # 删除临时生成的递增列
    return data_withindex

def zScoreNormal(dataDF, colName, suffix=r"zScoreNormal"):
    '''
    Z-Score 标准化
    对指定的colName，逐一进行标准化
    只能处理数值型字段
    Input
        dataDF:待处理数据表，spark dataframe
        colName:待标准化的字段
        suffix:处理后的字段后缀
    '''
    des = dataDF.describe(colName).toPandas()
    for each in colName:
        func = r"(dataDF."+each+"-"+str(des[each][1])+")"+"/"+str(des[each][2])
        dataDF = dataDF.withColumn(each+'_'+suffix, eval(func))
        dataDF = dataDF.drop(each) # 删除原始列
    return dataDF

def minMaxNormal(dataDF, colName, suffix=r"minMaxNormal"):
    '''
    minMaxNormal 归一化
    对指定的colName，逐一进行归一化
    只能处理数值型字段
    Input
        dataDF:待处理数据表，以后用spark dataframe
        colName:待标准化的字段
        suffix:处理后的字段后缀
    '''
    des = dataDF.describe(colName).toPandas()
    for each in colName:
        func = r"(dataDF."+each+"-"+str(des[each][3])+")"+"/("+str(des[each][4])+'-'+str(des[each][3])+')'
        print(func)
        dataDF = dataDF.withColumn(each+'_'+suffix, eval(func))
        dataDF = dataDF.drop(each) # 删除原始列
    return dataDF

def smooth(dataDF, colName, width=3, stepSize=1, mode=r"mean", suffix=r"smooth"):
    '''
    移动平滑
    只能处理数值型字段
    Input
        dataDF:暂时用[]，以后用spark dataframe
        colName:待标准化的字段
        width:平滑窗口宽度
        stepSize:步长
        mode:平滑方式，可选mean\std\min\max
        suffix:处理后的字段后缀
    '''
    mode2index = {"mean":1, "max":4, "min":3, "std":2}
    assert width>=stepSize
    dataDF = addIdCol(dataDF, idColName="id_temp_bob")
    res_list = []
    for i in range(0, dataDF.count(), stepSize):
        temp = dataDF.filter(dataDF.id_temp_bob>=i).filter(dataDF.id_temp_bob<i+width)
        if temp.count()==0:
            break
        des = temp.describe(colName).toPandas()
        print(des)
        res_list.append([float(des[colName][mode2index[mode]])])
    resDF = spark.createDataFrame(spark.sparkContext.parallelize(res_list), StructType([StructField(colName+"_"+suffix+'_'+mode, FloatType(), True)])) # 标记列
    return resDF

def sortWith(dataDF, cond):
    '''
    按照表达式升序排序
    Input
        dataDF:暂时用[]，以后用spark dataframe
        cond:排序条件/表达式
    Case
        cond = “dataDF.id_temp_bob_smooth_mean”
    '''
    dataDF = dataDF.sort(eval(cond))
    return dataDF

def granularityPartition(dataDF, N, aggMode='mean'):
    '''
    针对数值型数据表，需要指定划分的记录块大小、字段的聚合方式。
    仅仅支持纯数值型表格
    Input
        dataDF:暂时用[]，以后用spark dataframe
        N:块大小
        aggMode:mean/min/max/sum
    Case
        cond = “dataDF.id_temp_bob_smooth_mean”
    '''
    dataDF = addIdCol(dataDF, idColName="id_temp_bob")
    dataDF = dataDF.withColumn("group_id_temp_bob", floor(dataDF.id_temp_bob/N))
    dataDF = eval("dataDF.groupby('group_id_temp_bob')."+aggMode+"()")
    dataDF = dataDF.drop("id_temp_bob").drop("group_id_temp_bob")
    return dataDF

def multiFieldPartition(dataDF, fieldNameList, aggMode='mean'):
    '''
    针对数值型数据表，按照传入的多个字段，聚合划分数据表
    仅仅支持纯数值型表格
    Input
        dataDF:暂时用[]，以后用spark dataframe
        fieldNameList:用于划分数据块的字段名列表
        aggMode:mean/min/max/sum
    Case

    '''
    dataDF = eval("dataDF.groupby(fieldNameList)."+aggMode+"()")
    return dataDF

def changeFieldName(dataDF, origName, newName):
    '''
    修改字段名
    Input
        dataDF:待处理的数据表
        origName:原字段名
        newName:新字段名
    '''
    return dataDF.withColumnRenamed(origName, newName)

def mapFunTest(x):
    '''
    Input
        x:目标字段的数据元素，为字段的数据类型
    '''
    return [x+1, x*2]

def mapSingleField2Multi(dataDF, origFieldName, mapFun, newFieldNameList=None):
    '''
    根据传入mapFun，处理指定字段。可将一个字段拆分为多个字段。
    Input
        dataDF:待处理的数据表
        origFieldName:待拆分的字段
        mapFun:拆分函数
        newFieldNameList:拆分成的新字段名列表
    '''
    colRdd = dataDF.select(origFieldName).rdd.map(lambda x:mapFun(x[0]))
    colN = len(colRdd.take(1)[0])
    splitedDf = spark.createDataFrame(colRdd)
    if newFieldNameList==None: # 按默认方式命名新字段
        for i in range(colN):
            splitedDf = changeFieldName(splitedDf, "_"+str(i+1), origFieldName+"_splited_"+str(i+1))
    else:
        for i, name in enumerate(newFieldNameList): # 指定新字段名
            splitedDf = changeFieldName(splitedDf, "_"+str(i+1), name)
    splitedDf = addIdCol(splitedDf, idColName="continuousID")
    dataDF = addIdCol(dataDF, idColName="continuousID")
    dataDF = dataDF.join(splitedDf, dataDF.continuousID==splitedDf.continuousID)
    return dataDF.drop("continuousID")

def createField(dataDF, exp, filedName='new_field'):
    '''
    针对数值型数据表，需要指定划分的记录块大小、各个字段的聚合方式。
    仅仅支持纯数值型表格
    Input
        dataDF:待处理的数据表spark dataframe
        exp:新字段表达式
        convergMode:max/mean/min/sum
    Case

    '''
    resDF = dataDF.withColumn(filedName, eval(exp))
    return resDF

def sampling(dataDF, sampleMode='equalInterval', randomFraction=0.5, randomSeed=1, equalIntervalStep=3):
    '''
    对数据表行抽样
    仅仅支持纯数值型表格
    Input
        dataDF:待处理数据表spark dataframe
        sampleMode:抽样方式。等间隔/随机：equalInterval/random
        randomFraction:随机抽样-抽样比率
        randomSeed:随机抽样-随机种子
        equalIntervalStep:等间隔抽样-间隔
    Case

    '''
    if sampleMode=="equalInterval":
        dataDF = addIdCol(dataDF, idColName="id_temp_bob")
        dataDF = dataDF.withColumn("group_id_temp_bob", dataDF.id_temp_bob%equalIntervalStep)
        dataDF = dataDF.filter(dataDF.group_id_temp_bob==0).drop("group_id_temp_bob").drop("id_temp_bob")
    elif sampleMode=="random":
        dataDF = dataDF.sample(False, randomFraction, seed=randomSeed)
    else:
        raise KeyError("sampleMode必须是'equalInterval'/'random'")
    return dataDF

#==============================================================================
#==============================================================================
'''
单元测试
在hive中必须有测试用表格test.base_comp_main_orig
'''
class TestBasicOpera1(unittest.TestCase):
    def setUp(self):
        '''
        初始化测试环境
        '''
        print('创建测试用数据表')
        self.dataDF = spark.createDataFrame([[i,i*2-1,i*3-1] for i in range(100)])

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
        self.assertTrue(data_res.count()==4)
        
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
        
if __name__=="__main__":
    from pyspark.sql import SparkSession
    spark=SparkSession \
    .builder \
    .appName('bob_app') \
    .getOrCreate()
    spark.sparkContext.addPyFile("/usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip") # 导入依赖的模块
    spark.sparkContext.addPyFile("/home/hdfs/bob/packages/dataInterface.py") # 导入依赖的模块
    unittest.main()
    