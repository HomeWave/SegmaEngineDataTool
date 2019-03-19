# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 22:39:00 2019

@author: bob
"""
import warnings
warnings.filterwarnings('ignore')
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, floor, row_number
from pyspark.sql.types import StructType, StructField, LongType, FloatType, StringType  # 导入类型
from pyspark.sql import Window
import numpy as np
from dataBasicOpera import changeFieldName, addIdCol

spark=SparkSession \
.builder \
.appName('dataTranster') \
.getOrCreate()


def zScoreNormal(dataDF, fieldNameList, suffix=r"zScoreNormal"):
    '''
    Z-Score 标准化
    对指定的fieldNameList，逐一进行标准化
    只能处理数值型字段
    :param dataDF:处理数据表，spark dataframe
    :param fieldNameList:待标准化的字段
    :param suffix:处理后的字段后缀
    :return:
    '''
    for each in fieldNameList:
        columns = dataDF.columns
        if each not in columns:
            raise ValueError(each+"字段不存在！")
    des = dataDF.describe(fieldNameList).toPandas()
    num = dataDF.count()
    for each in fieldNameList:
        func = r"(dataDF."+each+"-"+str(des[each][1])+")"+"/"+str(float(des[each][2])*pow((num-1)/num, 0.5))
        dataDF = dataDF.withColumn(each+'_'+suffix, eval(func))
        dataDF = dataDF.drop(each) # 删除原始列
    return dataDF

def minMaxNormal(dataDF, fieldNameList, suffix=r"minMaxNormal"):
    '''
    minMaxNormal 归一化
    对指定的fieldNameList，逐一进行归一化
    只能处理数值型字段
    :param dataDF:待处理数据表，以后用spark dataframe
    :param fieldNameList:待归一化的字段
    :param suffix:处理后的字段后缀
    :return:
    '''
    for each in fieldNameList:
        columns = dataDF.columns
        if each not in columns:
            raise ValueError(each+"字段不存在！")
    des = dataDF.describe(fieldNameList).toPandas()
    for each in fieldNameList:
        func = r"(dataDF."+each+"-"+str(des[each][3])+")"+"/("+str(des[each][4])+'-'+str(des[each][3])+')'
        dataDF = dataDF.withColumn(each+'_'+suffix, eval(func))
        dataDF = dataDF.drop(each) # 删除原始列
    return dataDF

def smooth(dataDF, fieldName, width=3, stepSize=1, mode=r"mean", suffix=r"smooth"):
    '''
    移动平滑
    只能处理数值型字段
    :param dataDF: 待处理数据表
    :param fieldName:待标准化的字段
    :param width:平滑窗口宽度
    :param stepSize:步长
    :param mode:平滑方式，可选mean\std\min\max
    :param suffix:
    :return:
    '''
    mode2index = {"mean":1, "max":4, "min":3, "std":2}
    if mode not in mode2index:
        raise ValueError("mode必须为mean\std\min\max之一")
    if width<stepSize: # 窗口宽度需要大于等于步长
        raise ValueError("窗口宽度width不能小于步长stepSize！")
    if fieldName not in dataDF.columns:
        raise ValueError("待处理字段不存在！")
    dataDF = addIdCol(dataDF, idFieldName="id_temp_bob")
    res_list = []
    for i in range(0, dataDF.count(), stepSize):
        temp = dataDF.filter(dataDF.id_temp_bob>=i).filter(dataDF.id_temp_bob<i+width)
        if temp.count()==0:
            break
        des = temp.describe(fieldName).toPandas()
        res_list.append([float(des[fieldName][mode2index[mode]])])
    resDF = spark.createDataFrame(spark.sparkContext.parallelize(res_list), StructType([StructField(fieldName+"_"+suffix+'_'+mode, FloatType(), True)])) # 标记列
    return resDF

def sortWith(dataDF, cond):
    '''
    按照表达式升序排序
    :param dataDF: 待处理数据表
    :param cond: 排序条件/表达式
    :return:
    '''

    dataDF = dataDF.sort(eval(cond))
    return dataDF

def granularityPartition(dataDF, N, aggMode='mean'):
    '''
    针对数值型数据表，需要指定划分的记录块大小、字段的聚合方式。
    仅仅支持纯数值型表格
    :param dataDF:待处理数据表
    :param N:块大小
    :param aggMode:
    :return:聚合方式，可选mean/std/min/max/count
    '''
    mode2index = {"mean": 1, "max": 4, "min": 3, "std": 2, "count":0}
    if aggMode not in mode2index:
        raise ValueError("aggMode必须为mean\std\min\max\count之一")
    dataDF = addIdCol(dataDF, idFieldName="id_temp_bob")
    dataDF = dataDF.withColumn("group_id_temp_bob", floor(dataDF.id_temp_bob/N))
    dataDF = eval("dataDF.groupby('group_id_temp_bob')."+aggMode+"()")
    dataDF = dataDF.drop("id_temp_bob").drop("group_id_temp_bob")
    dataDF = changeFieldName(dataDF, "count", "countN")
    for each in dataDF.columns:
        dataDF = changeFieldName(dataDF, each, each.replace('(','_').replace(')','_'))
    return dataDF.drop(aggMode+"_id_temp_bob_").drop(aggMode+"_group_id_temp_bob_")

def multiFieldPartition(dataDF, fieldNameList, aggMode='mean'):
    '''
    针对数值型数据表，按照传入的多个字段，聚合划分数据表
    mean/min/max/sum aggMode仅仅支持纯数值型表格
    :param dataDF:待处理数据表
    :param fieldNameList:用于划分数据块的字段名列表
    :param aggMode:mean/min/max/sum/count
    :return:
    '''
    mode2index = {"mean": 1, "max": 4, "min": 3, "std": 2, "count": 0}
    if aggMode not in mode2index:
        raise ValueError("aggMode必须为mean\std\min\max\count之一")
    columns = dataDF.columns
    for each in fieldNameList:
        if each not in columns:
            raise ValueError(each+"字段不存在！")
    exp = "dataDF.groupby(fieldNameList)."+aggMode+"()"
    if aggMode=='count':
        dataDF = dataDF.groupby(fieldNameList).count()
    else:
        dataDF = eval(exp)
    dataDF = changeFieldName(dataDF, "count", "countN") # 将count改为countN，避免和count()混淆
    return dataDF

def mapSingleField2Multi(dataDF, origFieldName, mapFun, newFieldNameList=None):
    '''
    根据传入mapFun，处理指定字段。可将一个字段拆分为多个字段。
    :param dataDF:待处理的数据表
    :param origFieldName:待拆分的字段
    :param mapFun:拆分函数。注意：该函数必须保证输出为List且维度固定
    :param newFieldNameList:拆分成的新字段名列表
    :return:
    '''

    if origFieldName not in dataDF.columns:
        raise ValueError("待处理字段不存在！")

    colRdd = dataDF.select(origFieldName).rdd.map(lambda x:mapFun(x[0]))
    colN = len(colRdd.take(1)[0])
    splitedDf = spark.createDataFrame(colRdd)
    if newFieldNameList==None: # 按默认方式命名新字段
        for i in range(colN):
            splitedDf = changeFieldName(splitedDf, "_"+str(i+1), origFieldName+"_splited_"+str(i+1))
    else:
        for i, name in enumerate(newFieldNameList): # 指定新字段名
            splitedDf = changeFieldName(splitedDf, "_"+str(i+1), name)
    splitedDf = addIdCol(splitedDf, idFieldName="continuousID")
    dataDF = addIdCol(dataDF, idFieldName="continuousID")
    dataDF = dataDF.join(splitedDf, dataDF.continuousID==splitedDf.continuousID)
    return dataDF.drop("continuousID")

def createField(dataDF=None, exp=None, filedName='new_field'):
    '''
    利用已有字段构造新字段
    :param dataDF: 待处理的数据表
    :param exp: 新字段表达式
    :param filedName: 新字段名称
    :return:
    '''
    data = dataDF
    resDF = data.withColumn(filedName, eval(exp))
    return resDF

def sampling(dataDF, sampleMode='equalInterval', equalIntervalStep=3, randomFraction=0.5, randomSeed=1):
    '''
    对数据表行抽样
    仅仅支持纯数值型表格
    :param dataDF:
    :param sampleMode: 抽样策略。等间隔/随机：equalInterval/random
    :param randomFraction:随机抽样-抽样比率
    :param randomSeed:随机抽样-随机种子
    :param equalIntervalStep:等间隔抽样-间隔
    :return:
    '''

    if sampleMode=="equalInterval":
        dataDF = addIdCol(dataDF, idFieldName="id_temp_bob")
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
'''
class TestDataTranster(unittest.TestCase):
    def setUp(self):
        '''
        初始化测试环境
        '''
        print('生成测试用数据')
        import numpy as np
        # 构造测试数据
        data = [[1,2,3],[4,5,6],[7,8,9],[10,11,12],[13,14,15]]
        self.dataDF = spark.createDataFrame(data)
        data2 = [['zhai bo',2,3],['tang bin',5,6],['zhang jing han',8,9],['xie xiao dong',11,12],['zu jie',14,15]]
        self.dataDF2 = spark.createDataFrame(data2)

        from sklearn import preprocessing
        scaler = preprocessing.StandardScaler().fit(data)
        minmax_scale = preprocessing.MinMaxScaler().fit(data)
        self.data_processed = scaler.transform(data)
        self.data_minmax = minmax_scale.transform(data)
        self.granuParTest = np.array([[4, 5, 6], [11.5, 12.5, 13.5]])

    def tearDown(self):
        '''
        退出函数
        '''
#        print('测试结束')
        pass
        
    def test_addIdCol(self):
        '''
        测试添加列
        '''
        pass # 不需要测试，经常使用
        
    def test_zScoreNormal(self):
        '''
        测试数据标准化
        '''
        # 接口测试

        # 路径测试

        # 正确性
        fieldNameList = self.dataDF.columns
        resDF = zScoreNormal(self.dataDF, fieldNameList)
        self.assertTrue(sum(sum((np.array(resDF.toPandas())-self.data_processed)**2))<0.1)

    def test_minMaxNormal(self):
        '''
        测试数据归一化
        '''
        # 接口测试

        # 路径测试

        # 正确性
        fieldNameList = self.dataDF.columns
        resDF = minMaxNormal(self.dataDF, fieldNameList)
        self.assertTrue(sum(sum((np.array(resDF.toPandas())-self.data_minmax)**2))<0.1)

    def test_smooth(self):
        '''
        测试数据平滑
        '''
        # 接口测试
        # ----字段不存在
        with self.assertRaises(ValueError):
            resDF = smooth(self.dataDF, fieldName='abc')
        # ----width<stepSize
        with self.assertRaises(ValueError):
            resDF = smooth(self.dataDF, fieldName='_1', width=3, stepSize=4)
        # ----mode不存在
        with self.assertRaises(ValueError):
            resDF = smooth(self.dataDF, fieldName='_1', width=3, stepSize=2, mode='count')
        # 路径测试

        # 正确性
        fieldNameList = self.dataDF.columns
        resDF = smooth(self.dataDF, fieldName='_1', mode='mean')
        resDF = smooth(self.dataDF, fieldName='_1', mode='std')
        resDF = smooth(self.dataDF, fieldName='_1', mode='min')
        resDF = smooth(self.dataDF, fieldName='_1', mode='max')

    def test_granularityPartition(self):
        '''
        测试数据颗粒度划分
        '''
        # 接口测试
        # ----mode不存在
        with self.assertRaises(ValueError):
            resDF = granularityPartition(self.dataDF, N=3, aggMode='mode')
        # 路径测试

        # 正确性
        fieldNameList = self.dataDF.columns
        # resDF = granularityPartition(self.dataDF, N=3, aggMode='mean')
        # self.assertTrue(sum(sum((np.array(resDF.toPandas())-self.granuParTest)**2))<0.1)

    def test_multiFieldPartition(self):
        '''
        测试数据颗粒度划分
        '''
        # 接口测试
        # ----mode不存在
        with self.assertRaises(ValueError):
            resDF = multiFieldPartition(self.dataDF, fieldNameList=['_1', '_2'], aggMode='mode')
        # 路径测试

        # 正确性
        fieldNameList = self.dataDF.columns
        resDF = multiFieldPartition(self.dataDF, fieldNameList=['_1', '_2'], aggMode='mean')
        print("test_multiFieldPartition processed", resDF.show())


    def test_mapSingleField2Multi(self):
        '''
        测试字段拆分
        '''
        # 接口测试
        # ----mode不存在
        with self.assertRaises(ValueError):
            resDF = mapSingleField2Multi(self.dataDF2, origFieldName='abc', mapFun=lambda x:x)
        # 路径测试

        # 正确性
        resDF = mapSingleField2Multi(self.dataDF2, origFieldName="_1", mapFun=lambda x:x.split()[:2])
        resDF = mapSingleField2Multi(self.dataDF2, origFieldName="_1", mapFun=lambda x: x.split()[:2], newFieldNameList=["first_name", "sec_name"])
        print("test_mapSingleField2Multi processed data",resDF.show())
        resDF = mapSingleField2Multi(self.dataDF2, origFieldName="_2", mapFun=lambda x: [x*3, x**2], newFieldNameList=["first_name", "sec_name"])

    def test_sampling(self):
        '''
        测试抽样
        '''
        # 接口测试
        # ----mode不存在
        with self.assertRaises(KeyError):
            resDF = sampling(self.dataDF, sampleMode='abc')
        # 路径测试

        # 正确性
        resDF = sampling(self.dataDF)
        resDF = sampling(self.dataDF, sampleMode='random')

if __name__=="__main__":
    # from pyspark.sql import SparkSession
    # spark=SparkSession \
    # .builder \
    # .appName('bob_app') \
    # .getOrCreate()
    # spark.sparkContext.addPyFile("/usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip") # 导入依赖的模块
    # spark.sparkContext.addPyFile("/home/hdfs/bob/packages/dataInterface.py") # 导入依赖的模块
    unittest.main()
    