# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 14:37:26 2019

@author: jerry
"""

# 初始化sparkSession和HiveSession
import pyspark.sql.functions as fn
from  pyspark.sql import Row
from pyspark.sql.functions import isnan, isnull
import unittest
import pyspark.sql.types as typ
from pyspark.sql import Window
from pyspark.sql import SparkSession

# 初始化SparkSession
spark = SparkSession.builder.appName("RDD_and_DataFrame").config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext

def addIdCol(dataDF, idColName = "continuousID"):
    '''
    :param dataDF: 数据表; DataFrame
    :param idColName: 生成的ID列的名称：String
    :return: 末列添加了从0开始的连续递增ID的列表；DataFrame
    '''
    numParitions = dataDF.rdd.getNumPartitions()
    data_withindex = dataDF.withColumn("increasing_id_temp", fn.monotonically_increasing_id())
    data_withindex = data_withindex.withColumn(idColName, fn.row_number().over(Window.orderBy("increasing_id_temp"))-1)
    data_withindex = data_withindex.repartition(numParitions)
    data_withindex = data_withindex.sort("increasing_id_temp")
    data_withindex = data_withindex.drop("increasing_id_temp")
    return data_withindex

def addIdCol1(dataDF, idColName = "continuousID"):
    '''
        :param dataDF: 数据表; DataFrame
        :param idColName: 生成的ID列的名称：String
        :return: 末列添加了从1开始的连续递增ID的列表；DataFrame
    '''
    numParitions = dataDF.rdd.getNumPartitions()
    data_withindex = dataDF.withColumn("increasing_id_temp", fn.monotonically_increasing_id())
    data_withindex = data_withindex.withColumn(idColName, fn.row_number().over(Window.orderBy("increasing_id_temp")))
    data_withindex = data_withindex.repartition(numParitions)
    data_withindex = data_withindex.sort("increasing_id_temp")
    data_withindex = data_withindex.drop("increasing_id_temp")
    return data_withindex

def geneExp(tableName, field):
    '''
    :param tableName: 表名，String
    :param field: 字段名，String
    :return: 计数表达式;String
    '''
    temp = r"(isnan(tableName.field)|isnull(tableName.field)).cast('int')"
    return temp.replace("tableName", tableName).replace("field",field)

def geneExp1(tableName, field1,field2):
    '''
    :param tableName: 表名，String
    :param field1: 字段名，String
    :param field2: 字段名，String
    :return: 逻辑表达式;String
    '''
    temp = r"(tableName.field1 == tableName.field2)"
    return temp.replace("tableName", tableName).replace("field1",field1).replace("field2",field2)

def checkInvalidRow(dataDF, threshold):
    '''
    :param dataDF: 数据表; DataFrame
    :param threshold:空值(Null/Nan)阈值;float or int
    :return:无效行个数，含有大于阈值的无效行序数和连续递增ID的DataFrame
    '''
    if type(threshold) is not float and type(threshold) is not int:
        raise TypeError('Invalid threshold')
    columns = dataDF.columns
    if len(columns)==0:
        raise Exception('bad_DF')
    if threshold < 0:
        raise ValueError('Invalid threshold')
    exp = geneExp("dataDF", dataDF.columns[0])
    for eachname in dataDF.columns[1:]:
        exp += '+'+geneExp("dataDF", eachname)
    if threshold>1:
        exp = exp+'>'+str(threshold)
    else:
        exp = "("+exp+")"+"/"+str(len(columns))+'>'+str(threshold)
    dataDF = dataDF.withColumn("isNullOrNan", eval(exp)).select("isNullOrNan")
    dataDF = addIdCol(dataDF, idColName="continuousID")
    dataDF = dataDF.filter(dataDF.isNullOrNan == True)
    dataDF = dataDF.select("continuousID") .selectExpr('continuousID as invalidRow')
    dataDF = addIdCol(dataDF, idColName="continuousID")
    invalidnum = dataDF.count()
    return invalidnum, dataDF

def checkInvalidCol(dataDF,threshold):
    '''
    :param dataDF: 数据表; DataFrame
    :param threshold:空值(Null/Nan)阈值;float or int
    :return:无效列个数，含有大于阈值的无效列列名和连续递增ID的DataFrame
    '''
    if type(threshold) is not float and type(threshold) is not int:
        raise TypeError('Invalid threshold')
    if threshold < 0:
        raise ValueError('Invalid threshold')
    miss1 = dataDF.agg(*[(1-(fn.count(value)/fn.count('*'))).alias(value) for value in dataDF.columns]).toPandas()
    miss2 = dataDF.agg(*[(fn.count('*')-fn.count(value)).alias(value) for value in dataDF.columns]).toPandas()
    invalidname = []
    invalidnum = 0
    if threshold <= 1 & threshold >= 0:
        for value in miss1.columns:
            if float(miss1[value]) > threshold:
                invalidname.append([value, invalidnum])
                invalidnum += 1
        schema = typ.StructType([typ.StructField('invalidColName', typ.StringType(), True), typ.StructField('ContinuesID', typ.IntegerType(), True)])
        invalidname = spark.createDataFrame(invalidname,schema=schema)
    else:
        for value in miss2.columns:
            if float(miss2[value])>threshold:
                invalidname.append([value,invalidnum])
                invalidnum += 1
        schema = typ.StructType([typ.StructField('invalidColName',typ.StringType(),True),typ.StructField('continuousID', typ.IntegerType(),True)])
        invalidname = spark.createDataFrame(invalidname,schema=schema)
    return invalidnum, invalidname




def changename(dataDF):
    '''
    :param dataDF: 数据表；DataFrame
    :return:  更改列表名后的数据表；DataFrame
    '''
    for name in dataDF.columns:
        dataDF = dataDF.withColumnRenamed(name, name + '_new')
    return dataDF

def checkDuplicateRow(dataDF,fieldNameList=None):
    '''
    :param dataDF: 数据表；DataFrame
    :param fieldNameList: 字段名；list
    :return:含有不包括首个重复行的重复行行标和连续递增ID的DataFrame
    '''
    if fieldNameList == None:
        df1 = addIdCol(dataDF)
        df2 = addIdCol1(dataDF)
        df2 = changename(df2)
    elif type(fieldNameList) != list:
        raise TypeError('Invalid fieldNameList')
    elif len(fieldNameList) == 1 and fieldNameList[0] not in dataDF.columns:
        raise ValueError('Invalid fieldNameList')
    else:
        for name in fieldNameList:
            if name not in dataDF.columns:
                raise ValueError('Invalid fieldNameList')
        df1 = addIdCol(dataDF.select(fieldNameList))
        df2 = addIdCol1(dataDF.select(fieldNameList))
        df2 = changename(df2)
    m = len(df1.columns)
    df_join = df1.join(df2, df1.continuousID == df2.continuousID_new)
    df_join = df_join.sort('continuousID')
    exp = geneExp1("df_join", df_join.columns[0],df_join.columns[m])
    i = 0
    for eachname in df_join.columns[1:m]:
        i = i+1
        eachname2 = df_join.columns[m+i]
        exp = exp +'&' + geneExp1("df_join", eachname,eachname2)
    dataDF = df_join.withColumn("isDuplicateOrNot", eval(exp).cast('int')).select("isDuplicateOrNot")
    dataDF = dataDF.rdd
    insertRow = sc.parallelize([Row(isDuplicateOrNot=0)])
    dataDF = sc.union([ insertRow,dataDF])
    dataDF = spark.createDataFrame(dataDF)
    dataDF = addIdCol(dataDF)
    return dataDF

'''
单元测试
'''
class TestcheckInvalidData(unittest.TestCase):
    def test_input(self):
        '''
        检测阈值输入的数据格式是否有误
        '''
        self.assertRaises(TypeError,checkInvalidRow, spark.createDataFrame([(None, 2, 3), (2, None, 6)], ['id', 'number1', 'number2']) ,'1')
        self.assertRaises(TypeError,checkInvalidCol,spark.createDataFrame([(None, 2, 3), (2, None, 6)], ['id', 'number1', 'number2']), [1])
    def test_inputvalue(self):
        '''
        检测阈值输入的数据是否大于等于0
        '''
        test = spark.createDataFrame([(None, 2, 3), (2, None, 6), (3, None, 9), (4, 3, None), (4, 3, None), (6, 2, None), (7, 5, 6), (8, 0, 2),(9, 2, None)], ['id', 'number1', 'number2'])
        self.assertRaises(ValueError, checkInvalidRow, test, -1)
        self.assertRaises(ValueError, checkInvalidCol, test, -1)
    def test_value(self):
        '''
        检测阈值输入的极端情况
        '''
        test = spark.createDataFrame([(None, 2, 3), (2, None, 6), (3, None, 9), (4, 3, None), (4, 3, None), (6, 2, None), (7, 5, 6), (8, 0, 2),(9, 2, None)], ['id', 'number1', 'number2'])
        a, b = checkInvalidRow(test, 9999)
        c, d = checkInvalidCol(test, 0)
        self.assertTrue(a == 0)
        self.assertTrue(b.dtypes == [('invalidRow', 'int'), ('continuousID', 'int')])
        self.assertTrue(c == 3)
        self.assertTrue(d.dtypes == [('invalidColName', 'string'), ('ContinuesID', 'int')])

class TestcheckDuplicateData(unittest.TestCase):

    def test_input(self):
        '''
            检测字段输入的数据格式是否有误
        '''
        test = spark.createDataFrame([(1, 1, 1), (4, 3, 3), (4, 3, 3), (3, 12, 12), (4, 3, 3)],['id', 'number1', 'number2'])
        self.assertRaises(ValueError, checkDuplicateRow, test, ['i'])
        self.assertRaises(TypeError, checkDuplicateRow, test, -1)
    def test_value(self):
        '''
            检测输入的表格数据与字段的各类情况
        '''
        test = spark.createDataFrame([(None, 2, 3), (2, None, 6), (3, None, 9), (4, 3, None), (4, 3, None), (6, 2, None), (7, 5, 6), (8, 0, 2),(8,0,2)], ['id', 'number1', 'number2'])
        a = checkDuplicateRow(test)
        b = checkDuplicateRow(test, ['number2'])
        test1 = spark.createDataFrame([(1,1,1),(2,1,1),(3,2,2),(4,2,2),(5,3,3),(6,4,4),(7,4,4),(8,4,4)],['id', 'number1', 'number2'])
        c = checkDuplicateRow(test1, ['number1','number2'])
        self.assertTrue(a.filter(a.isDuplicateOrNot==1).rdd.count()==1)
        self.assertTrue(b.filter(b.isDuplicateOrNot==1).rdd.count()==1)
        self.assertTrue(c.filter(c.isDuplicateOrNot==1).rdd.count()==4)
if __name__ == "__main__":
    unittest.main()
