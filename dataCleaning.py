# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 14:37:26 2019

@author: sijun, jerry, bob
"""
import warnings
warnings.filterwarnings('ignore')
# 初始化sparkSession和HiveSession
import pyspark.sql.functions as fn
from  pyspark.sql import Row
import pyspark.sql.types as typ
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, isnull
from pyspark.sql import Window
from dataBasicOpera import addIdCol
import unittest

spark = SparkSession.builder.appName('dataCleaning').getOrCreate()
sc = spark.sparkContext

def addIdCol1(dataDF, idFieldName = "continuousID"):
    '''
        :param dataDF: 数据表; DataFrame
        :param idColName: 生成的ID列的名称：String
        :return: 末列添加了从1开始的连续递增ID的列表；DataFrame
    '''
    numParitions = dataDF.rdd.getNumPartitions()
    data_withindex = dataDF.withColumn("increasing_id_temp", fn.monotonically_increasing_id())
    data_withindex = data_withindex.withColumn(idFieldName, fn.row_number().over(Window.orderBy("increasing_id_temp")))
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
    :return:无效行数invalidnum，无效行标记tagDF
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
    tagDF = dataDF.withColumn("isNullOrNan", eval(exp)).select("isNullOrNan")
    tagDF = tagDF.selectExpr('isNullOrNan as isInvalid')
    invalidnum = tagDF.filter(tagDF.isInvalid==True).count()
    return invalidnum, tagDF

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
    :return:含有不包括首个重复行的重复行行标tagDF
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
    tagDF = df_join.withColumn("isDuplicateOrNot", eval(exp).cast('int')).select("isDuplicateOrNot")
    tagDF = tagDF.rdd
    insertRow = sc.parallelize([Row(isDuplicateOrNot=0)])
    tagDF = sc.union([ insertRow,tagDF])
    tagDF = spark.createDataFrame(tagDF)
    return tagDF


def checkOutlier(tableName, ColName, n=3):
    '''
    该函数可以检测离群值，离群限制范围由n值决定，具体概率范围可以计算
    仅支持数值型列！！（int,float,double）
    :param tableName: 传入数据表名称
    :param ColName: 需要检测离群值的字段名
    :param n: 离群值范围（用3sigma法则确定，默认参数为3（99.74%））
    :return: finaltbale：；nums: 离群值总数
    '''

    if ColName not in tableName.columns:
        raise KeyError('---Input Column Name IS NOT IN  the input Table--')
    else:
        col_type = tableName.select(tableName[ColName]).dtypes[0][1]
        if col_type != "int" and col_type != "float" and col_type != "double":
            raise TypeError('-------------- Type Error: The type of column must be INT or FLOAT! ---------')
        elif n <= 0:
            raise ValueError('--------------Input n must bigger than ZERO (No Meaning when n<=0)--------------')
        else:
            desc_col = tableName.describe(ColName)
            pandas_desc = desc_col.toPandas()
            mean = pandas_desc.ix[1, ColName]
            std = pandas_desc.ix[2, ColName]
            mean = float(mean)
            std = float(std)
            max_range = mean + n * std
            min_range = mean - n * std

            out_table = tableName.select(tableName[ColName],
                                         F.when((tableName[ColName] > max_range) | (tableName[ColName] < min_range),
                                                1).otherwise(0).alias('is_outlier'))

            tagDF = out_table.drop(ColName)
            # final_table.show()
            nums = tagDF.filter(tagDF.is_outlier == 1).count()
            # print(nums)
            return tagDF, nums

def nanProcess(tableName, ColName, Method, Filling_Manually_Value=None):
    '''
    此函数可以处理指定表指定列的缺失值，并返回处理后的表格
    :param tableName: 需要处理的表
    :param ColName:  需要处理的的字段名
    :param Method: 选择处理方法（Filling_Manually；Mean_Completer；Min_Completer；Max_Completer；Mode_Completer）
                    其中Min，Max,Mean只能处理float，int，double型数值
    :param Filling_Manually_Value: 当选择Filling_Manually方法后需要自主选择填充值（必须键入，默认参数为None）
                                   注意填充值要与原行的数据类型相同
    :return: 处理后的表格
    '''
    if ColName not in tableName.columns:
        raise KeyError('--------Input Column Name IS NOT IN  the input Table--------')
    else:
        # 选取目标列
        Coltable = tableName.select(tableName[ColName])

        # 检测缺失值数目
        num_null_nan = Coltable.filter(Coltable[ColName].isNull()).count() + Coltable.filter(isnan(ColName)).count()
        # print(num_null_nan)

        # 所选列数据类型
        type = tableName.select(tableName[ColName]).dtypes[0][1]
        # print(type)

        # 剔除空值后所得表
        NotN_tabel = Coltable.na.drop()
        num_notna = NotN_tabel.count()
        # print(num_notna)

        # 非空表描述性统计
        desc_col = NotN_tabel.describe()
        pandas_desc = desc_col.toPandas()

        if num_null_nan == 0:  # 检测缺失值是否为0
            raise ValueError('---------- This Column Has NO NULL OR NAN VALUE-------------')
        elif num_notna == 0:  # 是否全为缺失值
            raise ValueError('----------- This Column IS ALL Missing Values -------------')
        else:
            if Method == "Filling_Manually":  # 人为自主填充
                filled_table = tableName.na.fill({ColName: Filling_Manually_Value})
                # filled_table.show()
                return filled_table

            elif Method == "Mean_Completer":  # 均值填充
                if type == "int" or type == "float" or type == "double":
                    mean = pandas_desc.ix[1, ColName]
                    mean_n = float(mean)
                    filled_table = tableName.na.fill({ColName: mean_n})
                    # filled_table.show()
                    return filled_table
                else:
                    raise TypeError('---Mean_Completer ONLY work for INT, FLOAT, DOUBLE value---')

            elif Method == "Min_Completer":  # 最小值填充
                if type == "int" or type == "float" or type == "double":
                    min = pandas_desc.ix[3, ColName]
                    min_n = float(min)
                    filled_table = tableName.na.fill({ColName: min_n})
                    # filled_table.show()
                    return filled_table
                else:
                    raise TypeError('---Min_Completer ONLY work for INT, FLOAT, DOUBLE value---')

            elif Method == "Max_Completer":  # 最大值填充
                if type == "int" or type == "float" or type == "double":
                    max = pandas_desc.ix[4, ColName]
                    max_n = float(max)
                    filled_table = tableName.na.fill({ColName: max_n})
                    # filled_table.show()
                    return filled_table
                else:
                    raise TypeError('---Max_Completer ONLY work for INT, FLOAT, DOUBLE value---')

            elif Method == "Mode_Completer":  # 众数填充
                count_table = NotN_tabel.groupby(ColName).count()
                mode_table = count_table.sort(count_table['count'].desc())
                mode_row = mode_table.head(1)[0]
                mode = mode_row[ColName]
                filled_table = tableName.na.fill({ColName: mode})
                # filled_table.show()
                return filled_table
            else:
                raise KeyError('--DO NOT Have THIS Method--; ',
                               'Method : Filling_Manually;Mean_Completer;Min_Completer;Max_Completer;Mode_Completer;')

def valueMap(TableName, to_replace, value, subset=None):
    '''

    :param TableName: input table
    :param to_replace: bool, int, long, float, string, list or dict.
            Value to be replaced.
            If the value is a dict, then `value` is ignored or can be omitted, and `to_replace`
            must be a mapping between a value and a replacement.
    :param value:bool, int, long, float, string, list or None.
            The replacement value must be a bool, int, long, float, string or None. If `value` is a
            list, `value` should be of the same length and type as `to_replace`.
            If `value` is a scalar and `to_replace` is a sequence, then `value` is
            used as a replacement for each item in `to_replace`.

    :param subset:optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

    :return:
    '''

    replace_table = TableName.na.replace(to_replace, value, subset)
    # replace_table.show()
    return replace_table

'''
单元测试
'''

class Test_checkOutlier(unittest.TestCase):
    def setUp(self):
        df = spark.createDataFrame(
            [(10, 1.0, 'lily'), (18, 1.85, 'marry'), (25, 1.75, 'james'), (15, 1.80, 'kris'), (50, 1.60, 'colin'),
             (66, 1.55, 'best')],
            ["age", "height", "name"])
        df1 = df.withColumn("age", df['age'].cast('float'))
        self.dataDF = df1.withColumn("height", df['height'].cast('float'))

    def tearDown(self):
        '''
        退出函数
        '''
        # print('finish')
        pass

    def test_input_wrong(self):
        with self.assertRaises(KeyError):
            result = checkOutlier(self.dataDF, "gender")
        with self.assertRaises(ValueError):
            result1 = checkOutlier(self.dataDF, "age", -3)

    def test_col_type(self):
        with self.assertRaises(TypeError):
            result = checkOutlier(self.dataDF, "name")

    def test_nums(self):
        result1 = checkOutlier(self.dataDF, "age")
        self.assertEqual(result1[1], 0)
        result2 = checkOutlier(self.dataDF, "height", 0.5)
        self.assertEqual(result2[1], 4)

    def test_n_input_big_or_small(self):
        result1 = checkOutlier(self.dataDF, "age", 999999)
        self.assertEqual(result1[1], 0)
        result2 = checkOutlier(self.dataDF, "height", 0.000001)
        self.assertEqual(result2[1], 6)



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

class Test_nanProcess(unittest.TestCase):
    def setUp(self):
        df = spark.createDataFrame([(1.0, 3.0, float('nan'), 'n'), (float('nan'), 2.0, float('nan'), 'n'),
                                    (None, 2.0, float('nan'), 'y'), (1.1, 2.0, float('nan'), 'y'),
                                    (None, 3.0, float('nan'), None),
                                    (1.1, 3.0, float('nan'), 'y')], ("a", "b", "c", "z"))
        self.dataDF = df

    def tearDown(self):
        '''
        退出函数
        '''
        # print('finish')
        pass

    def test_input_wrong(self):
        with self.assertRaises(KeyError):
            result = nanProcess(self.dataDF, "fx", 'Mean_Completer')
        with self.assertRaises(KeyError):
            result1 = nanProcess(self.dataDF, "a", 'Completer')

    def test_col_value(self):
        with self.assertRaises(ValueError):
            result = nanProcess(self.dataDF, "c", "Mode_Completer")
        with self.assertRaises(ValueError):
            result1 = nanProcess(self.dataDF, "b", "Mode_Completer")

    def test_col_type(self):
        with self.assertRaises(TypeError):
            result = nanProcess(self.dataDF, "z", "Mean_Completer")
        with self.assertRaises(TypeError):
            result1 = nanProcess(self.dataDF, "z", "Min_Completer")
        with self.assertRaises(TypeError):
            result2 = nanProcess(self.dataDF, "z", "Max_Completer")

    def test_result(self):
        result = nanProcess(self.dataDF, "a", "Mean_Completer")
        num_null_nan = result.filter(result["a"].isNull()).count() + result.filter(isnan("a")).count()
        self.assertEqual(num_null_nan, 0)

        result1 = nanProcess(self.dataDF, "a", "Min_Completer")
        num_null_nan1 = result1.filter(result1["a"].isNull()).count() + result1.filter(isnan("a")).count()
        self.assertEqual(num_null_nan1, 0)

        result2 = nanProcess(self.dataDF, "a", "Max_Completer")
        num_null_nan2 = result2.filter(result2["a"].isNull()).count() + result2.filter(isnan("a")).count()
        self.assertEqual(num_null_nan2, 0)

        result3 = nanProcess(self.dataDF, "a", "Mode_Completer")
        num_null_nan3 = result3.filter(result3["a"].isNull()).count() + result3.filter(isnan("a")).count()
        self.assertEqual(num_null_nan3, 0)

        result4 = nanProcess(self.dataDF, "a", "Filling_Manually", 2.0)
        num_null_nan4 = result4.filter(result4["a"].isNull()).count() + result4.filter(isnan("a")).count()
        self.assertEqual(num_null_nan4, 0)
if __name__ == "__main__":
    unittest.main()
