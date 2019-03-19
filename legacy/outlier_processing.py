#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
import unittest

spark = SparkSession.builder .appName('my_first_app_name').getOrCreate()


def checkOutlier(tableName, ColName, n=3):
    '''
    该函数可以检测离群值，离群限制范围由n值决定，具体概率范围可以计算
    仅支持数值型列！！（int,float,double）
    :param tableName: 传入数据表名称
    :param ColName: 需要检测离群值的字段名
    :param n: 离群值范围（用3sigma法则确定，默认参数为3（99.74%））
    :return: finaltbale：输出是否是离群值以及对应的行号；nums: 离群值总数
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

            def addIdCol(dataDF, idColName="continuousID"):
                numParitions = dataDF.rdd.getNumPartitions()
                data_withindex = dataDF.withColumn("increasing_id_temp", monotonically_increasing_id())
                data_withindex = data_withindex.withColumn(idColName,
                                                           row_number().over(Window.orderBy("increasing_id_temp")) - 1)
                data_withindex = data_withindex.repartition(numParitions)
                data_withindex = data_withindex.sort("increasing_id_temp")
                data_withindex = data_withindex.drop("increasing_id_temp")
                return data_withindex

            final_table = addIdCol(out_table).drop(ColName)
            # final_table.show()
            nums = final_table.filter(final_table.is_outlier == 1).count()
            # print(nums)
            return final_table, nums


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


if __name__ == '__main__':
    unittest.main()



