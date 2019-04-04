#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import warnings
warnings.filterwarnings('ignore')
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, isnull
import unittest

spark = SparkSession.builder .appName('my_first_app_name').getOrCreate()


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
            print('---------- This Column Has NO NULL OR NAN VALUE-------------')
        elif num_notna == 0:  # 是否全为缺失值
            print('----------- This Column IS ALL Missing Values -------------')
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
if __name__ == '__main__':
    unittest.main()












