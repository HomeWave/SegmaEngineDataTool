# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 16:12:06 2019

@author: bob
"""
import warnings
warnings.filterwarnings('ignore')
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
spark=SparkSession \
.builder \
.appName('basicOpera') \
.getOrCreate()

def selectRow(dataDF, tagDF, haveIdCol=False, idColName=None, tagName=None):
    '''
    选择数据的指定行
    :param dataDF: 数据表; DataFrame
    :param tagDF: 标识表，标识了待选择行; DataFrame
    :return: 选择后的数据表; DataFrame
    '''
    # 接口检测
    if type(spark.createDataFrame([[1]])) != type(tagDF):
        raise ValueError("标识表必须为spark DataFrame！")

    if len(tagDF.columns)!=1 and not haveIdCol:
        raise ValueError("标识表只能为单列表！")
    tagName = tagDF.columns[0]
    rowN = dataDF.count()
    tagRow = tagDF.count()
    # 异常输入检测
    if tagRow==0:
        return dataDF

    # 边界检测，行标是否符合规定
    if tagRow!=rowN:
        raise IndexError("输入的标识表行数应当与数据表一致！")

    # 对dataDf添加id列
    data_withindex = addIdCol(dataDF,idFieldName="id_left_temp_bob")

    # 生tagDF添加id列
    select_flag_withindex = addIdCol(tagDF, idFieldName="id_right_temp_bob")

    # 以id为主键拼接dataDF和标记表
    # print(select_flag_withindex.count())
    data_withindex2 = data_withindex.join(select_flag_withindex, data_withindex.id_left_temp_bob==select_flag_withindex.id_right_temp_bob)
    # 按照选择标记筛选行
    # print(data_withindex2.show())
    data_filtered = data_withindex2.filter(eval("data_withindex2."+tagName+">0"))
    # print(data_filtered.count())
    # 删除id_temp和标识行
    data_res = data_filtered.drop("id_left_temp_bob").drop("id_right_temp_bob")
    # data_res.show()
    # print(data_res.show(1))
    return data_res

def deleteRow(dataDF, tagDF, haveIdCol=False, idColName=None, tagName=None):
    '''
    删除数据的指定行
    :param dataDF: 数据表; DataFrame
    :param tagDF: 标识表，标识了待删除行; DataFrame
    :return: 选择后的数据表; DataFrame
    '''
    # 接口检测
    if type(spark.createDataFrame([[1]])) != type(tagDF):
        raise ValueError("标识表必须为spark DataFrame！")

    if len(tagDF.columns)!=1 and not haveIdCol:
        raise ValueError("标识表只能为单列表！")
    tagName = tagDF.columns[0]
    rowN = dataDF.count()
    tagRow = tagDF.count()
    # 异常输入检测
    if tagRow==0:
        return dataDF

    # 边界检测，行标是否符合规定
    if tagRow!=rowN:
        raise IndexError("输入的标识表行数应当与数据表一致！")

    # 对dataDf添加id列
    data_withindex = addIdCol(dataDF,idFieldName="id_left_temp_bob")

    # 生tagDF添加id列
    select_flag_withindex = addIdCol(tagDF, idFieldName="id_right_temp_bob")

    # 以id为主键拼接dataDF和标记表
    # print(select_flag_withindex.count())
    data_withindex2 = data_withindex.join(select_flag_withindex, data_withindex.id_left_temp_bob==select_flag_withindex.id_right_temp_bob)
    # 按照选择标记筛选行
    # print(data_withindex2.show())
    data_filtered = data_withindex2.filter(eval("data_withindex2."+tagName+"==0"))
    # print(data_filtered.count())
    # 删除id_temp和标识行
    data_res = data_filtered.drop("id_left_temp_bob").drop("id_right_temp_bob").drop(tagDF.columns[0])
    # data_res.show()
    # print(data_res.show(1))
    return data_res

# def selectRow(dataDF, rowIndex=[1,2,3,4]):
#     '''
#     选择数据的指定行
#     :param dataDF: 数据表; DataFrame
#     :param rowIndex: 标识了待选择行的索引; DataFrame
#     :return: 选择后的数据表; DataFrame
#     '''
#     # 异常输入检测
#     if rowIndex==[]:
#         return dataDF
#     rowN = dataDF.count()
#     # 边界检测，行标是否符合规定,raise IndexError("行标越界")
#     if len(rowIndex)>rowN:
#         raise IndexError("指定筛选的行标数超过数据表总行数！")
#     if min(rowIndex)<0 or max(rowIndex)>rowN-1:
#         raise IndexError("行标越界！")
#     # 对dataDf添加索引列
#     data_withindex = addIdCol(dataDF,idColName="id_left_temp_bob")
#
#     # 生成行选择标签表
#     select_flag = [[0]]*rowN
#     for each in rowIndex:
#         select_flag[each] = [1]
#     selectRow_flag_bob = spark.createDataFrame(spark.sparkContext.parallelize(select_flag),
#                                         StructType([StructField("selectRow_flag_bob", LongType(), True)])) # 标记列
#     # selectRow_flag_bob.show()
#     select_flag_withindex = addIdCol(selectRow_flag_bob, idColName="id_right_temp_bob")
#     # 以索引为主键拼接dataDF和标记表
#     # print(select_flag_withindex.count())
#     data_withindex2 = data_withindex.join(select_flag_withindex, data_withindex.id_left_temp_bob==select_flag_withindex.id_right_temp_bob)
#     # 按照选择标记筛选行
#     # print(data_withindex2.show())
#     data_filtered = data_withindex2.filter(data_withindex2.selectRow_flag_bob>0)
#     # print(data_filtered.count())
#     # 删除id_temp和标识行
#     data_res = data_filtered.drop("id_left_temp_bob").drop("id_right_temp_bob")
#     # data_res.show()
#     # print(data_res.show(1))
#     return data_res


def changeFieldName(dataDF, origName, newName):
    '''
    修改字段名
    Input
        dataDF:待处理的数据表
        origName:原字段名
        newName:新字段名
    '''
    return dataDF.withColumnRenamed(origName, newName)

def filterRow(dataDF, cond):
    '''
    自定义条件筛选行
    条件表达式：一个Bool类型的Column，或者SQL字符串表达式
    '''
    return dataDF.filter(cond)

def selectCol(dataDF, fieldNameList):
    '''
    选择列
    '''
    return dataDF.select(fieldNameList)

def deleteCol(dataDF, fieldNameList):
    '''
    删除列
    '''
    for each in fieldNameList:
        dataDF = dataDF.drop(each)
    return dataDF

def addIdCol(dataDF, idFieldName="continuousID"):
    '''
    添加连续递增id列
    :param dataDF: 数据表; DataFrame
    :param idFieldName: id列名
    :return: 增加id列的数据表; DataFrame
    '''
    numParitions = dataDF.rdd.getNumPartitions() # 获取分区数
    data_withindex = dataDF.withColumn("increasing_id_temp", monotonically_increasing_id()) # 添加递增列
    data_withindex = data_withindex.withColumn(idFieldName, row_number().over(Window.orderBy("increasing_id_temp"))-1) # 生成连续递增id列，此时分区数为1
    data_withindex = data_withindex.repartition(numParitions) # 按原分区数重新分区，所以不用紧张前面分区数变为1时报出的性能降低警告
    data_withindex = data_withindex.sort("increasing_id_temp") # 由于重分区后会打乱顺序，需重新排序
    data_withindex = data_withindex.drop("increasing_id_temp") # 删除临时生成的递增列
    return data_withindex

def selectRowByPartField(dataDF, partValueDF):
    '''
    利用部分字段值，匹配选择数据
    :param dataDF: 数据表; DataFrame
    :param partValueDF: 包含部分字段的数据表
    :return: 选择后的数据表; DataFrame
    '''
    # 接口检测
    if type(spark.createDataFrame([[1]])) != type(partValueDF):
        raise ValueError("输入表必须为spark DataFrame！")
    col1 = dataDF.columns
    col2 = partValueDF.columns
    for each in col2:
        if each not in col1:
            raise KeyError(each+"字段在数据表中不存在！")
    col3 = [each+'_1' for each in col2]
    changelist = []
    exp = "dataDF"
    exp_d = "dataDF"
    for i, each in enumerate(col3):
        changelist.append(col2[i]+" as "+col3[i]) # 修改列名
        exp += ".filter(dataDF."+col2[i]+"==dataDF."+col3[i]+")"
        exp_d += ".drop('"+col3[i]+"')"
    partValueDF = partValueDF.selectExpr(changelist)
    dataDF = dataDF.join(partValueDF, eval("dataDF."+col2[0]+'==partValueDF.'+col3[0]))
    dataDF = eval(exp)
    return eval(exp_d)

def StandardDeviation(dataDF):
    '''
    计算一列的标准差
    :param dataDF:只有一列的数据表,DataFrame
    :return:标准差的值,float
    '''
    # 接口检测
    if type(spark.createDataFrame([[1]])) != type(dataDF):
        raise TypeError("标识表必须为spark DataFrame！")
    if len(dataDF.columns) != 1 :
        raise TypeError("标识表只能为单列表！")
    data = dataDF.toPandas()
    STD = data.std()
    STD = float(STD)
    return STD


#==============================================================================
#==============================================================================
'''
单元测试
'''
class TestBasicOpera1(unittest.TestCase):
    def setUp(self):
        '''
        初始化测试环境
        '''
        print('生成测试用数据')
        import numpy as np
        data = np.ones((10000,5)).tolist()
        self.dataDF = spark.createDataFrame(data)
        
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
        tag_list = [[0]] * self.dataDF.count()
        tag_list[0] = [1] # 选择第一行
        tagDF = spark.createDataFrame(tag_list)
        tagDF = changeFieldName(tagDF, '_1', 'tag')
        resDF = selectRow(self.dataDF, tagDF)
        self.assertTrue(resDF.count()==1)
        
    def test_selectRow_boundary(self):
        '''
        测试行选择，行标边界测试，异常测试
        '''
        # tagDF行数超过dataDF行数
        with self.assertRaises(IndexError):
            tag_list = [[0]] * (self.dataDF.count()+1)
            tag_list[0] = [1]  # 选择第一行
            tagDF = spark.createDataFrame(tag_list)
            tagDF = changeFieldName(tagDF, '_1', 'tag')
            resDF = selectRow(self.dataDF, tagDF)
        # tagDF行数小于dataDF行数
        with self.assertRaises(IndexError):
            tag_list = [[0]] * (self.dataDF.count()-1)
            tag_list[0] = [1]  # 选择第一行
            tagDF = spark.createDataFrame(tag_list)
            tagDF = changeFieldName(tagDF, '_1', 'tag')
            resDF = selectRow(self.dataDF, tagDF)

    def test_selectRow_tagDF(self):
        '''
        测试行选择，测试tagDF参数检查
        '''
        # tagDF为非dataframe
        with self.assertRaises(ValueError):
            resDF = selectRow(self.dataDF, [1,2,3,4])
        # tagDF有多列
        with self.assertRaises(ValueError):
            tag_list = [[0, 1]] * (self.dataDF.count())
            tagDF = spark.createDataFrame(tag_list)
            tagDF = changeFieldName(tagDF, '_1', 'tag')
            resDF = selectRow(self.dataDF, tagDF)
            
    def test_filterRow(self):
        '''
        测试条件筛选行
        '''
        # 无需测试
        pass

    def test_changeFieldName(self):
        '''
        测试修改列名
        '''
        # 无需测试
        pass

    def test_selectRowByPartField(self):
        '''
        测试根据部分字段选择数据行
        '''
        #
        res = selectRowByPartField(spark.createDataFrame([[1,2,3,4],[3,5,6,7]]), spark.createDataFrame([[1,2],[3,4]]))
        print(res.show())

    def test_StandardDeviation(self):
        '''
        测试接口和输出格式
        '''
        self.assertRaises(TypeError, StandardDeviation,spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")))
        test = spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))
        STD = StandardDeviation(test.select('id'))
        self.assertTrue(type(STD) == float)

if __name__=="__main__":
    # from pyspark.sql import SparkSession
    # spark=SparkSession \
    # .builder \
    # .appName('bob_app') \
    # .getOrCreate()
    # spark.sparkContext.addPyFile("/usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip") # 导入依赖的模块
    # spark.sparkContext.addPyFile("/home/hdfs/bob/packages/dataInterface.py") # 导入依赖的模块
    unittest.main()
     
    
    
    

    
