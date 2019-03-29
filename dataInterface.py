# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 20:42:26 2019

@author: bob
"""
# pyspark_hwc_path 路径可能变化。该地址用于指定python依赖pyspark_hwc的位置
pyspark_hwc_path = "/usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip"
import warnings
warnings.filterwarnings('ignore')
# 初始化sparkSession和HiveSession
from pyspark.sql import SparkSession
import unittest

'''
spark-Hive连接接口
'''
class HiveInterface():
    def __init__(self, hiveuser=r"hive", hivepassword=r""):
        self.spark=SparkSession \
        .builder \
        .appName('dataInterface') \
        .getOrCreate()
        self.spark.conf.set("hive.llap.daemon.service.hosts","@llap0")
        self.spark.sparkContext.addPyFile(pyspark_hwc_path)
        from pyspark_llap.sql.session import HiveWarehouseSession
        self.hive = HiveWarehouseSession.session(self.spark).userPassword(hiveuser, hivepassword).build()

    def linkHiveTable(self, databaseName='test', tableName='base_comp_main_orig', limitN=None, colName=[]):
        '''
        连接指定的hive数据表
        Input
            databaseName:数据库名
            tableName:数据表名
            limitN:连接数据表的行数
            colName:连接的数据表的列名
        Output
            dataDF:连接到的指定数据
        '''
        if colName == []:
            colName = r'*'
        else:
            colName = ','.join(colName)
        if limitN != None:
            dataDF = self.hive.executeQuery('select %s from %s limit %s'%(colName, databaseName+'.'+tableName, limitN))
        else:
            dataDF = self.hive.executeQuery('select %s from %s'%(colName, databaseName+'.'+tableName))
        return dataDF
    
    def saveAsHiveTable(self, dataDF, databaseName, tableName, saveMode=r"append"):
        '''
        将spark-dataframe保存到hive中
        Input
            dataDF:待保存的spark-dataframe对象
            databaseName:数据库名
            tableName:数据表名
        Output
            
        '''
        from pyspark_llap.sql.session import HiveWarehouseSession
        dataDF.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).\
        mode(saveMode=saveMode).option("table", databaseName+'.'+tableName).save()
        return True


#==============================================================================
#==============================================================================
'''
单元测试
'''
class TestHiveInterface(unittest.TestCase):
    def test_link(self):
        '''
        测试读Hive
        '''
        hif = HiveInterface()
        dataDF = hif.linkHiveTable(limitN=30)
        self.assertTrue('company_id' in dataDF.columns)
        
    def test_save(self):
        '''
        测试写Hive
        '''
        hif = HiveInterface()
        dataDF = hif.linkHiveTable(limitN=30)
        res = hif.saveAsHiveTable(dataDF,databaseName='test',tableName='test10')
        self.assertTrue(res)
    
        
if __name__=="__main__":
    # from pyspark_llap.sql.session import HiveWarehouseSession
    # spark.conf.set("hive.llap.daemon.service.hosts","@llap0")
    # hive = HiveWarehouseSession.session(spark).hs2url("jdbc:hive2://hdp-master2:10500").userPassword("hive", "").build()

    unittest.main()
