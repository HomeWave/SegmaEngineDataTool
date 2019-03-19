# SegmaEngineDataTool
基于pyspark 开发的segma engine数据处理组件

# 测试
在Engine环境下分别运行如下命令，提交组件模块
spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar --py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip dataBasicOpera.py

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar --py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip dataCleaning.py

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar --py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip dataTransfer.py

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar --py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.0.0-78.zip dataInterface.py
