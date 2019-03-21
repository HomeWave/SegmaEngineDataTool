# SegmaEngineDataTool
基于pyspark 开发的segma engine数据处理组件

# 测试
在Engine环境下分别运行如下命令，提交组件模块

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar dataBasicOpera.py

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar dataCleaning.py

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar dataTransfer.py

spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar dataInterface.py

# 如何调用组件？
1. git clone https://github.com/HomeWave/SegmaEngineDataTool/edit/master/README.md, clone本项目。
2. cd SegmaEngineDataTool-master，进入项目文件夹
3. zip dataTransfer.py dataInterface.py dataBasicOpera.py dataCleaning.py DataTool.zip，打包组件代码。
   注意：不要将代码放入文件夹后再打包，否则无法正确的导入。
4. import dataInterface, dataCleaning, dataTransfer, dataBasicOpera，在代码中导入四个模块，后续即可调用模块中的组件。（调用方法见文档）
5. 提交调用了组件的任务时需要同时提交组件压缩包，如下：
       spark-submit --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.10.0-78.jar --py-files DataTool.zip yourtask.py
       其中，--py-files后面跟的是组件代码压缩包，yourtask.py是利用python写的pyspark任务。
