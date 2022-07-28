import pandas as pd 
import numpy as np 
import time

import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from IPython.core.display import display
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql import functions as f
from pyspark.sql import Row
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, RegexTokenizer, Tokenizer, CountVectorizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pprint import pprint
from sklearn.metrics import classification_report
import pyspark as ps
cluster = Cluster()
# if __name__ == "__main__":
session = cluster.connect('k1')
# spark = SparkSession.builder.appName('FinalProject').getOrCreate()
# spark.set("spark.executor.heartbeatInterval","3600s")
conf = ps.SparkConf().setMaster("yarn-client").setAppName("FinalProject")
conf.set("spark.executor.heartbeatInterval","3600s")   
spark = SparkSession.builder.appName('FinalProject').getOrCreate()   
if __name__ == "__main__": 
    rows = session.execute("Select * from train_data")
    # rows_2 = session.execute("Select * from stream_data")

    list_ID = []
    list_QUARTER=[]	
    list_MONTH = []									
    list_DAY_OF_MONTH = []
    list_DAY_OF_WEEK = []
    # list_FL_DATE = []
    list_OP_UNIQUE_CARRIER= []
    # list_OP_CARRIER_FL_NUM_NOR = []
    list_ORIGIN = []
    list_DEST = []
    list_CRS_DEP_TIME = []
    list_DISTANCE = []
    list_OUTPUT = []

    for row in rows:
        list_ID.append(str(row.id))
        list_QUARTER.append(row.quarter)
        list_MONTH.append(row.month)
        list_DAY_OF_MONTH.append(row.day_of_month)
        list_DAY_OF_WEEK.append(row.day_of_week)
        # list_FL_DATE.append(str(row.fl_date))
        list_OP_UNIQUE_CARRIER.append(row.op_unique_carrier)
        # list_OP_CARRIER_FL_NUM_NOR.append(row.op_carrier_fl_num_nor)
        list_ORIGIN.append(row.origin)
        list_DEST.append(row.dest)
        list_CRS_DEP_TIME.append(row.crs_dep_time)
        list_DISTANCE.append(row.distance)
        list_OUTPUT.append(row.label)


    # for row in rows_2:
    #     list_ID.append(str(row.id))
    #     list_QUARTER.append(row.quarter)
    #     list_MONTH.append(row.month)
    #     list_DAY_OF_MONTH.append(row.day_of_month)
    #     list_DAY_OF_WEEK.append(row.day_of_week)
    #     # list_FL_DATE.append(str(row.fl_date))
    #     list_OP_UNIQUE_CARRIER.append(row.op_unique_carrier)
    #     # list_OP_CARRIER_FL_NUM_NOR.append(row.op_carrier_fl_num_nor)
    #     list_ORIGIN.append(row.origin)
    #     list_DEST.append(row.dest)
    #     list_CRS_DEP_TIME.append(row.crs_dep_time)
    #     list_DISTANCE.append(row.distance)
    #     list_OUTPUT.append(row.label)


    df = pd.DataFrame(list(zip(list_ID,list_QUARTER,list_MONTH,list_DAY_OF_MONTH, \
                            list_DAY_OF_WEEK,list_OP_UNIQUE_CARRIER, \
                            list_ORIGIN,list_DEST, list_DISTANCE,\
                            list_CRS_DEP_TIME,list_OUTPUT)))


    # adding column name to the respective columns
    df.columns =['ID', 'QUARTER','MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', \
                'OP_UNIQUE_CARRIER','ORIGIN', \
                'DEST','DISTANCE','CRS_DEP_TIME','LABEL']   


    schema = '''ID STRING, QUARTER INT,MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT,
                OP_UNIQUE_CARRIER STRING,
                ORIGIN STRING,
                DEST STRING,DISTANCE DOUBLE,
                CRS_DEP_TIME DOUBLE, LABEL DOUBLE
                '''
    


            
    Train = spark.createDataFrame(df, schema=schema)

    Test = spark.read.csv('C:\kafka-demo\data\stream_final.csv', header=True, schema=schema)

    from pyspark.ml.classification import NaiveBayes

    # ohe1 = OneHotEncoder(inputCol = 'OP_UNIQUE_CARRIER_CATE', outputCol = 'OP_UNIQUE_CARRIER_CATE_OHE')
    # ohe2 = OneHotEncoder(inputCol = 'ORIGIN_CATE', outputCol = 'ORIGIN_CATE_OHE')
    # ohe3 = OneHotEncoder(inputCol = 'DEST_CATE', outputCol = 'DEST_CATE_OHE')

    # VA = VectorAssembler(inputCols = ['QUARTER', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
    #                                   'OP_UNIQUE_CARRIER_CATE_OHE', 'OP_CARRIER_FL_NUM_NOR',
    #                                   'ORIGIN_CATE_OHE','DEST_CATE_OHE',
    #                                   'DISTANCE_NOR', 'CRS_DEP_HOUR'], outputCol="features")

    # trainer = NaiveBayes(featuresCol='features', labelCol='OUTPUT', smoothing=1.0, modelType="multinomial")

    # pipeline = Pipeline(stages=[ohe1,ohe2,ohe3,VA,trainer])

    # model = pipeline.fit(Train)
    OP_UNIQUE_CARRIER_indexer = StringIndexer(inputCol='OP_UNIQUE_CARRIER',outputCol='OP_UNIQUE_CARRIERIndex')
    OP_UNIQUE_CARRIER_encoder = OneHotEncoder(inputCol='OP_UNIQUE_CARRIERIndex',outputCol='OP_UNIQUE_CARRIERVec')
    ORIGIN_indexer = StringIndexer(inputCol='ORIGIN',outputCol='ORIGINIndex')
    ORIGIN_encoder = OneHotEncoder(inputCol='ORIGINIndex',outputCol='ORIGINVec')
    DEST_indexer = StringIndexer(inputCol='DEST',outputCol='DESTIndex')
    DEST_encoder = OneHotEncoder(inputCol='DESTIndex',outputCol='DESTVec')

    assembler = VectorAssembler(inputCols=['QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIERVec', 'ORIGINVec', 
                                        'DESTVec', 'DISTANCE', 'CRS_DEP_TIME'], outputCol='features')#maxDepth=16
    DT = DecisionTreeClassifier(featuresCol='features',labelCol='LABEL')
    # NB = NaiveBayes(featuresCol='features',labelCol='LABEL', smoothing=0.0003, modelType="multinomial")
    pipeline = Pipeline(stages=[OP_UNIQUE_CARRIER_indexer, 
                                OP_UNIQUE_CARRIER_encoder, 
                                ORIGIN_indexer, 
                                ORIGIN_encoder, 
                                DEST_indexer, 
                                DEST_encoder, 
                                assembler, DT])
    model = pipeline.fit(Train)

    Tested = model.transform(Test)
    predicted_score = Tested.select('LABEL', 'prediction').toPandas()
    # print(classification_report(predicted_score.LABEL, predicted_score.prediction))

    model.write().overwrite().save("C:\kafka-demo\model")

    print(classification_report(predicted_score.LABEL, predicted_score.prediction))
    # execfile('C:\kafka-demo\\test.py')
    # import subprocess

    # spark_submit_str= "spark-submit test.py"
    # process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
    # stdout,stderr = process.communicate()
    # if process.returncode !=0:
    #     print(stderr)
    # print(stdout)
# while(True):
#     # time.sleep(10)
    

#     rows_2 = session.execute("Select * from stream_data")
    
#     list_ID = []
#     list_QUARTER=[]	
#     list_MONTH = []									
#     list_DAY_OF_MONTH = []
#     list_DAY_OF_WEEK = []
#     # list_FL_DATE = []
#     list_OP_UNIQUE_CARRIER= []
#     # list_OP_CARRIER_FL_NUM_NOR = []
#     list_ORIGIN = []
#     list_DEST = []
#     list_CRS_DEP_TIME = []
#     list_DISTANCE = []
#     list_OUTPUT = []




#     for row in rows_2:
#         list_ID.append(str(row.id))
#         list_QUARTER.append(row.quarter)
#         list_MONTH.append(row.month)
#         list_DAY_OF_MONTH.append(row.day_of_month)
#         list_DAY_OF_WEEK.append(row.day_of_week)
#         # list_FL_DATE.append(str(row.fl_date))
#         list_OP_UNIQUE_CARRIER.append(row.op_unique_carrier)
#         # list_OP_CARRIER_FL_NUM_NOR.append(row.op_carrier_fl_num_nor)
#         list_ORIGIN.append(row.origin)
#         list_DEST.append(row.dest)
#         list_CRS_DEP_TIME.append(row.crs_dep_time)
#         list_DISTANCE.append(row.distance)
#         list_OUTPUT.append(row.label)


#     df_2 = pd.DataFrame(list(zip(list_ID,list_QUARTER,list_MONTH,list_DAY_OF_MONTH, \
#                             list_DAY_OF_WEEK,list_OP_UNIQUE_CARRIER, \
#                             list_ORIGIN,list_DEST, list_DISTANCE,\
#                             list_CRS_DEP_TIME,list_OUTPUT)))


#     # adding column name to the respective columns
#     df_2.columns =['ID', 'QUARTER','MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', \
#                 'OP_UNIQUE_CARRIER','ORIGIN', \
#                 'DEST','DISTANCE','CRS_DEP_TIME','LABEL']   


#     schema = '''ID STRING, QUARTER INT,MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT,
#                 OP_UNIQUE_CARRIER STRING,
#                 ORIGIN STRING,
#                 DEST STRING,DISTANCE DOUBLE,
#                 CRS_DEP_TIME DOUBLE, LABEL DOUBLE
#                 '''
    

#     print("Update model")
            
#     Train = spark.createDataFrame(df, schema=schema)

#     Test = spark.read.csv('C:\kafka-demo\data\stream_final.csv', header=True, schema=schema)

#     from pyspark.ml.classification import NaiveBayes

#     # ohe1 = OneHotEncoder(inputCol = 'OP_UNIQUE_CARRIER_CATE', outputCol = 'OP_UNIQUE_CARRIER_CATE_OHE')
#     # ohe2 = OneHotEncoder(inputCol = 'ORIGIN_CATE', outputCol = 'ORIGIN_CATE_OHE')
#     # ohe3 = OneHotEncoder(inputCol = 'DEST_CATE', outputCol = 'DEST_CATE_OHE')

#     # VA = VectorAssembler(inputCols = ['QUARTER', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
#     #                                   'OP_UNIQUE_CARRIER_CATE_OHE', 'OP_CARRIER_FL_NUM_NOR',
#     #                                   'ORIGIN_CATE_OHE','DEST_CATE_OHE',
#     #                                   'DISTANCE_NOR', 'CRS_DEP_HOUR'], outputCol="features")

#     # trainer = NaiveBayes(featuresCol='features', labelCol='OUTPUT', smoothing=1.0, modelType="multinomial")

#     # pipeline = Pipeline(stages=[ohe1,ohe2,ohe3,VA,trainer])

#     # model = pipeline.fit(Train)
#     OP_UNIQUE_CARRIER_indexer = StringIndexer(inputCol='OP_UNIQUE_CARRIER',outputCol='OP_UNIQUE_CARRIERIndex')
#     OP_UNIQUE_CARRIER_encoder = OneHotEncoder(inputCol='OP_UNIQUE_CARRIERIndex',outputCol='OP_UNIQUE_CARRIERVec')
#     ORIGIN_indexer = StringIndexer(inputCol='ORIGIN',outputCol='ORIGINIndex')
#     ORIGIN_encoder = OneHotEncoder(inputCol='ORIGINIndex',outputCol='ORIGINVec')
#     DEST_indexer = StringIndexer(inputCol='DEST',outputCol='DESTIndex')
#     DEST_encoder = OneHotEncoder(inputCol='DESTIndex',outputCol='DESTVec')

#     assembler = VectorAssembler(inputCols=['QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIERVec', 'ORIGINVec', 
#                                         'DESTVec', 'DISTANCE', 'CRS_DEP_TIME'], outputCol='features')
#     # DT = DecisionTreeClassifier(maxDepth=16, featuresCol='features',labelCol='LABEL')
#     NB = NaiveBayes(featuresCol='features',labelCol='LABEL', smoothing=0.0003, modelType="multinomial")
#     pipeline = Pipeline(stages=[OP_UNIQUE_CARRIER_indexer, 
#                                 OP_UNIQUE_CARRIER_encoder, 
#                                 ORIGIN_indexer, 
#                                 ORIGIN_encoder, 
#                                 DEST_indexer, 
#                                 DEST_encoder, 
#                                 assembler, NB])
#     model = pipeline.fit(Train)

#     Tested = model.transform(Test)
#     predicted_score = Tested.select('LABEL', 'prediction').toPandas()
#     # print(classification_report(predicted_score.LABEL, predicted_score.prediction))

#     model.write().overwrite().save("C:\kafka-demo\model")

#     print(classification_report(predicted_score.LABEL, predicted_score.prediction))

