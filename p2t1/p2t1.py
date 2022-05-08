#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'
import re
import sys
from operator import add
from typing import Iterable,Tuple
from pyspark.resultiterable import ResultIterable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType


# In[2]:


## Run pagerank(p1t3q8) .ipynb immediately after running this cell

userSchema =  StructType([ StructField("_c0", StringType(), True), StructField("_c1", FloatType(), True)])

spark = SparkSession.builder.getOrCreate()

stream = spark.readStream     .option("sep", "\t")     .schema(userSchema)     .csv("gs://csee4121homework/outputs/p2t1_whole.csv/") 



path_checkpoint = 'gs://csee4121homework/outputs/whole_checkpoint/'
path2 = 'gs://csee4121homework/outputs/whole_receiver/'

receiver = stream    .select("_c0", "_c1")    .where("_c1 > 0.5")    .repartition(1)    .writeStream     .format("csv")     .option("mode","overwrite")    .option("sep", "\t")     .option("checkpointLocation", path_checkpoint)    .option("path", path2)     .start()


# In[3]:


receiver.stop()


# In[8]:



print("Number of articles in the database has a rank greater than 0.5:",spark.read.csv("gs://csee4121homework/outputs/whole_receiver",sep = "\t").count())

