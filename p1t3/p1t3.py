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


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("gs://csee4121homework/outputs/p1t2_small.csv",sep = "\t").dropna(subset=["_c1"])


# In[3]:


def parse(urls):
    return (urls[0],urls[1])

def computecontribution(neighbors,rank):
    for neighbor in neighbors:
        yield (neighbor, rank/len(neighbors))


links = df.rdd.map(lambda line:parse(line))
links = links.distinct().groupByKey()

##initialize rank to 1
ranks = links.map(lambda url_neighbors:(url_neighbors[0],1.0))

for i in range(10):
    contributions = links.join(ranks).    flatMap(lambda rank: computecontribution(rank[1][0],rank[1][1])) #individual contributions
    ### map null values to 0
    contributions = links.fullOuterJoin(contributions).    mapValues(lambda neighbors: neighbors[1] or 0)
    
    ranks = contributions.reduceByKey(add).mapValues(lambda rank:rank*0.85 + 0.15)



# In[4]:


ranks.toDF(["_c0","_c1"]).withColumnRenamed("_c0","articles").withColumnRenamed("_c1","links").dropna().repartition(1).orderBy(["articles","links"]).limit(5).write.csv("gs://csee4121homework/outputs/p1t3.csv",mode="overwrite",sep = "\t")

