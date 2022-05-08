#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')


# In[3]:


import regex
from pyspark.sql.types import StringType,ArrayType
from pyspark.sql.functions import udf, col,explode


def clean_title(titles):
    #All the letters should be convert to lower case.
    return titles.lower()

def parse(match_):
    ret = None
    match_ = match_.split("|")
    for ele in match_:
        if "#" in ele:
            continue
        elif (":" in ele) and (not ele.startswith("Category:")):
            continue
        else:
            ret = ele
            break
            
    if ret:
        return ret.lower()
    else:
        return ret

def clean_text(revisions):
    #All the letters should be convert to lower case.
    text = revisions["text"]['_VALUE']
    links_ = []
    #If multiple links appear in the brackets, take the first one.(Ignore links that contain a #)
    matches = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', text)
    for match in matches:
            link = parse(match)
            if link:
                links_.append(link)
    if len(links_) > 0:
        return links_
    else:
        return None
    
           
clean_title_udf = udf(lambda k: clean_title(k), StringType())
clean_text_udf = udf(lambda k: clean_text(k), ArrayType(StringType()))


output = df.withColumn("title",clean_title_udf(col("title"))).withColumn("links", clean_text_udf(col("revision"))).select("title",explode("links")).withColumnRenamed("col","links")

output.filter(output.links.isNotNull()).repartition(1).orderBy(["title","links"]).limit(5).write.csv("gs://csee4121homework/outputs/p1t2.csv",mode="overwrite",sep = "\t")

