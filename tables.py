#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import shutil

spark = SparkSession.builder.getOrCreate()

# remove directory of database
shutil.rmtree('spark-warehouse')

spark.sql('create database despachantes')
spark.sql('show databases').show()
spark.sql('use despachantes')

schema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("data/despachantes.csv", schema=schema, sep=',', header=False)

despachantes.write.saveAsTable('despachantes', mode='overwrite')
spark.sql('show tables').show()