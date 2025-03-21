#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.getOrCreate()

my_schema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

despachantes = spark.read.csv('data/despachantes.csv', header=False, schema=my_schema)

despachantes.select('id', 'nome').where(f.col('vendas')> 20).show()
