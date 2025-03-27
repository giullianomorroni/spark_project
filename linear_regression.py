#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler


spark = SparkSession.builder.getOrCreate()

carros = spark.read.csv('data/carros.csv', inferSchema=True, header=True, sep=';')

dados = carros.select('Consumo', 'Cilindros', 'Cilindradas', 'HP')

vetor = VectorAssembler(inputCols=[('Consumo'), ('Cilindros'), ('Cilindradas')], outputCol='Features')

features = vetor.transform(dados)

features.show(5)

train, test = features.randomSplit([0.7, 0.3])

model = LinearRegression(featuresCol='Features', labelCol='HP').fit(train)

model.transform(test).show(5)
