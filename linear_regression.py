#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
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

prediction = model.transform(test)

prediction.show(5)

model_eval = RegressionEvaluator(predictionCol='prediction', labelCol='HP')

rmse = model_eval.evaluate(prediction)
print('Raiz quadrada do erro-médio (LinearRegression):', rmse)

model = RandomForestRegressor(featuresCol='Features', labelCol='HP').fit(train)
prediction = model.transform(test)
rmse = model_eval.evaluate(prediction)
print('Raiz quadrada do erro-médio (RandomForestRegressor):', rmse)
