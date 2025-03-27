#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder.getOrCreate()

iris = spark.read.csv('data/iris.csv', inferSchema=True, header=True)

#Label Encoding
indexer = StringIndexer(inputCol='class', outputCol='label')
iris = indexer.fit(iris).transform(iris)
iris.show(5)

#Vector Assemble
features = VectorAssembler(inputCols=[('sepallength'),('sepalwidth'),('petallength'),('petalwidth')], outputCol='features')
features = features.transform(iris)
features.show(5)

# Split Train and Test
train, test = features.randomSplit([0.8,0.2])

# Model
model = NaiveBayes(featuresCol='features', labelCol='label').fit(train)
prediction = model.transform(test)
prediction.show(15)

# Model evaluation
model_eval = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
acc = model_eval.evaluate(prediction)
print('Acuracia:', acc)