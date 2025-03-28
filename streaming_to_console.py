#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Streaming Reader').getOrCreate()
jsonSchema = 'nome STRING, postagem STRING, data INT'

data = spark.readStream.json('data/streaming/', schema=jsonSchema)

sessionDirectory = 'data/app_session'

app = data.writeStream.format('console').outputMode('append').trigger(processingTime='5 second').option('checkpointlocation', sessionDirectory)

app.start().awaitTermination()
