#!/usr/bin/env python
# -*- coding: utf-8 -*-
# encoding=utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


spark = SparkSession.builder.getOrCreate()

clientes = spark.read.parquet("data/clientes.parquet")

clientes.select('Cliente', 'Estado', 'Status').orderBy('Cliente', 'Estado', 'Status').show()

clientes.select('Cliente', 'Status').where((f.col('Status') == 'Platinum') | (f.col('Status') == 'Gold')).show()

vendas = spark.read.parquet("data/vendas.parquet")

vendas.join(clientes, vendas.ClienteID == clientes.ClienteID).groupBy(clientes.Status).agg(f.sum('Total')).show()

# itens_vendas = spark.read.parquet("data/itens_vendas.parquet")
# produtos = spark.read.parquet("data/produtos.parquet")

# vendedores = spark.read.parquet("data/vendedores.parquet")

