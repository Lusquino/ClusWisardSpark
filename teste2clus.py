# -*- coding: utf-8 -*-
"""
Created on Mon Aug 27 20:08:04 2018

@author: leopoldolusquino
"""

'''import h5py
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="SparkHDF5")
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    #################################################
    # read a dataset and return it as a Python list #4
    def f(x):
        a = x.split(",")
        with h5py.File(a[0]) as f:
            result = f[a[1]]
            return list(result[:])

   ################################################

    file_paths = sc.textFile("/Users/leopoldolusquino/Documents/Doutorado/BD/climo_1979.h5", minPartitions=partitions)
    
    rdd = file_paths.flatMap(f)
    
    print "\ncount %d : min %f : mean %f : stdev %f : \
        max %f\n" % (rdd.count(), rdd.min(), rdd.mean(),\
        rdd.stdev(), rdd.max())'''
#treino        
import h5py
filename = '/Users/leopoldolusquino/Documents/Doutorado/BD/climo_1979.h5'
f = h5py.File(filename, 'r')

# List all groups
print("Keys: %s" % f.keys())
a_group_key = list(f.keys())[1]

print(a_group_key)

# Get the data
data = list(f[a_group_key])

dados_planos = [val for sublist in data for val in sublist]
dados_planos_2 = [val for sublist in dados_planos for val in sublist]

count = 0

dados = []
dados_int = []

for dado in dados_planos_2:
    count += 1
    dados_int.append(dado)

    if count == 5:
        count = 0
        dados.append(dados_int)
        dados_int = []
        

from pandas import DataFrame
df = DataFrame(dados)
df.columns = ['x1', 'y1', 'x2', 'y2', 'label']

from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local") \
      .appName("ImageClassification") \
      .config("spark.executor.memory", "6gb") \
      .getOrCreate()

df_spark = spark.createDataFrame(df)
df_spark.show()

import time

from filler_column_transformer import FillerColumnTransformer

ini = time.time()

transformer1 = FillerColumnTransformer()

t1 = transformer1._transform_x1(df_spark)
t2 = transformer1._transform_y1(t1)
t3 = transformer1._transform_x2(t2)
t4 = transformer1._transform_y2(t3)

print(t4.columns)

import numpy as np

test = np.array(t4.select("integralX1").collect())
entradas = [v for sublist in test for v in sublist]
#print(entradas)

treino = transformer1._transform_2(t4)
treino.show()


#######################################################
#######################################################
#######################################################


filename = '/Users/leopoldolusquino/Documents/Doutorado/BD/climo_1987.h5'
f = h5py.File(filename, 'r')

# List all groups
print("Keys: %s" % f.keys())
a_group_key = list(f.keys())[1]

print(a_group_key)

# Get the data
data = list(f[a_group_key])

dados_planos = [val for sublist in data for val in sublist]
dados_planos_2 = [val for sublist in dados_planos for val in sublist]

count = 0

dados = []
dados_int = []

for dado in dados_planos_2:
    count += 1
    dados_int.append(dado)

    if count == 5:
        count = 0
        dados.append(dados_int)
        dados_int = []
        
df = DataFrame(dados)
df.columns = ['x1', 'y1', 'x2', 'y2', 'label']

transformer1 = FillerColumnTransformer()

t1 = transformer1._transform_x1(df_spark)
t2 = transformer1._transform_y1(t1)
t3 = transformer1._transform_x2(t2)
t4 = transformer1._transform_y2(t3)

print(t4.columns)

test = np.array(t4.select("integralX1").collect())
entradas = [v for sublist in test for v in sublist]
#print(entradas)

teste = transformer1._transform_2(t4)
teste.show()

fim = time.time()

print("TEMPO###################")
print(fim-ini)

#########################################################
#########################################################
#########################################################

from cluswisard_estimator import CluswisardEstimator

ini = time.time()

clus = CluswisardEstimator(4, 48, 4, 10, 0.1)
clus.treinar(treino, "features", "label")

classificacoes = clus._fit(teste, "features")

classificacoes.show()

fim = time.time()

print("TEMPO###################")
print(fim-ini)

classify = np.array(classificacoes.select("predicoes").collect())
entradas_classify = [v for sublist in classify for v in sublist]

gabarito = np.array(classificacoes.select("label").collect())
entradas_gabarito = [v for sublist in gabarito for v in sublist]

matriz_confusao = [[0]*4 for i in range(0, 4)]
acertos = 0
count = 0

print(entradas_classify)
print(entradas_gabarito)

for i in range(0, len(entradas_classify)):
    #print(int(entradas_gabarito[i]))
    #print(int(entradas_classify[i]))
    #print(matriz_confusao)
    #print(str(entradas_gabarito[i]) + " " + str(entradas_classify[i]))
    if(not(entradas_gabarito[i] == -1)):
        count += 1
        matriz_confusao[int(entradas_gabarito[i])][int(entradas_classify[i])] += 1
        if(str(entradas_gabarito[i]) == str(entradas_classify[i])):
            acertos += 1
        
print(acertos)
print(count)
print(matriz_confusao)

#print(np.array(t.select("input").collect())[0])
#print(len(np.array(t.select("input").collect())[0][0]))

print("OK")

#t.show()

#sc = SparkContext()
#df_spark = SQLContext().createDataFrame(df).show()