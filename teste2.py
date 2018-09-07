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

from filler_column_transformer import FillerColumnTransformer

import time

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


import h5py
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


df_spark = spark.createDataFrame(df)
df_spark.show()

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


#########################################################
#########################################################
#########################################################

#from cluswisard_estimator import CluswisardEstimator

#clus = CluswisardEstimator(2, 48, 4, 10, 0.1)
#clus.treinar(t, "input", "label")

#classificacoes = clus._fit(t, "input")

#classificacoes.show()

# Import `DenseVector`
from pyspark.ml.linalg import DenseVector

# Define the `input_data` 
input_data = treino.rdd.map(lambda x: (x[0], DenseVector(x[1:])))

# Replace `df` with the new DataFrame
df = spark.createDataFrame(input_data, ["label", "features"])

# Import `StandardScaler` 
from pyspark.ml.feature import StandardScaler

# Initialize the `standardScaler`
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")

# Fit the DataFrame to the scaler
scaler = standardScaler.fit(df)

# Transform the data in `df` with the scaler
scaled_df_treino = scaler.transform(df)


# Define the `input_data` 
input_data = teste.rdd.map(lambda x: (x[0], DenseVector(x[1:])))

# Replace `df` with the new DataFrame
df = spark.createDataFrame(input_data, ["label", "features"])

# Import `StandardScaler` 
from pyspark.ml.feature import StandardScaler

# Initialize the `standardScaler`
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")

fim = time.time()

print("TEMPO###################")
print(fim-ini)

# Fit the DataFrame to the scaler

ini = time.time()

scaler = standardScaler.fit(df)

# Transform the data in `df` with the scaler
scaled_df_teste = scaler.transform(df)


# Inspect the result
#scaled_df.take(2)

from pyspark.ml.regression import LinearRegression

# Initialize `lr`
lr = LinearRegression(labelCol="label", maxIter=1)

# Fit the data to the model
linearModel = lr.fit(scaled_df_treino)
predicted = linearModel.transform(scaled_df_teste)

# Extract the predictions and the "known" correct labels
predictions = predicted.select("prediction").rdd.map(lambda x: x[0])
labels = predicted.select("label").rdd.map(lambda x: x[0])

# Zip `predictions` and `labels` into a list
predictionAndLabel = predictions.zip(labels).collect()

fim = time.time()

print("TEMPO###################")
print(fim-ini)


#print(predictionAndLabel)

acertos = 0
count = 0

for pl in predictionAndLabel:
    if(not(pl[1] == -1)):
        #print(pl)
        a = round(pl[0])
        b = round(pl[1])
        count += 1
        if((a>=b-2)and(a<=b+2)):
            acertos += 1
            
print(acertos)
print(count)

#classify = np.array(classificacoes.select("predicoes").collect())
#entradas_classify = [v for sublist in classify for v in sublist]

#gabarito = np.array(classificacoes.select("label").collect())
#entradas_gabarito = [v for sublist in gabarito for v in sublist]

'''matriz_confusao = [[0]*4 for i in range(0, 4)]
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
print(matriz_confusao)'''

#print(np.array(t.select("input").collect())[0])
#print(len(np.array(t.select("input").collect())[0][0]))

print("OK")

#t.show()

#sc = SparkContext()
#df_spark = SQLContext().createDataFrame(df).show()