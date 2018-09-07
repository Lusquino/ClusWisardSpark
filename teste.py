import binary_input_transformer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
#from pyspark import *
#import sparkdl
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import numpy as np
import cv2


sc = SparkContext()
sql_context = SQLContext(sc)

spark = SparkSession.builder \
      .master("local") \
      .appName("ImageClassification") \
      .config("spark.executor.memory", "6gb") \
      .getOrCreate()

'''
# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = sql_context.createDataFrame(departmentsWithEmployeesSeq1)

display(df1)
'''

#dado = Row("imagem", "label")
#rdd = sc.parallelize(l)
#images = rdd.map(lambda x: Row(id=x[0]))
#dataset = sql_context.createDataFrame([dado])

path = "/Users/leopoldolusquino/Documents/Doutorado/Tese/originais/"
transformador = binary_input_transformer.BinaryInputTransformer()
numPartitions = 10

rdd = sc.binaryFiles(path, minPartitions=numPartitions).repartition(numPartitions).take(10)

file_bytes = np.asarray(bytearray(rdd[0][1]), dtype=np.uint8)

print(file_bytes)

image = cv2.imdecode(file_bytes,1)

print(image)

#print(rdd.count())

rdd = sc.binaryFiles(path, minPartitions=numPartitions).select(input_file_name(), "label".rdd)

#print(image)

#df_images = pyspark.readImages(path).withColumn('label', f.lit(0))

df = sql_context.createDataFrame(rdd)

#df = rdd.map(lambda x: Row(**f(x))).toDF()

'''from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

schema = StructType([StructField(str(i), StringType(), True) for i in range(32)])

df = sql_context.createDataFrame(rdd, schema).withColumn('label', f.lit(0))

#print(df)
#print(rdd)

novo_dataset = transformador._transform(df, 5, 7)

print(novo_dataset)
'''


###############################
###########rdd#################
###############################

###############################
#########data_frame############
###############################

###############################
#########pipeline##############
###############################

###############################
#########transformer###########
###############################

###############################
#########estimator#############
###############################


