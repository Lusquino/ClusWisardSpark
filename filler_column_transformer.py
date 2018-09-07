from pyspark import keyword_only 
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

MAX_X1 = 663
MAX_Y1 = 1112
MAX_X2 = 743
MAX_Y2 = 1192
MEAN_X1 = 113
MEAN_Y1 = 196
MEAN_X2 = 134
MEAN_Y2 = 218
FAIXA_X1 = 200
FAIXA_Y1 = 200
FAIXA_X2 = 200
FAIXA_Y2 = 200

class FillerColumnTransformer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(FillerColumnTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)
        
    @staticmethod
    def get_binary_input_x1(value):
        if(value == -1):
            value = MEAN_X1
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_X1))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_y1(value):
        if(value == -1):
            value = MEAN_Y1
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_Y1))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_x2(value):
        if(value == -1):
            value = MEAN_X2
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_X2))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_y2(value):
        if(value == -1):
            value = MEAN_Y2
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_Y2))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_x1_thermometer(value):
        if(value == -1):
            value = MEAN_X1
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_X1/FAIXA_X1))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_y1_thermometer(value):
        if(value == -1):
            value = MEAN_Y1
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_Y1/FAIXA_Y1))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_x2_thermometer(value):
        if(value == -1):
            value = MEAN_X2
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_X2/FAIXA_X2))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_binary_input_y2_thermometer(value):
        if(value == -1):
            value = MEAN_Y2
            
        binary_input = bin(value)[2:]
        
        for i in range(len(binary_input), len(bin(MAX_Y2/FAIXA_Y2))):
            binary_input = "0" + binary_input
            
        return binary_input
        
    @staticmethod
    def get_concat_column(x1, y1, x2, y2):
        return x1 + y1 + x2 + y2
        
    @staticmethod
    def get_sum_column(x1, y1, x2, y2):
        return str(x1 + y1) + str(x2 + y2)
        
    def _transform(self, dataset):
        return dataset
     
    def _transform_x1(self, dataset):
        f = udf(self.get_binary_input_x1, StringType())
        
        return dataset.withColumn('integralX1', f(dataset['x1']))
    
    def _transform_y1(self, dataset):
        f = udf(self.get_binary_input_y1, StringType())
        
        return dataset.withColumn("integralY1", f(dataset["y1"]))
        
    def _transform_x2(self, dataset):
        f = udf(self.get_binary_input_x2, StringType())
        
        return dataset.withColumn("integralX2", f(dataset["x2"]))
        
    def _transform_y2(self, dataset):
        f = udf(self.get_binary_input_y2, StringType())
        
        return dataset.withColumn("integralY2", f(dataset["y2"]))
        
    def _transform_x1_thermometer(self, dataset):
        f = udf(self.get_binary_input_x1_thermometer, StringType())
        
        return dataset.withColumn('integralX1', f(dataset['x1']))
    
    def _transform_y1_thermometer(self, dataset):
        f = udf(self.get_binary_input_y1_thermometer, StringType())
        
        return dataset.withColumn("integralY1", f(dataset["y1"]))
        
    def _transform_x2_thermometer(self, dataset):
        f = udf(self.get_binary_input_x2_thermometer, StringType())
        
        return dataset.withColumn("integralX2", f(dataset["x2"]))
        
    def _transform_y2_thermometer(self, dataset):
        f = udf(self.get_binary_input_y2_thermometer, StringType())
        
        return dataset.withColumn("integralY2", f(dataset["y2"]))
    
    #concatena as entradas
    def _transform_2(self, dataset):
        f = udf(self.get_concat_column, StringType())
        
        return dataset.withColumn("features", f(dataset["integralX1"], dataset["integralY1"], dataset["integralX2"], dataset["integralY2"]))
        
    #concatena as entradas somando
    def _transform_3(self, dataset):
        f = udf(self.get_sum_column, StringType())
        
        return dataset.withColumn("input", f(dataset["x1"], dataset["y1"], dataset["x2"], dataset["y2"]))