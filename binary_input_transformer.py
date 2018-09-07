import cv2
from pyspark import keyword_only 
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
#import numpy as np
#from pyspark.sql.types import BooleanType
from pyspark.sql.types import ArrayType
from math import sqrt

class BinaryInputTransformer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(BinaryInputTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)
        
    def get_binary_input(self, image, threshold = 1.5):
        binary_input = []
    
        average_luminance = 0
       
        for x in range(0, image.shape[0]):
            for y in range(0, image.shape[1]):
                color = image[x, y]
                average_luminance += self.get_luminance(color)
            
        average_luminance /= image.shape[0] * image.shape[1]
    
        for x in range(0, image.shape[0]):
            for y in range(0, image.shape[1]):
                color = image[x, y]
                luminance = self.get_luminance(color)
                binary_input.append(True) if (luminance >= threshold * average_luminance) else binary_input.append(False)

        return binary_input

    def calculate_histogram(self, image):
        histogram = [0]*256

        for x in range(0, image.shape[0]):
            for y in range(0, image.shape[1]):
                color = image[x, y]
                histogram[int(self.get_luminance(color))] += 1
        return histogram
      
    @staticmethod
    def calculate_mean(image):
        mean = 0
              
        for x in range(0, image.shape[0]):
            for y in range(0, image.shape[1]):
                mean += sum(image[x, y])/3

        mean /= image.shape[0] * image.shape[1]
              
        return mean
      
    @staticmethod
    def calculate_standard_deviation(histogram, mean):
        variance = 0

        for i in range(0, 256):
            variance += (histogram[i]-mean)**2

        return sqrt(variance/256)
      
    def get_sauvola_binarization(self, image, weight = 1):
        binary_input = []
          
        histogram = self.calculate_histogram(image)
        mean = self.calculate_mean(image)
        standard_deviation = self.calculate_standard_deviation(histogram, mean)
          
        threshold = mean + weight * (standard_deviation/128 - 1) + 1
          
        for x in range(0, image.shape[0]):
            for y in range(0, image.shape[1]):
                binary_input.append(True) if(sum(image[x, y])/3 > threshold) else binary_input.append(False)
                  
        return image

    @staticmethod
    def get_luminance(color):
          return 0.2126 * color[0] + 0.7152 * color[1] + 0.0722 * color[2]

    @staticmethod
    def get_canny_filter(image, lower_bound = 4, upper_bound = 8):
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)        
        smoothed_image = cv2.GaussianBlur(image, (7, 7), 0)        
        canny = cv2.Canny(smoothed_image, lower_bound, upper_bound)
 
        return canny
     
    def _transform(self, dataset, tipo_pre_processamento, lower_bound = 4, upper_bound=8):
        if(tipo_pre_processamento == "binary_input"):
            f = udf(self.get_binary_input, ArrayType())
            
        if(tipo_pre_processamento == "luminance"):
            f = udf(self.get_luminance, ArrayType())
            
        if(tipo_pre_processamento == "sauvola"):
            f = udf(self.get_sauvola_binarization, ArrayType())
        
        if(tipo_pre_processamento == "canny"):
            f = udf(self.get_canny_filter, ArrayType())
        
        return dataset.withColumn('pre', f(dataset['age']))
        