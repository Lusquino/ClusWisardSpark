from cluswisard import Cluswisard
from binary_input_transformer import BinaryInputTransformer
from filler_column_transformer import FillerColumnTransformer
from pyspark.ml.pipeline import Estimator
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import numpy as np

class CluswisardEstimator(Estimator):
    
    def __init__(self, numero_entradas_rams, tamanho_entrada, numero_discriminadores, score_minimo, intervalo_crescimento):
        self.cluswisard = Cluswisard(numero_entradas_rams, tamanho_entrada, numero_discriminadores, score_minimo, intervalo_crescimento)
    
    def treinar(self, dataset, coluna_entrada, coluna_label):
        entradas_dataset = np.array(dataset.select(coluna_entrada).collect())
        entradas = [v for sublist in entradas_dataset for v in sublist]
        
        labels_dataset = np.array(dataset.select(coluna_label).collect())
        labels = [v for sublist in labels_dataset for v in sublist]
        
        for entrada in entradas:
            #############ad-hoc###################
            ent = entrada[2:]
            entrada_true = []
            for e in ent:
                entrada_true.append(True if e == '1' else False)
            ######################################
            label = labels[entradas.index(entrada)]
            if(not(label == -1)):
                #print("PASSEI 1: " + str(label))
                self.cluswisard.treinar(entrada_true, label)
            #else:
                #print("PASSEI 2")
                #self.cluswisard.treinar_cluster(entrada_true)
    
    def treinar_cluster(self, entrada):
        self.cluswisard.treinar_cluster(entrada)
        
    def classificar(self, entrada):
        #############ad-hoc###################
        ent = entrada[2:] 
        entrada_true = []
        for e in ent:
            entrada_true.append(True if e == '1' else False)
        ###############################
            
        return self.cluswisard.classificar(entrada_true)
        
    def _fit(self, dataset, coluna):
        f = udf(self.classificar, IntegerType())
        
        return FillerColumnTransformer().setOutputCol("new").transform(dataset.withColumn('predicoes', f(dataset[coluna])))
        #return BinaryInputTransformer().setOutputCol("new").transform(dataset.withColumn('predicoes', f(dataset[coluna])))