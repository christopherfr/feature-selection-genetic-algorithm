import random
import string
from random import randint
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator

class ADN:
    def __init__(self, generador, fitness, reproduccion, mutacion, porcentaje_mutacion, ss):
        self.generador = generador
        self.fitness = fitness
        self.genes = ""
        self.fitness_result = 0
        self.reproduccion = reproduccion
        self.mutacion = mutacion
        self.porcentaje_mutacion = porcentaje_mutacion
        self.ss = ss

    def generar(self, longitud):
        self.genes = self.generador(longitud)

    def calcular_fitness(self):
        self.fitness_result = self.fitness(self.genes, self.ss)
        return self.fitness_result

    def reproducir(self, pareja):
        #funcion reproduccion
        genes_hijo = self.reproduccion(self, pareja)
        especie_hijo = ADN(self.generador, self.fitness, self.reproduccion, self.mutacion, self.porcentaje_mutacion, self.ss)
        especie_hijo.genes = genes_hijo
        return especie_hijo

    def mutar(self):
        if random.random() < self.porcentaje_mutacion:
            self.genes = self.mutacion(self.genes)

    def __str__(self):
        #return " ".join(self.genes)
        return str(self.genes)

class Poblacion:
    def __init__(self, cantidad, generador, fitness, f_reproductora, f_mutadora, porcentaje_mutacion, ss):
        self.cantidad = cantidad
        self.poblacion = []
        self.generador = generador
        self.fitness = fitness
        self.fitness_results = []
        self.fitness_total = 0
        self.lista_reproduccion = []
        self.f_reproductora = f_reproductora
        self.f_mutadora = f_mutadora
        self.porcentaje_mutacion = porcentaje_mutacion
        self.ss = ss

        for i in range(0,cantidad):
            especie = ADN(self.generador,self.fitness, self.f_reproductora, self.f_mutadora, self.porcentaje_mutacion, self.ss)
            especie.generar(31)
            self.poblacion.append(especie)
            fitness_especie = especie.calcular_fitness()
            self.fitness_total = self.fitness_total + fitness_especie
            self.fitness_results.append(fitness_especie)

    def seleccion(self): #Método 1
        self.lista_reproduccion = []
        # Ordena la poblacion y fitness_results en base a este último
        index = list(range(len(self.fitness_results)))
        index.sort(key = self.fitness_results.__getitem__)
        self.fitness_results[:] = [self.fitness_results[i] for i in index]
        self.poblacion[:] = [self.poblacion[i] for i in index]
        for i in range(0, len(self.poblacion)):
            for j in range(0, i+1):
                self.lista_reproduccion.append(self.poblacion[i])
				
    #def seleccion(self): #Método 2
    #    self.lista_reproduccion = []
    #    for i in range(0, len(self.poblacion)):
    #        porcentaje_especie = float(self.fitness_results[i]) / self.fitness_total
    #        n = int(porcentaje_especie * len(self.poblacion))
    #        for j in range(0, n):
    #            self.lista_reproduccion.append(self.poblacion[i])

    def reproduccion(self):
        self.poblacion = []
        self.fitness_results = []
        self.fitness_total = 0
        for i in range(0, self.cantidad):
            pareja_a = self.lista_reproduccion[random.randint(0, len(self.lista_reproduccion)-1)]
            pareja_b = self.lista_reproduccion[random.randint(0, len(self.lista_reproduccion)-1)]

            hijo = pareja_a.reproducir(pareja_b)
            hijo.mutar()
            self.poblacion.append(hijo)
            fitness_especie = hijo.calcular_fitness()
            self.fitness_total = self.fitness_total + fitness_especie
            self.fitness_results.append(fitness_especie)

    def promedio_fitness(self):
        return float(self.fitness_total) / len(self.fitness_results)

    def imprimir(self):
        for especie in self.poblacion:
            print("{} {}".format(especie, especie.calcular_fitness()))
Poblacion
###################################################################################################################

def generador(longitud):
    cromo = []
    for i in range(0, longitud):
        cromo.append(randint(0,1))
    return cromo

def fitness(cromo, ss):
    return regresion_logistica(cromo, ss)

def f_reproduccion(pareja1, pareja2):
    k = random.randint(0, len(pareja1.genes)-1 )
    parte_izq = pareja1.genes[0:k]
    parte_derecha = pareja2.genes[k:]
    return parte_izq + parte_derecha

def f_mutacion(genes):
    k = random.randint(0, len(genes)-1)
    genes[k] = int(not bool(genes[k]))
    return genes

def main():
    POBLACION = 20
    MAX_ITERACIONES = 100
    PORCENTAJE_MUTACION = 0.15
    poblacion = Poblacion(POBLACION, generador, fitness, f_reproduccion, f_mutacion, PORCENTAJE_MUTACION, s)

if __name__ == '__main__':
    main()

###################################################################################################################

def parseLinea(linea):
    sample = linea.split(",")
    if sample[6] != '?':
        return list(map(lambda x : int(x), sample))
#Buscar la primera posición preferida
def eb(cad):
    numero = cad.find("/")
    cortado = cad[0:numero]
    return cortado
#Buscar la segunda posición preferida (tenga 2 o 3 posiciones preferidas)
def eb1(cad):
    numero = cad.find("/") + 1
    fin = len(cad)
    cortado1 = cad[numero:fin]
    numero2 = cortado1.find("/")
    if numero2 == -1:
        cortado = cortado1
    else:
        cortado = cortado1[0:numero2]
    return cortado
#Buscar la tercera posición preferida
def eb2(cad):
        numero = cad.find("/") + 1
        fin = len(cad)
        cortado1 = cad[numero:fin]
        numero2 = cortado1.find("/") + 1
        fin2 = len(cortado1)
        if numero2 == -1:
            cortado = cortado1
        else:
            cortado = cortado1[numero2:fin2]
        return cortado
# Cargamos la data de FIFA17
# Se carga primero solo quitando a los arqueros y se quitan por ahora los que
# tiene más de un posición preferida, los cuales se tratarán después...
# A los que tiene solo una posición se clasifica con 1 y 0 según su posición
# es categoría 1[atacantes] los de posción ST, LF, CF, RF, LW, CAM, RW, LM y RM, y los demás son 0 [defensivos]
def load_dataset(spark,cromo):
    rdd = spark.sparkContext.textFile(
        "data/FullData.csv").map(lambda linea: linea.split(","))
    rdd_filter = rdd.filter(lambda linea: 'GK' not in linea[15])
    rdd_filter1 = rdd_filter.filter(lambda linea: '/' not in linea[15])
    rdd_data = rdd_filter1.map(lambda sample: [
        sample[0], 1 if sample[15] == 'ST' or sample[15] == 'LF' or sample[15] == 'CF'
        or sample[15] == 'RF' or sample[15] == 'LW' or sample[15] == 'CAM' or sample[15] == 'RW'
        or sample[15] == 'LM' or sample[15] == 'RM' else 0,
        int(sample[17]) * cromo[0],
        int(sample[18]) * cromo[1], int(sample[19]) * cromo[2], int(sample[20]) * cromo[3],
        int(sample[21]) * cromo[4], int(sample[22]) * cromo[5], int(sample[23]) * cromo[6],
        int(sample[24]) * cromo[7], int(sample[25]) * cromo[8], int(sample[26]) * cromo[9],
        int(sample[27]) * cromo[10], int(sample[28]) * cromo[11], int(sample[29]) * cromo[12],
        int(sample[30]) * cromo[13], int(sample[31]) * cromo[14], int(sample[32]) * cromo[15],
        int(sample[33]) * cromo[16], int(sample[34]) * cromo[17], int(sample[35]) * cromo[18],
        int(sample[36]) * cromo[19], int(sample[37]) * cromo[20], int(sample[38]) * cromo[21],
        int(sample[39]) * cromo[22], int(sample[40]) * cromo[23], int(sample[41]) * cromo[24],
        int(sample[42]) * cromo[25], int(sample[43]) * cromo[26], int(sample[44]) * cromo[27],
        int(sample[45]) * cromo[28], int(sample[46]) * cromo[29], int(sample[47]) * cromo[30]])

    # En esta parte empieza lo que se complicó, debido a que algunas tenían dos
    # posiciones preferidas que hizo una nueva carga en la cual solo se contaban ellos
    # después se procedio a ejecutar una función para obtener primera posición de las tres posibles
    rdd2 = spark.sparkContext.textFile(
        "data/FullData.csv").map(lambda linea: linea.split(","))
    rdd_filter2 = rdd2.filter(lambda linea: '/' in linea[15])
    rdd_data2 = rdd_filter2.map(lambda sample: [
        sample[0], 1 if eb(sample[15]) == 'ST' or eb(sample[15]) == 'LF' or eb(sample[15]) == 'CF'
        or eb(sample[15]) == 'RF' or eb(sample[15]) == 'LW' or eb(sample[15]) == 'CAM' or eb(sample[15]) == 'RW'
        or eb(sample[15]) == 'LM' or eb(sample[15]) == 'RM' else 0,
        int(sample[17]) * cromo[0],
        int(sample[18]) * cromo[1], int(sample[19]) * cromo[2], int(sample[20]) * cromo[3],
        int(sample[21]) * cromo[4], int(sample[22]) * cromo[5], int(sample[23]) * cromo[6],
        int(sample[24]) * cromo[7], int(sample[25]) * cromo[8], int(sample[26]) * cromo[9],
        int(sample[27]) * cromo[10], int(sample[28]) * cromo[11], int(sample[29]) * cromo[12],
        int(sample[30]) * cromo[13], int(sample[31]) * cromo[14], int(sample[32]) * cromo[15],
        int(sample[33]) * cromo[16], int(sample[34]) * cromo[17], int(sample[35]) * cromo[18],
        int(sample[36]) * cromo[19], int(sample[37]) * cromo[20], int(sample[38]) * cromo[21],
        int(sample[39]) * cromo[22], int(sample[40]) * cromo[23], int(sample[41]) * cromo[24],
        int(sample[42]) * cromo[25], int(sample[43]) * cromo[26], int(sample[44]) * cromo[27],
        int(sample[45]) * cromo[28], int(sample[46]) * cromo[29], int(sample[47]) * cromo[30]])

    #Se hizo lo mismo pero ahora solo cargamos los jugadores que tenian una segunda posición.
    rdd3 = spark.sparkContext.textFile(
        "data/FullData.csv").map(lambda linea: linea.split(","))
    rdd_filter3 = rdd3.filter(lambda linea: '/' in linea[15])
    rdd_data3 = rdd_filter3.map(lambda sample: [
        sample[0], 1 if eb1(sample[15]) == 'ST' or eb1(sample[15]) == 'LF' or eb1(sample[15]) == 'CF'
        or eb1(sample[15]) == 'RF' or eb1(sample[15]) == 'LW' or eb1(sample[15]) == 'CAM' or eb1(sample[15]) == 'RW'
        or eb1(sample[15]) == 'LM' or eb1(sample[15]) == 'RM' else 0,
        int(sample[17]) * cromo[0],
        int(sample[18]) * cromo[1], int(sample[19]) * cromo[2], int(sample[20]) * cromo[3],
        int(sample[21]) * cromo[4], int(sample[22]) * cromo[5], int(sample[23]) * cromo[6],
        int(sample[24]) * cromo[7], int(sample[25]) * cromo[8], int(sample[26]) * cromo[9],
        int(sample[27]) * cromo[10], int(sample[28]) * cromo[11], int(sample[29]) * cromo[12],
        int(sample[30]) * cromo[13], int(sample[31]) * cromo[14], int(sample[32]) * cromo[15],
        int(sample[33]) * cromo[16], int(sample[34]) * cromo[17], int(sample[35]) * cromo[18],
        int(sample[36]) * cromo[19], int(sample[37]) * cromo[20], int(sample[38]) * cromo[21],
        int(sample[39]) * cromo[22], int(sample[40]) * cromo[23], int(sample[41]) * cromo[24],
        int(sample[42]) * cromo[25], int(sample[43]) * cromo[26], int(sample[44]) * cromo[27],
        int(sample[45]) * cromo[28], int(sample[46]) * cromo[29], int(sample[47]) * cromo[30]])
    # Por último si existia alguna que más de tres posiciones preferidas, se filtraba
    # el rdd con número de caracteres en ese campo mayor a 7 pues lo máximo que podía tener
    # de caracteres un jugador con dos posiciones preferidas era 7 caracteres. Ejemplo -> [CAM/CDM]
    # y el minimo que tenía un jugador con tres posiciones preferidas es 8 caracteres. Ejemplo -> [RF/ST/LF]
    rdd4 = spark.sparkContext.textFile(
        "data/FullData.csv").map(lambda linea: linea.split(","))
    rdd_filter4 = rdd4.filter(lambda linea: len(linea[15]) > 7)
    rdd_data4 = rdd_filter4.map(lambda sample: [
        sample[0], 1 if eb2(sample[15]) == 'ST' or eb2(sample[15]) == 'LF' or eb2(sample[15]) == 'CF'
        or eb2(sample[15]) == 'RF' or eb2(sample[15]) == 'LW' or eb2(sample[15]) == 'CAM' or eb2(sample[15]) == 'RW'
        or eb2(sample[15]) == 'LM' or eb2(sample[15]) == 'RM' else 0,
        int(sample[17]) * cromo[0],
        int(sample[18]) * cromo[1], int(sample[19]) * cromo[2], int(sample[20]) * cromo[3],
        int(sample[21]) * cromo[4], int(sample[22]) * cromo[5], int(sample[23]) * cromo[6],
        int(sample[24]) * cromo[7], int(sample[25]) * cromo[8], int(sample[26]) * cromo[9],
        int(sample[27]) * cromo[10], int(sample[28]) * cromo[11], int(sample[29]) * cromo[12],
        int(sample[30]) * cromo[13], int(sample[31]) * cromo[14], int(sample[32]) * cromo[15],
        int(sample[33]) * cromo[16], int(sample[34]) * cromo[17], int(sample[35]) * cromo[18],
        int(sample[36]) * cromo[19], int(sample[37]) * cromo[20], int(sample[38]) * cromo[21],
        int(sample[39]) * cromo[22], int(sample[40]) * cromo[23], int(sample[41]) * cromo[24],
        int(sample[42]) * cromo[25], int(sample[43]) * cromo[26], int(sample[44]) * cromo[27],
        int(sample[45]) * cromo[28], int(sample[46]) * cromo[29], int(sample[47]) * cromo[30]])

    # Como se hicieron muchos rdd para obtener los DIFICILES datos, se agruparon con la función .union
    rdd_total_0 = rdd_data.union(rdd_data2)
    rdd_total_1 = rdd_total_0.union(rdd_data3)
    rdd_total = rdd_total_1.union(rdd_data4)
    # Se hace unpersist a los RDDs para liberar memoria RAM
    rdd_data.unpersist()
    rdd_data2.unpersist()
    rdd_data3.unpersist()
    rdd_data4.unpersist()
    rdd_total_0.unpersist()
    rdd_total_1.unpersist()

    # Se colocaron las cabeceras
    headers = ["Name", "CLASS", "Weak_foot", "Skill_Moves",
        "Ball_Control", "Dribbling", "Marking", "Sliding_Tackle", "Standing_Tackle", "Aggression",
        "Reactions","Attacking_Position","Interceptions","Vision","Composure","Crossing",
        "Short_Pass","Long_Pass"
        ,"Acceleration","Speed","Stamina","Strength","Balance","Agility","Jumping",
        "Heading","Shot_Power","Finishing","Long_Shots","Freekick_Accuracy","Penalties","Volleys"]

    # Se hizo el dataFrame.
    return spark.createDataFrame(rdd_total, headers)

# Se preparó la data que sería utilizada en modelo
def prepare_dataset(data):
    train, test = data.randomSplit(
        [0.7, 0.3], seed=12
    )
    #train.show()
    headers_feature = ["Weak_foot", "Skill_Moves",
        "Ball_Control", "Dribbling", "Marking", "Sliding_Tackle", "Standing_Tackle", "Aggression",
        "Reactions","Attacking_Position","Interceptions","Vision","Composure","Crossing",
        "Short_Pass","Long_Pass"
        ,"Acceleration","Speed","Stamina","Strength","Balance","Agility","Jumping",
        "Heading","Shot_Power","Finishing","Long_Shots","Freekick_Accuracy","Penalties","Volleys"]
    header_output = "features"

    assembler = VectorAssembler(
        inputCols=headers_feature,
        outputCol=header_output)
    train_data = assembler.transform(train).select("features", "CLASS")
    test_data = assembler.transform(test).select("features", "CLASS")

    return train_data,test_data

def regresion_logistica(cromo, ss):
    spark = ss
    data = load_dataset(spark,cromo)
    #data.show()
    train_data, test_data = prepare_dataset(data)
    #train_data.show()
    #print("Encontrando h...")

    # Se ejecuta el modelo de LogisticRegression
    lr = LogisticRegression(
        maxIter=100, regParam=0.3, elasticNetParam=0.8,
        labelCol='CLASS', family='binomial')

    lr_model = lr.fit(train_data)

    #print("Coeficientes: " + str(lr_model.coefficients))
    #print("Intercept: " + str(lr_model.intercept))

    #print("Test model...")

    data_to_validate = lr_model.transform(test_data)

    ## Se realizan las pruebas de ROC, PR y RMSE

    ##areaUnderROC
    #evaluator1 = BinaryClassificationEvaluator(
    #    labelCol='CLASS', metricName='areaUnderROC',
    #    rawPredictionCol='rawPrediction'
    #)
    #print("{}:{}".format(
    #    "areaUnderROC",evaluator1.evaluate(data_to_validate)))

    ##areaUnderPR
    evaluator2 = BinaryClassificationEvaluator(
        labelCol='CLASS', metricName='areaUnderPR',
        rawPredictionCol='rawPrediction'
    )
    areaUnderPR = evaluator2.evaluate(data_to_validate)
    #print("{}:{}".format(
    #    "areaUnderPR",areaUnderPR))

    ##rmse
    #evaluator3 = RegressionEvaluator(
    #    labelCol='CLASS', metricName='rmse',
    #    predictionCol="prediction"
    #)
    #print("{}:{}".format(
    #    "rmse",evaluator3.evaluate(data_to_validate)))
    
    # Se hace unpersist a los dataframes para liberar memoria RAM
    train_data.unpersist()
    test_data.unpersist()
    data_to_validate.unpersist()
    data.unpersist()
    return areaUnderPR
