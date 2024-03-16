from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length #, colRegex
from pyspark.context import SparkContext

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


spark = SparkSession.builder.appName("Testing Spark ML Example").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


# Load the cleaned Titanic dataset from the CSV file
cleaned_titanic_df = spark.read.csv("working_files/cleaned_titanic_dataset.csv/part-00000-0d579c62-d835-4324-8933-65a34e76990a-c000.csv", header=True, inferSchema=True)
