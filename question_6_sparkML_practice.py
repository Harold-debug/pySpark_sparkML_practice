from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length #, colRegex
from pyspark.context import SparkContext

from pyspark.ml.feature import StringIndexer
# Question 6b, importing the VectorAssembler class from the pyspark.ml.feature
from pyspark.ml.feature import VectorAssembler
# Question 6f
from pyspark.ml.classification import RandomForestClassifier
# Question 6j
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# Question 6m
from pyspark.ml.stat import Correlation


spark = SparkSession.builder.appName("Testing Spark ML Example").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


# Load the cleaned Titanic dataset from the CSV file
cleaned_titanic_df = spark.read.csv("working_files/cleaned_titanic_dataset.csv/part-00000-73e64fe1-a793-41c6-9c4d-a4bff4ca33e1-c000.csv", header=True, inferSchema=True)


# Question 6a
required_features = ["Pclass", "Age", "SibSp", "Parch", "Fare", "Survived", "PassengerId"]

# Question 6c
assembler = VectorAssembler(inputCols=required_features, outputCol='features')
transformed_data = assembler.transform(cleaned_titanic_df)

# Question 6d) We can now train a model
assembler = VectorAssembler(inputCols=required_features, outputCol='features')


# Question 6e
(training_data, test_data) = transformed_data.randomSplit([0.7, 0.3])

# Question 6g
randomForest = RandomForestClassifier(labelCol='Survived', featuresCol='features', maxDepth=5)

# Question 6h, training the model
model = randomForest.fit(training_data)

# Question 6i, transforming test_data into predictions
predictions = model.transform(test_data)

# Question 6k
evaluator = MulticlassClassificationEvaluator(labelCol='Survived', predictionCol='prediction', metricName='accuracy')

# Question 6l, evaluating the model
accuracy = evaluator.evaluate(predictions)

# Question 6m (Correlation matrix of the DataFrame)
print("Accuracy:", accuracy)
# Write results to files
with open("question_6_results.txt", "w") as f:
    f.write(f"Accuracy: {accuracy}\n")
    
    
# Assemble all numeric columns into a single feature vector
assembler = VectorAssembler(inputCols=required_features, outputCol="features")
feature_vector = assembler.transform(cleaned_titanic_df)

# Compute the correlation matrix
correlation_matrix = Correlation.corr(feature_vector, "features").head()

print(correlation_matrix)

with open("correlation_matrix.txt", "w") as f:
    f.write(str(correlation_matrix))
    
    
# For movies dataset
movies_df = spark.read.csv("working_files/movies.csv", header=True, inferSchema=True)

# Casting relevant columns to the appropriate data types
movies_df = movies_df.withColumn("budget", col("budget").cast("double"))
movies_df = movies_df.withColumn("popularity", col("popularity").cast("double"))
movies_df = movies_df.withColumn("revenue", col("revenue").cast("double"))
movies_df = movies_df.withColumn("runtime", col("runtime").cast("double"))
movies_df = movies_df.withColumn("vote_average", col("vote_average").cast("double"))
movies_df = movies_df.withColumn("vote_count", col("vote_count").cast("int"))

# Handle null values by dropping rows containing nulls
movies_df = movies_df.dropna()

# Columns of the movies DataFrame
movies_columns = [ "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count"]

# Assemble all numeric columns into a single feature vector
movies_assembler = VectorAssembler(inputCols=movies_columns, outputCol="movie_features", handleInvalid="skip")
movies_feature_vector = movies_assembler.transform(movies_df)

# Compute the correlation matrix
movies_correlation_matrix = Correlation.corr(movies_feature_vector, "movie_features").head()

# Write the correlation matrix to a file
with open("movies_correlation_matrix.txt", "w") as f:
    f.write(str(movies_correlation_matrix))