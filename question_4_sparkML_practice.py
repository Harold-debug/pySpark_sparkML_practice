from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.context import SparkContext

spark = SparkSession.builder.appName("Testing Spark ML Example: Titanic Analysis").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# Dataset not found in blackboard so I downloaded
# Question 4a, function to load the Titanic dataset and return the DataFrame
def load_titanic_dataset(spark, filename):
    df = spark.read.csv(filename, header=True, inferSchema=True)
    return df

# Question 4c, function to convert columns to float if needed
def convert_columns_to_float(df):
    float_columns = ["Age", "Fare"]
    for column in float_columns:
        # Checking if column exists and can be cast to float
        if column in df.columns and df.schema[column].dataType != "float":
            df = df.withColumn(column, df[column].cast("float"))
    return df


# Question 4d, function to describe the dataset and save the result to a file
def describe_dataset(df):
    description = df.describe()
    description.write.option("header", "true").csv("titanic_dataset_description")



if __name__ == "__main__":
    # Question 4a
    titanic_df = load_titanic_dataset(spark, "working_files/titanic_train.csv")
    # Counting the number of rows in the DataFrame
    row_count = titanic_df.count()
    with open("titanic_row_count.txt", "w") as f:
        f.write(str(row_count))
        
    # Question 4c
    titanic_df.show(5)
    titanic_df = convert_columns_to_float(titanic_df)
    
    # Question 4d
    describe_dataset(titanic_df)