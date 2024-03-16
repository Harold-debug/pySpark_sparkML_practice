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
    
#########################################################################################################
###################################### Question 5 #######################################################
#########################################################################################################
#########################################################################################################

# Question 5a, function to count missing values in each column
def count_missing_values(df):
    missing_values_counts = {}
    for column in df.columns:
        missing_count = df.filter(col(column).isNull()).count()
        missing_values_counts[column] = missing_count
    return missing_values_counts



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
    
    
    #############################
    #######Question 5#########
    #############################
    # Question 5a, counting missing values for each column
    missing_values_counts = count_missing_values(titanic_df)
    # Writing the missing values counts to a text file
    with open("titanic_missing_values_counts.txt", "w") as f:
        for column, count in missing_values_counts.items():
            f.write(f"{column}: {count}\n")


    # Question 5b
    embarked_missing = missing_values_counts.get("Embarked", 0)
    age_missing = missing_values_counts.get("Age", 0)
    titanic_df.filter(col("Embarked").isNull()).show()
    titanic_df.filter(col("Age").isNull()).show()
    # Writing the counts to separate text files
    with open("titanic_embarked_missing_count.txt", "w") as f:
        f.write(str(embarked_missing))

    with open("titanic_age_missing_count.txt", "w") as f:
        f.write(str(age_missing))
        
    # Question 5c, examining unique values in Embarked and Age columns to identify missing value representations
    embarked_unique_values = titanic_df.select("Embarked").distinct().collect()
    age_unique_values = titanic_df.select("Age").distinct().collect()

    # Writing the unique values to text files
    with open("titanic_embarked_unique_values.txt", "w") as f:
        for val in embarked_unique_values:
            f.write(str(val) + "\n")

    with open("titanic_age_unique_values.txt", "w") as f:
        for val in age_unique_values:
            f.write(str(val) + "\n")
            
    # Replacing missing values with None
    #titanic_df = titanic_df.na.fill("NA", subset=["Embarked"])
    #titanic_df = titanic_df.na.fill({'Age': None}, subset=["Age"])
    
    # Question 5d, dropping rows with any null values
    titanic_df = titanic_df.dropna(how='any')

    # Writing the cleaned DataFrame to a CSV file
    titanic_df.write.option("header", "true").csv("cleaned_titanic_dataset")
    
    # Export the cleaned Titanic dataset to a CSV file
    # Repartition the DataFrame to a single partition
    cleaned_titanic_df = cleaned_titanic_df.repartition(1)
    titanic_df.write.csv("working_files/cleaned_titanic_dataset.csv", header=True)

