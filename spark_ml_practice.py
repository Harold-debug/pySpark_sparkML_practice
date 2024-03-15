from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.context import SparkContext


spark = SparkSession.builder.appName("Testing Spark ML Example").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


# Function to save results into a text file
def save_result_to_txt(result, filename):
    with open(filename + ".txt", "w") as file:
        file.write(str(result))


# Function to divide even digits by two and square odd digits
def process_number(num):
    if num % 2 == 0:
        return num / 2
    else:
        return num ** 2
    
    
# Question 1b
def process_list():
    # Here I create a list of 10 random elements
    random_list = sc.parallelize([i for i in range(10)])
    # Here, i divide even digits by two and square odd digits
    processed_list = random_list.map(process_number)
    # now saving the processed list to a file
    processed_list.saveAsTextFile("even_odd_question_1b")
    

# Question 1c
def load_movies():
    # Loading JSON file into DataFrame
    movies_df = spark.read.option("multiline", "true").json("working_files/movies-2020s.json")
    return movies_df