from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length #, colRegex
from pyspark.context import SparkContext

spark = SparkSession.builder.appName("Testing Spark ML Example").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# Function to write DataFrame to a CSV file
def write_dataframe_to_csv(df, output_path):
    df.write.option("header", True).csv(output_path)

# Function to write DataFrame to a text file
def write_dataframe_to_txt(df, output_path):
    df.write.text(output_path)
    
# Function to write number of lines to text file
def write_num_lines_to_txt(num_lines, output_path):
    with open(output_path, "w") as file:
        file.write(str(num_lines))


# Question 3a
def read_movie_csv(file_path):
    movie_df = spark.read.option("header", True).csv('working_files/movies.csv')
    # Writing number of lines in the DataFrame to a text file
    num_lines = movie_df.count()
    write_num_lines_to_txt(num_lines, "num_lines.txt")
    return movie_df

# Question 3b
def count_movies_from_2012(movie_df):
    # Getting the number of movies from 2012
    num_movies_2012 = movie_df.filter(movie_df.release_date.contains('2012')).count()
    write_dataframe_to_txt_3b(spark.createDataFrame([(str(num_movies_2012),)], ["num_movies_2012"]), "num_movies_2012.txt")
    return num_movies_2012
# writing DataFrame to a text file for question 3b
def write_dataframe_to_txt_3b(df, output_path):
    df_string = df.withColumn("num_movies_2012", df["num_movies_2012"].cast("string"))
    df_string.write.text(output_path)


# Question 3c
def select_profitable_movies(movie_df):
    # Selecting the title, budget, and revenue from profitable movies
    movie_df = movie_df.withColumn("budget", movie_df["budget"].cast("float"))
    movie_df = movie_df.withColumn("revenue", movie_df["revenue"].cast("float"))
    profitable_movies_df = movie_df.filter(movie_df.revenue > movie_df.budget).select(
        movie_df["title"].alias("title"),
        movie_df["budget"].alias("profitable_budget"),
        movie_df["revenue"].alias("profitable_revenue"))
    write_dataframe_to_csv(profitable_movies_df, "profitable_movies.csv")
    return profitable_movies_df

# Question 3d
def find_movies_with_number_title(movie_df):
    # Finding movies with a number in their title
    #movies_with_number_title_df = movie_df.filter(colRegex("title", "[0-9]"))
    # colRegex isn't available in my version of pySpark so i used this:
    movies_with_number_title_df = movie_df.filter(movie_df.title.rlike("\\d"))
    write_dataframe_to_csv(movies_with_number_title_df, "movies_with_number_title.csv")
    return movies_with_number_title_df

# Question 3e
def join_with_profitable_movies(movie_df, profitable_movies_df):
    # using profitable_movies_df because i couldn't find rating_table.csv
    joined_df = movie_df.join(profitable_movies_df, on="title", how="inner")
    write_dataframe_to_csv(joined_df, "joined_with_profitable_movies.csv")
    return joined_df

# Question 3f
def check_schema_and_cast(joined_df):
    # Checking schema of the DataFrame
    joined_df.printSchema()
    joined_df = joined_df.withColumn("budget", joined_df["budget"].cast("float"))
    joined_df = joined_df.withColumn("revenue", joined_df["revenue"].cast("float"))
    write_dataframe_to_csv(joined_df, "movie_df_casted.csv")
    return joined_df

if __name__ == "__main__":
    # Question 3a
    movie_df = read_movie_csv("movie.csv")
    # Question 3b
    num_movies_2012 = count_movies_from_2012(movie_df)
    # Question 3c
    profitable_movies_df = select_profitable_movies(movie_df)
    # Question 3d
    movies_with_number_title_df = find_movies_with_number_title(movie_df)
    # Question 3e
    joined_df = join_with_profitable_movies(movie_df, profitable_movies_df)
    # Question 3f
    checked_and_casted_movie_df = check_schema_and_cast(joined_df)
