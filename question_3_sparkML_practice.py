from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
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

# Question 3a
def read_movie_csv(file_path):
    movie_df = spark.read.option("header", True).csv('working_files/movies.csv')
    # Writing number of lines in the DataFrame to a text file
    num_lines = movie_df.count()
    write_dataframe_to_txt(spark.createDataFrame([(num_lines,)], ["num_lines"]), "num_lines.txt")
    return movie_df

# Question 3b
def count_movies_from_2012(movie_df):
    # Getting the number of movies from 2012
    num_movies_2012 = movie_df.filter(movie_df.year == 2012).count()
    write_dataframe_to_txt(spark.createDataFrame([(num_movies_2012,)], ["num_movies_2012"]), "num_movies_2012.txt")
    return num_movies_2012

# Question 3c
def select_profitable_movies(movie_df):
    # Selecting the title, budget, and revenue from profitable movies
    movie_df = movie_df.withColumn("budget", movie_df["budget"].cast("float"))
    movie_df = movie_df.withColumn("revenue", movie_df["revenue"].cast("float"))
    profitable_movies_df = movie_df.filter(movie_df.revenue > movie_df.budget).select("title", "budget", "revenue")
    write_dataframe_to_csv(profitable_movies_df, "profitable_movies.csv")
    return profitable_movies_df

# Question 3d
def find_movies_with_number_title(movie_df):
    # Finding movies with a number in their title
    movies_with_number_title_df = movie_df.filter(colRegex("title", "[0-9]"))
    write_dataframe_to_csv(movies_with_number_title_df, "movies_with_number_title.csv")
    return movies_with_number_title_df

# Question 3e
#def join_with_rating_table(movie_df, rating_table_df):
    # Doing a join operation with rating_table
#    joined_df = movie_df.join(rating_table_df, on="movie_id")
#    write_dataframe_to_csv(joined_df, "joined_df.csv")
#    return joined_df

# Question 3f
#def check_schema_and_cast(movie_df):
    # Checking schema of the DataFrame
#    movie_df.printSchema()
#    movie_df = movie_df.withColumn("budget", movie_df["budget"].cast("float"))
#    movie_df = movie_df.withColumn("revenue", movie_df["revenue"].cast("float"))
#    write_dataframe_to_csv(movie_df, "movie_df_casted.csv")
#    return movie_df

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
    #rating_table_df = spark.read.option("header", True).csv("rating_table.csv")
    #joined_df = join_with_rating_table(movie_df, rating_table_df)
    # Question 3f
    #checked_and_casted_movie_df = check_schema_and_cast(movie_df)
