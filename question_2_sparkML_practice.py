import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.context import SparkContext


spark = SparkSession.builder.appName("Testing Spark ML Example").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# Question 2f, Function to write results to file
def write_results(output, result):
    if isinstance(output, str):
        with open(output, "w") as file:
            file.write(str(result))
    elif isinstance(output, (list, tuple)):
        output_path = output[0]
        output_format = output[1]
        if output_format == "csv":
            result.write.option("header", True).csv(output_path)
        elif output_format == "txt":
            result.write.text(output_path)


# Question 2a, Function to generate a random point and check if it's inside the unit circle
def is_point_inside_unit_circle(num):
    x = random.random()
    y = random.random()
    return (x ** 2 + y ** 2) < 1


# Question 2c Function to approximate Pi using Spark
def approximate_pi_spark(N):
    # Creating an RDD with N random values
    random_points = sc.parallelize(range(N))
    # I filter the RDD to keep points inside unit circle
    points_inside_circle = random_points.filter(is_point_inside_unit_circle)
    # Counting the number of points inside circle
    count_inside_circle = points_inside_circle.count()
    # Approximating Pi
    pi_approximation = 4 * count_inside_circle / N
    return pi_approximation

# Function for the entire process
def compute_pi_with_spark(N, output):
    pi_approximation = approximate_pi_spark(N)
    write_results(output, pi_approximation)


if __name__ == "__main__":
    # Question 2b Just to test question 2a
    for i in range(4):
        val = i * 2 
        print("Iteration:", i)
        print("Point inside unit circle:", is_point_inside_unit_circle(val))
    
    # Testing the approximation of Pi
    pi_1000 = approximate_pi_spark(1000)
    pi_50000 = approximate_pi_spark(50000)
    pi_800000 = approximate_pi_spark(800000)
    print("Approximation of Pi with N=1000:", pi_1000)
    print("Approximation of Pi with N=50000:", pi_50000)
    print("Approximation of Pi with N=800000:", pi_800000)


    # Writing the results to files
    compute_pi_with_spark(1000, "pi_approximation_1000.txt")
    compute_pi_with_spark(50000, ("pi_approximation_50000", "csv"))
    compute_pi_with_spark(800000, ("pi_approximation_800000", "txt"))