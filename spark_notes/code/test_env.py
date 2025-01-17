import os
import pyspark
from pyspark.sql import SparkSession

os.environ['SPARK_HOME']='C:/MyData/Installs/Sparksetup/spark'
os.environ['HADOOP_HOME']='C:/MyData/Installs/Sparksetup/hadoop'

java_home=os.getenv('JAVA_HOME')
if java_home is None:
    print("Java not set")
else:
    print(f"Java-Home is set to {java_home} ")

try:
    spark=SparkSession.builder.appName('SparkByEample').getOrCreate()
    print("SparkSession")
    df=spark.read.text("C:/MyData/Repositories/Prep_work/useful_notes/spark_notes/tips1.md")
    print(df.take(10))
    sc = spark.sparkContext
    textData = sc.parallelize(["Yamaha is known for its agility", "Ducati excels in raw power", "Both brands have their unique strengths"])
    counts = textData.flatMap(lambda line: line.split(" ")) \
                 .map(lambda word: (word, 1)) \
                 .reduceByKey(lambda a, b: a + b)
    counts.collect().foreach(print)  # Printing the word counts directly
    spark.stop()
except Exception as e:
    print(f"Error creating SparkSession: {e}")