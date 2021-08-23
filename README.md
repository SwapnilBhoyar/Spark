# Spark version: 3.1.2
# Loading data from Pyspark to mysql
# save dataframe to hdfs as csv
user_data.repartition(1).write.save('hdfs://localhost:9000/spark_data/record_counts',format='csv',mode='append')
# Read csv file from hdfs to pyspark dataframe
# Step 1:
# Create schema for dataframe
schema = StructType([\
   StructField("user_name", StringType(), True),\
   StructField("count", IntegerType(), True)])
# Step 2:
count_df = spark.read.format("csv").option("header", "false").schema(schema).load("hdfs://localhost:9000/spark_data/record_counts/part-00000-fe8be7ac-ddf8-4391-b673-f4b69bca54eb-c000.csv")
# Save pyspark dataframe to mysql as a table
count_df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark_db") \
   .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "user_counts") \
   .option("user", "root").option("password", "neo").mode('append').save()
# Load sql table as a dataframe in pyspark
Load sql table as a dataframe in pyspark
