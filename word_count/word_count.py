'''
@Author: Swapnil Bhoyar
@Date: 2021-08-16
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-08-16
@Title : Program to count the number of times a word is repeated
'''
import findspark
from pyspark.sql import SparkSession
from Log import logger
def word_count():
    """
    Description:
        Program to count the number of times a word is repeated
    """
    try:
        findspark.init()
        spark = SparkSession.builder.master("local").appName('Firstprogram').getOrCreate()
        sc=spark.sparkContext
        text_file = sc.textFile("input.txt")
        counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
        output = counts.collect()
        for (word, count) in output:
            print("%s: %i" % (word, count))
        sc.stop()
        spark.stop()
    except Exception as e:
        logger.info(e)


if __name__=="__main__":
    word_count()