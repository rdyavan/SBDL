import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == 'LOCAL':
        return SparkSession.builder \
                .config('spark.driver.extraJavaOptions','-Dlog4j.configuration=file:log4j.properties') \
                .master('local[5]') \
                .enableHiveSupport() .getOrCreate()
    else:
        return SparkSession.builder \
                .enableHiveSupport() .getOrCreate()