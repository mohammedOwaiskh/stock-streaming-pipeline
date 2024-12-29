from pyspark import SparkConf
from utils.configutils import read_application_config


def get_spark_config() -> SparkConf:
    sparkConf = SparkConf()

    for k, v in read_application_config(section="SPARK_DEFAULTS"):
        sparkConf.set(k, v)
    return sparkConf
