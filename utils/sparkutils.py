import os
import sys


# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils.loggingutils import Log4j2
from utils.configutils import read_application_config


def get_spark_config() -> SparkConf:
    sparkConf = SparkConf()

    for k, v in read_application_config(section="SPARK_DEFAULTS"):
        sparkConf.set(k, v)
    return sparkConf


def get_spark_session() -> SparkSession:
    conf = get_spark_config()
    return SparkSession.builder.config(conf=conf).getOrCreate()
