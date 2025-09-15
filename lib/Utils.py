from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_conf


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .config('spark.sql.autoBroadcastJoinThreshold',-1) \
            .config('spark.sql.adaptive.enabled','false') \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .config('spark.python.worker.faulthandler.enabled', 'true') \
            .config('spark.sql.execution.pyspark.udf.faulthandler.enabled', 'true')\
            .master("local[3]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .enableHiveSupport() \
            .getOrCreate()
