import pyspark
from pyspark.sql.session import SparkSession


def initSparkSession() -> SparkSession:
    conf = pyspark.SparkConf().set('spark.driver.host', '127.0.0.1')
    conf.set("spark.jars", "local:///opt/spark/jars/aws-java-sdk-bundle-1.11.563.jar,local:///opt/spark/jars/hadoop-aws-3.2.0.jar")
    conf.set('spark.hadoop.fs.s3a.endpoint', 'http://10.142.20.66:9000')
    conf.set('spark.hadoop.fs.s3a.access.key', 'myaccesskey')
    conf.set('spark.hadoop.fs.s3a.secret.key', 'mysecretkey')
    conf.set('spark.hadoop.fs.s3a.fast.upload', True)
    conf.set('spark.hadoop.fs.s3a.path.style.access', True)
    conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    sc = pyspark.SparkContext(master='local', appName='dev', conf=conf)
    return SparkSession(sc)
