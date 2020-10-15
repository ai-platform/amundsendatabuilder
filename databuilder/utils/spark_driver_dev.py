import pyspark
from pyspark.sql.session import SparkSession
import socket
import os


def initSparkSessionLocal() -> SparkSession:
    conf = pyspark.SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = pyspark.SparkContext(master='local', appName='dev', conf=conf)
    return SparkSession(sc)


def initSparkSessionServer() -> SparkSession:
    k8s_master = f'k8s://https://10.142.20.66:6443'
    driver_ip = socket.gethostbyname(socket.gethostname())
    jars_dir = os.path.join(os.environ.get('SPARK_HOME'), 'jars')
    spark_image = 'rcpai/spark-python:3.0.1'

    conf = pyspark.SparkConf().setMaster(k8s_master)
    conf.set('spark.jars',
             f'local://{jars_dir}/aws-java-sdk-bundle-1.11.563.jar,local://{jars_dir}/hadoop-aws-3.2.0.jar')
    conf.set('spark.hadoop.fs.s3a.endpoint', 'http://10.142.20.66:9000')
    conf.set('spark.hadoop.fs.s3a.access.key', 'myaccesskey')
    conf.set('spark.hadoop.fs.s3a.secret.key', 'mysecretkey')
    conf.set('spark.hadoop.fs.s3a.fast.upload', True)
    conf.set('spark.hadoop.fs.s3a.path.style.access', True)
    conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    conf.set('spark.executor.instances', 1)
    conf.set('spark.executor.memory', '4g')
    conf.set('spark.driver.memory', '2g')
    conf.set('spark.driver.hostname', driver_ip)
    conf.set('spark.driver.host', driver_ip)
    conf.set('spark.kubernetes.driver.container.image', spark_image)
    conf.set('spark.kubernetes.executor.container.image', spark_image)
    conf.set('spark.kubernetes.container.image.pullPolicy', 'IfNotPresent')
    conf.set('spark.kubernetes.authenticate.driver.serviceAccountName', 'spark')

    sc = pyspark.SparkContext(master=k8s_master, appName='amundsen-index-minio', conf=conf)
    return SparkSession(sc)
