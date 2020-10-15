import os
import socket
from typing import Tuple

import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from databuilder.utils.minio_conf import MinioConf


def initSparkSession(minio_conf: MinioConf,
                     k8s_hostname: str,
                     k8s_port: int = 6443) -> Tuple[SparkContext, SparkSession]:
    k8s_master = f'k8s://https://{k8s_hostname}:{k8s_port}'
    driver_ip = socket.gethostbyname(socket.gethostname())
    jars_dir = os.path.join(os.environ.get('SPARK_HOME', '/usr/local/spark'), 'jars')
    spark_image = 'rcpai/spark-python:3.0.1'

    conf = pyspark.SparkConf().setMaster(k8s_master)
    conf.set('spark.jars',
             f'local://{jars_dir}/aws-java-sdk-bundle-1.11.563.jar,local://{jars_dir}/hadoop-aws-3.2.0.jar')
    conf.set('spark.hadoop.fs.s3a.endpoint', minio_conf.endpoint)
    conf.set('spark.hadoop.fs.s3a.access.key', minio_conf.access_key)
    conf.set('spark.hadoop.fs.s3a.secret.key', minio_conf.secret_key)
    conf.set('spark.hadoop.fs.s3a.fast.upload', str(True))
    conf.set('spark.hadoop.fs.s3a.path.style.access', str(True))
    conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    conf.set('spark.executor.instances', '1')
    conf.set('spark.executor.memory', '4g')
    conf.set('spark.driver.memory', '2g')
    conf.set('spark.driver.hostname', driver_ip)
    conf.set('spark.driver.host', driver_ip)
    conf.set('spark.kubernetes.driver.container.image', spark_image)
    conf.set('spark.kubernetes.executor.container.image', spark_image)
    conf.set('spark.kubernetes.container.image.pullPolicy', 'IfNotPresent')
    conf.set('spark.kubernetes.authenticate.driver.serviceAccountName', 'spark')

    sc = pyspark.SparkContext(master=k8s_master, appName='amundsen-index-minio', conf=conf)
    return sc, SparkSession(sc)
