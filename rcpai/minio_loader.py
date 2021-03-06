import os

from pyhocon import ConfigFactory
from pyspark.sql.session import SparkSession

from databuilder.extractor.minio_extractor import MinioExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.rcpai.base_data_loader import BaseDataLoader
from databuilder.rcpai.minio_arg_parser import MinioParser
from databuilder.task.task import DefaultTask
from databuilder.utils import log
from databuilder.utils.minio_conf import MinioConf
from databuilder.utils.spark_driver import initSparkSession


class MinioLoader(BaseDataLoader):
    def create_extract_job(self, minio_conf: MinioConf, session: SparkSession, *args, **kwargs) -> DefaultJob:
        tmp_folder = os.path.join(self.base_dir, 'table_metadata')
        node_files_folder = f'{tmp_folder}/nodes/'
        relationship_files_folder = f'{tmp_folder}/relationships/'

        job_config = ConfigFactory.from_dict({
            'extractor.minio.{}'.format(MinioExtractor.ACCESS_KEY):
                minio_conf.access_key,
            'extractor.minio.{}'.format(MinioExtractor.SECRET_KEY):
                minio_conf.secret_key,
            'extractor.minio.{}'.format(MinioExtractor.BUCKET_NAME):
                minio_conf.bucket,
            'extractor.minio.{}'.format(MinioExtractor.ENDPOINT_URL):
                minio_conf.endpoint,
            'extractor.minio.{}'.format(MinioExtractor.SPARK_SESSION_KEY):
                session,
            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
                node_files_folder,
            'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
                relationship_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
                node_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
                relationship_files_folder,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
                self.neo4j_conf.endpoint,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
                self.neo4j_conf.user,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
                self.neo4j_conf.password,
            'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
                'unique_tag',  # should use unique tag here like {ds}
        })
        job = DefaultJob(conf=job_config,
                         task=DefaultTask(extractor=MinioExtractor(), loader=FsNeo4jCSVLoader()),
                         publisher=Neo4jCsvPublisher())
        return job


if __name__ == "__main__":
    log.init()
    parser = MinioParser(description='Index tables in a Minio bucket')

    es_client = parser.es_client()
    neo4j_conf = parser.neo4j_conf()
    minio_conf = parser.minio_conf()
    args = parser.parse_args()

    loader = MinioLoader(es_client=es_client, neo4j_conf=neo4j_conf)
    sc, session = initSparkSession(minio_conf=minio_conf,
                                   app_name='amundsen-index-minio',
                                   k8s_hostname=args.hostname)
    loader.load(minio_conf, session)
    sc.stop()
