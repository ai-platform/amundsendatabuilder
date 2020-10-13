import os

from pyhocon import ConfigFactory
from pyspark.sql.session import SparkSession

from databuilder.extractor.minio_stats_extractor import MinioStatsExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.rcpai.base_arg_parser import RCPArgParser
from databuilder.rcpai.base_data_loader import BaseDataLoader
from databuilder.task.task import DefaultTask
from databuilder.utils.minio_conf import MinioConf
from databuilder.utils.spark_driver import initSparkSession


class MinioStatsLoader(BaseDataLoader):
    def create_extract_job(self, minio_conf: MinioConf, session: SparkSession, *args, **kwargs) -> DefaultJob:
        tmp_folder = os.path.join(self.base_dir, 'table_metadata')
        node_files_folder = f'{tmp_folder}/nodes/'
        relationship_files_folder = f'{tmp_folder}/relationships/'

        job_config = ConfigFactory.from_dict({
            'extractor.minio.columnstats.{}'.format(MinioStatsExtractor.ACCESS_KEY):
                minio_conf.access_key,
            'extractor.minio.columnstats.{}'.format(MinioStatsExtractor.SECRET_KEY):
                minio_conf.secret_key,
            'extractor.minio.columnstats.{}'.format(MinioStatsExtractor.BUCKET_NAME):
                minio_conf.bucket,
            'extractor.minio.columnstats.{}'.format(MinioStatsExtractor.ENDPOINT_URL):
                minio_conf.endpoint,
            'extractor.minio.columnstats.{}'.format(MinioStatsExtractor.SPARK_SESSION_KEY):
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
                         task=DefaultTask(extractor=MinioStatsExtractor(), loader=FsNeo4jCSVLoader()),
                         publisher=Neo4jCsvPublisher())
        return job


if __name__ == "__main__":
    parser = RCPArgParser(description='Index data in a Minio bucket')
    parser.add_argument('--scheme', '-s', type=str, dest='scheme', default='http',
                        help='Scheme of the MinIO server')
    parser.add_argument('--port', '-p', type=int, dest='port', default=9000,
                        help='Port of the MinIO server')
    parser.add_argument('--accesskey', '-ak', type=str, dest='accesskey',
                        help='Access key for the MinIO server')
    parser.add_argument('--secretkey', '-sk', type=str, dest='secretkey',
                        help='Secret key for the MinIO server')
    parser.add_argument('--bucket', '-b', type=str, dest='bucket', default='data-raw-dev',
                        help='MinIO bucket from which to retrieve objects')

    es_client = parser.es_client()
    neo4j_conf = parser.neo4j_conf()

    args = parser.parse_args()
    minio_endpoint = f'{args.scheme}://{args.hostname}:{args.port}/'
    minio_conf = MinioConf(
        endpoint=minio_endpoint,
        access_key=args.accesskey,
        secret_key=args.secretkey,
        bucket=args.bucket
    )

    loader = MinioStatsLoader(es_client=es_client, neo4j_conf=neo4j_conf)
    sc, session = initSparkSession(minio_conf, args.hostname)
    loader.load(minio_conf, session)
    sc.stop()
