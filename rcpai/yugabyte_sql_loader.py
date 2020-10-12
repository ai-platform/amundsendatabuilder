"""
This is a script used to index a local Yugabyte SQL cluster
into Neo4j and Elasticsearch without using an Airflow DAG.
"""

import os
import textwrap

from pyhocon import ConfigFactory

from databuilder.extractor.yugabyte_sql_metadata_extractor import YugabyteSqlMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.rcpai.base_arg_parser import RCPArgParser
from databuilder.rcpai.base_data_loader import BaseDataLoader
from databuilder.task.task import DefaultTask


class YugabyteSQLLoader(BaseDataLoader):
    def create_extract_job(self, connection_string: str, *args, **kwargs) -> DefaultJob:
        where_clause_suffix = textwrap.dedent("""
            where table_schema = 'public'
        """)

        tmp_folder = os.path.join(self.base_dir, 'table_metadata')
        node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
        relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

        job_config = ConfigFactory.from_dict({
            'extractor.yugabyte_sql_metadata.{}'.format(YugabyteSqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY):
                where_clause_suffix,
            'extractor.yugabyte_sql_metadata.{}'.format(YugabyteSqlMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME):
                True,
            'extractor.yugabyte_sql_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
                connection_string,
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
                         task=DefaultTask(extractor=YugabyteSqlMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                         publisher=Neo4jCsvPublisher())
        return job


if __name__ == "__main__":
    parser = RCPArgParser(description='Index data in a Yugabyte SQL database')
    parser.add_argument('--port', '-p', type=int, dest='port', default=5433,
                        help='Port of the yugabyte SQL server')
    parser.add_argument('--database', '--db', '-d', type=str, dest='db', required=True,
                        help='DB in the yugabyte SQL server')
    parser.add_argument('--user', '-u', type=str, dest='user', default='yugabyte',
                        help='Username to log in to the yugabyte server as')
    parser.add_argument('--password', '-P', type=str, dest='password', required=True,
                        help='Password to log in to the yugabyte server with')

    es_client = parser.es_client()
    neo4j_conf = parser.neo4j_conf()
    args = parser.parse_args()
    connection_string = f'postgresql://{args.user}:{args.password}@{args.hostname}:{args.port}/{args.db}'

    loader = YugabyteSQLLoader(es_client=es_client, neo4j_conf=neo4j_conf)
    loader.load(connection_string)
