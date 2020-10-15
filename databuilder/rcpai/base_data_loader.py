import os
import uuid
from typing import Any, Dict, Optional

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_es_last_updated_extractor import Neo4jEsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.rcpai.base_arg_parser import Neo4JConf


class BaseDataLoader(object):
    base_dir: str = '/var/tmp/amundsen'
    search_data_path: str = os.path.join(base_dir, 'search_data.json')
    updated_data_dir: str = os.path.join(base_dir, 'last_updated_data')

    def __init__(self, es_client: Elasticsearch, neo4j_conf: Neo4JConf):
        self.es_client = es_client
        self.neo4j_conf = neo4j_conf

    def load(self, *args: Any, **kwargs: Any) -> None:
        loading_job = self.create_extract_job(*args, **kwargs)
        loading_job.launch()

        self.create_last_updated_job().launch()

        job_es_table = self.create_es_publisher_sample_job(
            elasticsearch_index_alias='table_search_index',
            elasticsearch_doc_type_key='table',
            model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
        job_es_table.launch()

    def create_extract_job(self, *args: Any, **kwargs: Any) -> DefaultJob:
        raise NotImplementedError

    def create_es_publisher_sample_job(
        self,
        elasticsearch_index_alias: str = 'table_search_index',
        elasticsearch_doc_type_key: str = 'table',
        model_name: str = 'databuilder.models.table_elasticsearch_document.TableESDocument',
        cypher_query: Optional[str] = None,
        elasticsearch_mapping: Optional[Dict[str, Any]] = None
    ) -> DefaultJob:
        """
        :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                           amundsensearchlibrary/search_service/config.py as an
                                           index
        :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with.
                                           Defaults to `table` resulting in `table_search_index`
        :param model_name:                 the Databuilder model class used in transporting between
                                           Extractor and Loader
        :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if
                                           None is given (default) it uses the `Table` query baked
                                           into the Extractor
        :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the
                                           `ElasticsearchPublisher` class,
                                           if None is given (default) it uses the `Table` query
                                           baked into the Publisher
        """
        # loader saves data to this location and publisher reads it from here
        extracted_search_data_path = self.search_data_path

        task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                           extractor=Neo4jSearchDataExtractor(),
                           transformer=NoopTransformer())

        # elastic search client instance
        elasticsearch_client = self.es_client
        # unique name of new index in Elasticsearch
        elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())

        job_config = ConfigFactory.from_dict({
            'extractor.search_data.extractor.neo4j.{}'.format(
                Neo4jExtractor.GRAPH_URL_CONFIG_KEY): self.neo4j_conf.endpoint,
            'extractor.search_data.extractor.neo4j.{}'.format(
                Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
            'extractor.search_data.extractor.neo4j.{}'.format(
                Neo4jExtractor.NEO4J_AUTH_USER): self.neo4j_conf.user,
            'extractor.search_data.extractor.neo4j.{}'.format(
                Neo4jExtractor.NEO4J_AUTH_PW): self.neo4j_conf.password,
            'loader.filesystem.elasticsearch.{}'.format(
                FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY): extracted_search_data_path,
            'loader.filesystem.elasticsearch.{}'.format(
                FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
                extracted_search_data_path,
            'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
            'publisher.elasticsearch.{}'.format(
                ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
                elasticsearch_client,
            'publisher.elasticsearch.{}'.format(
                ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
                elasticsearch_new_index_key,
            'publisher.elasticsearch.{}'.format(
                ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
                elasticsearch_doc_type_key,
            'publisher.elasticsearch.{}'.format(
                ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
                elasticsearch_index_alias,
        })

        # only optionally add these keys, so need to dynamically `put` them
        if cypher_query:
            job_config.put(
                'extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY),
                cypher_query)
        if elasticsearch_mapping:
            job_config.put(
                'publisher.elasticsearch.{}'.format(
                    ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY
                ),
                elasticsearch_mapping)

        job = DefaultJob(conf=job_config,
                         task=task,
                         publisher=ElasticsearchPublisher())
        return job

    def create_last_updated_job(self) -> DefaultJob:
        # loader saves data to these folders and publisher reads it from here
        tmp_folder = self.updated_data_dir
        node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
        relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

        task = DefaultTask(extractor=Neo4jEsLastUpdatedExtractor(),
                           loader=FsNeo4jCSVLoader())

        job_config = ConfigFactory.from_dict({
            'extractor.neo4j_es_last_updated.model_class':
                'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated',

            'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
            'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
            'publisher.neo4j.node_files_directory': node_files_folder,
            'publisher.neo4j.relation_files_directory': relationship_files_folder,
            'publisher.neo4j.neo4j_endpoint': self.neo4j_conf.endpoint,
            'publisher.neo4j.neo4j_user': self.neo4j_conf.user,
            'publisher.neo4j.neo4j_password': self.neo4j_conf.password,
            'publisher.neo4j.neo4j_encrypted': self.neo4j_conf.encrypted,
            'publisher.neo4j.job_publish_tag': 'unique_lastupdated_tag',
            # should use unique tag here like {ds}
        })

        return DefaultJob(conf=job_config,
                          task=task,
                          publisher=Neo4jCsvPublisher())
