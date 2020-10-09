from typing import Any, Iterator

import boto3
from pyhocon import ConfigTree
from pyspark.sql.session import SparkSession

from databuilder.extractor.base_extractor import Extractor
from databuilder.utils.minio_spec import MinioSpecUtils
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class MinioExtractor(Extractor):
    """
    An Extractor that extracts meta data from minio and stores them in amundsen.
    """
    # CONFIG KEYS
    ACCESS_KEY = 'access_key'
    SECRET_KEY = 'secret_key'
    BUCKET_NAME = 'bucket_name'
    ENDPOINT_URL = 'endpoint_url'
    SPARK_SESSION_KEY = 'spark_session'

    def init(self, conf: ConfigTree) -> None:
        """
        :param conf:
        """
        self.conf = conf
        self._extract_iter = None

        self.bucket_name = conf.get_string(MinioExtractor.BUCKET_NAME)
        self.endpoint_url = conf.get_string(MinioExtractor.ENDPOINT_URL)
        self.access_key = conf.get_string(MinioExtractor.ACCESS_KEY)
        self.secret_key = conf.get_string(MinioExtractor.SECRET_KEY)
        self.spark_session: SparkSession = conf.get(MinioExtractor.SPARK_SESSION_KEY)

        self.s3 = boto3.resource('s3',
                                 endpoint_url=self.endpoint_url,
                                 aws_access_key_id=self.access_key,
                                 aws_secret_access_key=self.secret_key,
                                 region_name='us-east-1')

    def get_dataset_iter(self) -> Iterator[str]:
        """Get a list of keys in an S3 bucket."""
        dataset_paths = set()
        result = self.s3.meta.client.list_objects(Bucket=self.bucket_name, Delimiter='/',
                                                  Prefix='v0/')
        for o in result.get('CommonPrefixes', []):
            path = o.get('Prefix')
            if MinioSpecUtils.path_to_dataset_name(path) is None:
                continue
            dataset_paths.add(path)
        return iter(dataset_paths)

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self.get_dataset_iter()
        try:
            dataset_path = next(self._extract_iter)
            return self.metadata_from_dataset_path(dataset_path)
        except StopIteration:
            return None

    def metadata_from_dataset_path(self, dataset_path) -> TableMetadata:
        dataset_name = MinioSpecUtils.path_to_dataset_name(dataset_path)
        basename, format = MinioSpecUtils.split_dataset(dataset_name)

        s3_path = f's3a://{self.bucket_name}/{dataset_path}'
        df = format.spark_load(self.spark_session, s3_path)

        schema = df.schema
        column_metadatas = []
        for i in range(len(schema)):
            col = schema[i]
            name = col.name
            data_type = col.dataType.typeName()
            metadata = ColumnMetadata(name=name,
                                      description=None,
                                      col_type=data_type,
                                      sort_order=i)
            column_metadatas.append(metadata)

        table = TableMetadata(database='minio',
                              cluster=self.bucket_name,
                              schema='minio',
                              name=basename,
                              description=None,
                              columns=column_metadatas,
                              tags=['minio', format.format]
                              )
        return table

    def get_scope(self) -> str:
        return 'extractor.minio'
