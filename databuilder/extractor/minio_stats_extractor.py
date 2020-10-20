import logging
from typing import Any, Iterator, List, Optional, cast

import boto3
from mypy_boto3_s3 import S3Client
from pyhocon import ConfigTree

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_stats import TableColumnStats
from databuilder.utils.minio_spec import MinioSpecUtils
from databuilder.utils.pyspark_stats import get_stats_by_column


class MinioStatsExtractor(Extractor):

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
        self._extract_iter: Optional[Iterator[TableColumnStats]] = None

        self.bucket_name = conf.get_string(MinioStatsExtractor.BUCKET_NAME)
        self.endpoint_url = conf.get_string(MinioStatsExtractor.ENDPOINT_URL)
        self.access_key = conf.get_string(MinioStatsExtractor.ACCESS_KEY)
        self.secret_key = conf.get_string(MinioStatsExtractor.SECRET_KEY)
        self.spark_session = conf.get(MinioStatsExtractor.SPARK_SESSION_KEY)

        self.s3 = boto3.resource('s3',
                                 endpoint_url=self.endpoint_url,
                                 aws_access_key_id=self.access_key,
                                 aws_secret_access_key=self.secret_key,
                                 region_name='us-east-1')

    def get_stats_iter(self) -> Iterator[TableColumnStats]:
        """Get a stats iter of column stats dictionaries."""
        stat_objs = []
        s3_client = cast(S3Client, self.s3.meta.client)
        keys = MinioSpecUtils.get_dataset_paths(s3_client,
                                                self.bucket_name)
        for key in keys:
            stat_objs.extend(self.get_file_stats(key))

        return iter(stat_objs)

    def get_file_stats(self, key: str) -> List[TableColumnStats]:
        stats = []

        dataset_name = MinioSpecUtils.path_to_dataset_name(key)
        if not dataset_name:
            raise ValueError("Invalid dataset path")
        basename, format = MinioSpecUtils.split_dataset(dataset_name)
        s3_path = f's3a://{self.bucket_name}/{key}'

        df = format.spark_load(self.spark_session, s3_path)
        count = df.count()

        for col in df.columns:
            logging.debug("get_file_stats, col: %s", col)
            stats.extend(get_stats_by_column(self.bucket_name, basename, col, df, count))

        return stats

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self.get_stats_iter()
        try:
            stat_obj = next(self._extract_iter)
            return stat_obj
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.minio.columnstats'
