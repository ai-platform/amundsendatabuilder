from datetime import datetime
from typing import Any, Iterator, List

import boto3
from pyhocon import ConfigTree

from databuilder.extractor.base_extractor import Extractor
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
        self._extract_iter = None

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

    def get_stats_iter(self) -> Iterator[dict]:
        """Get a stats iter of column stats dictionaries."""
        stat_objs = []
        keys = self.get_dataset_keys()

        for key in keys:
            stat_objs.extend(self.get_file_stats(key))

        print("stat_objs: ", stat_objs)
        return iter(stat_objs)

    def get_dataset_keys(self) -> List[str]:
        """Get a list of keys in an S3 bucket."""
        dataset_paths = set()
        result = self.s3.meta.client.list_objects(Bucket=self.bucket_name, Delimiter='/',
                                                  Prefix='v0/')
        for o in result.get('CommonPrefixes', []):
            path = o.get('Prefix')
            if MinioSpecUtils.path_to_dataset_name(path) is None:
                continue
            dataset_paths.add(path)

        print("dataset paths: ", dataset_paths)

        return dataset_paths

    def get_file_stats(self, key: str) -> List[dict]:
        stats = []

        dataset_name = MinioSpecUtils.path_to_dataset_name(key)
        basename, format = MinioSpecUtils.split_dataset(dataset_name)
        s3_path = f's3a://{self.bucket_name}/{key}'

        df = format.spark_load(self.spark_session, s3_path)
        count = df.count()

        for col in df.columns:
            print("col: ", col)
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
