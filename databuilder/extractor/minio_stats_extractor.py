from pyhocon import ConfigTree
from typing import Any, Iterator, List
import boto3
from databuilder.extractor.base_extractor import Extractor
from databuilder.utils.minio_spec import MinioSpecUtils
from databuilder.models.table_stats import TableColumnStats
from databuilder.utils.minio_spec import CSVFormat
from databuilder.utils.spark_driver_local import initSparkSession
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

        self.db = 'minio'
        self.schema = 'minio'

        self.s3 = boto3.resource('s3',
                                 endpoint_url=self.endpoint_url,
                                 aws_access_key_id=self.access_key,
                                 aws_secret_access_key=self.secret_key,
                                 region_name='us-east-1')
        # TODO: use spark session from the arg parser, on a Spark/Kubernetes deployment
        self.spark_session = initSparkSession()

    def get_stats_iter(self):
        """Get a stats iter of stats dictionaries in an S3 bucket."""
        stat_objs = []
        keys = self.get_dataset_keys()

        for key in keys:
            stat_objs.extend(self.get_file_stats(key))

        print("stat_objs: ", stat_objs)
        return iter(stat_objs)

    def get_dataset_keys(self) -> Iterator[str]:
        """Get a list of keys in an S3 bucket."""
        dataset_paths = set()
        result = self.s3.meta.client.list_objects(Bucket=self.bucket_name, Delimiter='/',
                                                  Prefix='v0/')
        for o in result.get('CommonPrefixes', []):
            path = o.get('Prefix')
            if MinioSpecUtils.path_to_dataset_name(path) is None:
                continue
            dataset_paths.add(path)
        return dataset_paths

    def get_file_stats(self, key):
        stats = []

        dataset_name = MinioSpecUtils.path_to_dataset_name(key)
        basename, format = MinioSpecUtils.split_dataset(dataset_name)
        s3_path = f's3a://{self.bucket_name}/{key}'

        print("dataset_name: ", dataset_name)
        print("basename: ", basename)
        print("format: ", format)
        print("s3_path: ", s3_path)

        df = format.spark_load(self.spark_session, s3_path)
        df.printSchema()

        for col in df.dtypes:
            stat_vals = get_stats_by_column(basename, col[0], col[1], df, df.count())
            stats.extend(stat_vals)
        return stats

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self.get_stats_iter()
        try:
            stat_obj = next(self._extract_iter)
            return self.column_stat_from_stat_obj(stat_obj)
        except StopIteration:
            return None

    def column_stat_from_stat_obj(self, stat_obj):
        stat = TableColumnStats(table_name=stat_obj['table_name'],
                                col_name=stat_obj['column_name'],
                                stat_name=stat_obj['stat_name'],
                                stat_val='"' + stat_obj['stat_val'] + '"',
                                # TODO: modify start/end epoch to reflect
                                # date stats were collected
                                start_epoch='123',
                                end_epoch='123',
                                db=self.db,
                                cluster=self.bucket_name,
                                schema=self.schema
                                )

        print("stat: ", stat.__dict__)

        return stat

    def get_scope(self) -> str:
        return 'extractor.minio.csv'
