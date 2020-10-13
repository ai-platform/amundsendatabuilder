from pyhocon import ConfigTree
from typing import Any
import boto3
from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_stats import TableColumnStats
from databuilder.utils.minio_spec import CSVFormat
from databuilder.utils.spark_driver_local import initSparkSession
from databuilder.utils.pyspark_stats import get_stats_by_column


class MinioStatsExtractor(Extractor):

    """
    An Extractor that extracts meta data from minio and stores them in amundsen.
    """
    # CONFIG KEYS
    ACCESS_KEY = 'myaccesskey'
    SECRET_KEY = 'mysecretkey'
    BUCKET_NAME = 'dev-raw-data'
    ENDPOINT_URL = 'http://10.142.20.66:9000/'

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

        self.s3 = boto3.client('s3',
                               endpoint_url=self.endpoint_url,
                               aws_access_key_id=self.access_key,
                               aws_secret_access_key=self.secret_key)

        self.spark_session = initSparkSession()

    def get_stats_iter(self, bucket):
        """Get a stats iter of stats dictionaries in an S3 bucket."""
        stat_objs = []
        resp = self.s3.list_objects_v2(Bucket=bucket)
        for obj in resp['Contents']:
            if obj['Key'].split('/')[-1] == 'data.csv':
                key = obj['Key']
                print("key: ", key)
                stat_objs.extend(self.get_file_stats(obj['Key']))
                break

        print("stat_objs: ", stat_objs)

        return iter(stat_objs)

    def get_file_stats(self, key):
        # df = CSVFormat.spark_load(
        #    self.spark_session, s3_path)
        stats = []
        local_path = '/Users/alexander.doria/workspace/data/sacramento-real-estate-transactions/data.csv'
        df = self.spark_session.read.csv(local_path, header=True, inferSchema=True, nullValue='-')
        print("columns: ", df.dtypes)
        for col in df.dtypes:
            stat_vals = get_stats_by_column(key, col[0], col[1], df, df.count())
            stats.extend(stat_vals)
        return stats

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self.get_stats_iter('dev-raw-data')
        try:
            stat_obj = next(self._extract_iter)

            stat = TableColumnStats(table_name=stat_obj['table_name'],
                                    col_name=stat_obj['column_name'],
                                    stat_name=stat_obj['stat_name'],
                                    stat_val='"' + stat_obj['stat_val'] + '"',
                                    start_epoch='123',
                                    end_epoch='123',
                                    db=self.db,
                                    cluster=self.bucket_name,
                                    schema=self.schema
                                    )

            print("stat: ", stat.__dict__)

            return stat

        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.minio.csv'
