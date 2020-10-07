from pyhocon import ConfigTree
from typing import Any
import boto3
from databuilder.extractor.base_extractor import Extractor
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

        self.client = boto3.client('s3',
                          endpoint_url=self.endpoint_url,
                          aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key,
                          region_name='us-east-1')

    def get_data_csv_keys(self, bucket):
        """Get a list of keys in an S3 bucket."""
        keys = []
        resp = self.client.list_objects_v2(Bucket=bucket)
        for obj in resp['Contents']:
            if obj['Key'].split('/')[-1] == 'data.csv':
                keys.append(obj['Key'])
        return iter(keys)

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self.get_data_csv_keys('dev-raw-data')
        try:
            name = next(self._extract_iter)

            r = self.client.select_object_content(
                Bucket=self.bucket_name,
                Key=name,
                ExpressionType='SQL',
                Expression="select * from s3object limit 1",
                InputSerialization={
                    'CSV': {
                        "FileHeaderInfo": "None",
                    },
                },
                OutputSerialization={'CSV': {}},
            )


            for event in r['Payload']:
                if 'Records' in event:
                    columns = event['Records']['Payload'].decode("utf-8").partition('\n')[0].split(",")

            colMetadatalist = []
            for i in range(len(columns)):
                col = ColumnMetadata(name= columns[i],
                                     description= None,
                                     col_type= 'str',
                                     sort_order= i)
                colMetadatalist.append(col)

            table = TableMetadata(database='minio',
                                  cluster='dev-raw-data',
                                  schema='minio',
                                  name=name.split('/',1)[0],
                                  description='',
                                  columns=colMetadatalist,
                                  # TODO: this possibly should parse stringified booleans;
                                  # right now it only will be false for empty strings
                                  is_view=True,
                                  tags=['minio', 'raw']
                                  )
            return table
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.minio.csv'
