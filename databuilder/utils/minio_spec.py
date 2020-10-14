import os
from typing import Optional, Tuple, Dict
from dataclasses import dataclass

from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame


@dataclass
class Format(object):
    format: str
    extension: str
    datafile: Optional[str]

    def spark_load(self, spark_session: SparkSession, s3_path: str) -> DataFrame:
        if self.datafile is not None:
            s3_path = f'{s3_path}/{self.datafile}'
        return spark_session.read.format(self.format).load(s3_path)


class CSVFormat(Format):
    def __init__(self):
        self.format = "csv"
        self.extension = ".csv"
        self.datafile = "data.csv"

    def spark_load(self, spark_session: SparkSession, s3_path: str) -> DataFrame:
        if self.datafile is not None:
            s3_path = f'{s3_path}/{self.datafile}'
        return spark_session.read\
            .format(self.format)\
            .option("header", True) \
            .option("inferSchema", True) \
            .option("nullValue", "-")\
            .load(s3_path)


csv = CSVFormat()
orc = Format("orc", ".orc", None)

v0: Dict[str, Format] = {
    csv.extension: csv,
    orc.extension: orc,
}


class MinioSpecUtils(object):
    @staticmethod
    def is_valid_dataset(name: str) -> bool:
        for k in v0.keys():
            if name.endswith(k):
                return True
        return False

    @staticmethod
    def path_to_dataset_name(path: str) -> Optional[str]:
        path = path.rstrip("/")
        try:
            dataset_name = os.path.split(path)[-1]
        except IndexError:
            return None
        if dataset_name.startswith(".") or not MinioSpecUtils.is_valid_dataset(dataset_name):
            return None
        return dataset_name

    @staticmethod
    def split_dataset(dataset_name: str) -> Tuple[Optional[str], Optional[Format]]:
        try:
            base, extension = os.path.splitext(dataset_name)
            return base, v0[extension]
        except Exception as e:
            print(e)
            return None, None
