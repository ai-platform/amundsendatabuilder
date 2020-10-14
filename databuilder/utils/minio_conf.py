from dataclasses import dataclass


@dataclass
class MinioConf(object):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


