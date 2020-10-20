from databuilder.utils.minio_conf import MinioConf

from databuilder.rcpai.base_arg_parser import RCPArgParser


class MinioParser(RCPArgParser):
    def __init__(self, description: str):
        super(MinioParser, self).__init__(description)
        self.parser.add_argument('--scheme', '-s', type=str, dest='scheme', default='http',
                                 help='Scheme of the MinIO server')
        self.parser.add_argument('--port', '-p', type=int, dest='port', default=9000,
                                 help='Port of the MinIO server')
        self.parser.add_argument('--accesskey', '-ak', type=str, dest='accesskey',
                                 help='Access key for the MinIO server')
        self.parser.add_argument('--secretkey', '-sk', type=str, dest='secretkey',
                                 help='Secret key for the MinIO server')
        self.parser.add_argument('--bucket', '-b', type=str, dest='bucket', default='data-raw-dev',
                                 help='MinIO bucket from which to retrieve objects')

    def minio_conf(self) -> MinioConf:
        if self.args is None:
            self.args = self.parser.parse_args()
        minio_endpoint = f'{self.args.scheme}://{self.args.hostname}:{self.args.port}/'
        return MinioConf(
            endpoint=minio_endpoint,
            access_key=self.args.accesskey,
            secret_key=self.args.secretkey,
            bucket=self.args.bucket
        )
