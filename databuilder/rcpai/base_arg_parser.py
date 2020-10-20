import argparse
from dataclasses import dataclass
from typing import Any

from elasticsearch import Elasticsearch

DEV_HOST = "10.142.20.66"


@dataclass
class Neo4JConf(object):
    endpoint: str
    user: str
    password: str
    encrypted: bool = False


class RCPArgParser(object):
    def __init__(self, description: str):
        self.args: Any = None
        self.parser: argparse.ArgumentParser = RCPArgParser._arg_parser(description)

    @staticmethod
    def _arg_parser(description: str) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description=description)

        # Host Info
        parser.add_argument('--hostname', '-H', type=str, dest='hostname', default=DEV_HOST,
                            help='Hostname of the Minio server')

        # Elasticsearch Info
        parser.add_argument('--eshost', type=str, dest='es_host',
                            help='Hostname for elasticsearch server')
        parser.add_argument('--esuser', type=str, dest='es_user', default='elasticsearch',
                            help='Username to log in to the elasticsearch server as')
        parser.add_argument('--espassword', type=str, dest='es_password', default='elasticsearch',
                            help='Password to log in to the elasticsearch server with')

        # Neo4j Info
        parser.add_argument('--n4jhost', type=str, dest='n4j_host',
                            help='Hostname for Neo4j server')
        parser.add_argument('--n4juser', type=str, dest='n4j_user', default='neo4j',
                            help='Username to log in to the Neo4j server as')
        parser.add_argument('--n4jpassword', type=str, dest='n4j_password', default='test',
                            help='Password to log in to the Neo4j server with')
        return parser

    def add_argument(self, *args: Any, **kwargs: Any) -> None:
        if self.args is not None:
            raise RuntimeError('attempted to add args to an already parsed argparser')
        self.parser.add_argument(*args, **kwargs)

    def parse_args(self) -> Any:
        if self.args is None:
            self.args = self.parser.parse_args()
        return self.args

    def es_client(self) -> Elasticsearch:
        if self.args is None:
            self.args = self.parser.parse_args()
        args = self.args

        es_host = args.es_host if args.es_host is not None else args.hostname
        es = Elasticsearch([
            {'host': es_host},
        ])
        return es

    def neo4j_conf(self) -> Neo4JConf:
        if self.args is None:
            self.args = self.parser.parse_args()
        args = self.args
        n4j_host = args.n4j_host if args.n4j_host is not None else args.hostname
        neo4j_endpoint = 'bolt://{}:7687'.format(n4j_host)

        return Neo4JConf(endpoint=neo4j_endpoint, user=args.n4j_user, password=args.n4j_password)
