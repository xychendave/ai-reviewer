from urllib.parse import urlparse

from pymongo import MongoClient
from clickhouse_driver import Client
from mysql.connector import connection

from util.conf import get_conf

_conf = get_conf()

def get_mongo_client():
    return MongoClient(_conf["db"]["mongo"]["url"])

def get_ch_client(db):
    ch_url = _conf["db"]["ch"]["url"] + "/" + db
    return Client.from_url(ch_url)

def get_mysql_client(db):
    parsed_url = urlparse(_conf["db"]["mysql"]["url"])
    return connection.MySQLConnection(
        host=parsed_url.hostname,
        port=parsed_url.port,
        user=parsed_url.username,
        password=parsed_url.password,
        database=db,
    )
