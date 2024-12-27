from pymongo import MongoClient
from clickhouse_driver import Client

from util.conf import get_conf

_conf = get_conf()

def get_mongo_client():
    return MongoClient(_conf["db"]["mongo"]["url"])

def get_ch_client(db):
    ch_url = _conf["db"]["ch"]["url"] + "/" + db
    return Client.from_url(ch_url)