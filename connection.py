import json
import pymongo

CONFIG_PREFIX = "connection"
MONGODB = "local"
# MONGODB = "remote"


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


@singleton
class MongoConnection:
    def __init__(self):
        config_file = "_".join([CONFIG_PREFIX, MONGODB]) + ".json"
        with open(config_file) as f:
            config = json.load(f)
            f.close()
        self._host = "mongodb://%s:%s@%s" % (config["user"], config["password"], config["host"])
        self._port = config["port"]
        self._client = pymongo.MongoClient(host=self._host, port=self._port)

    @property
    def client(self):
        return self._client

    @property
    def db(self):
        return self.client.get_database("model")

    @property
    def collection(self):
        return self.db.get_collection("strategy")
