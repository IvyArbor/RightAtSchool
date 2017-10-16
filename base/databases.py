import pymysql
import sys

class DB(object):
    connections = {}

    @staticmethod
    def configure(conf):
        __class__.connections = conf['mysql']

    @staticmethod
    def connect(name):
        if name not in __class__.connections:
            raise Exception("There is no connection named:", name, "in the settings.")

        try:
            if 'conn' not in __class__.connections[name]:
                params = __class__.connections[name]
                connection = pymysql.connect(**params)
                __class__.connections[name]['conn'] = connection
            return __class__.connections[name]['conn']
        except FileNotFoundError as e:
            raise Exception("Could not find certificate file: {}".format(params['ssl']['ca']))
        except:
            raise Exception("Could not connect to database:".format(params))

    @staticmethod
    def close(name):
        if name not in __class__.connections:
            raise Exception("There is no connection named:", name, "in the settings.")

        if 'conn' in __class__.open_connections[name]:
            __class__.open_connections[name]['conn'].close()
            del __class__.open_connections[name]['conn']
