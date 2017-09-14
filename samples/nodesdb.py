#coding: utf-8
"""
make association between two db.
date: 2017-09-14
author: zhongzi
"""

import pdb
import json
from bsddb3 import db

print db.DB_VERSION_STRING


def get_secondary_key(key, data):
    _key = db.DB_DONOTINDEX
    try:
        _data = json.loads(data)
        _key = _data.get('uid').encode('utf-8')
    except e:
        pass
    return _key


class NodesDB(object):
    def __init__(self, filename="nodes", homedir='storage/data'):
        self.filename = filename + '.db'
        self.homedir = homedir
        self.env = db.DBEnv()
        self.envflags = 0
        self.dbflags = 0
        self.primary = None
        self.secondary = None
        self.txn = None

    def open(self):
        self.env.open(self.homedir,
                      db.DB_CREATE | db.DB_INIT_MPOOL | db.DB_INIT_TXN |
                      db.DB_INIT_LOCK | db.DB_THREAD | self.envflags)
        # self.txn = self.env.txn_begin()

        self.primary = db.DB(self.env)
        self.primary.open(self.filename, "primary", db.DB_BTREE,
                          db.DB_CREATE | db.DB_THREAD | self.dbflags,
                          txn=self.txn)

        self.secondary = db.DB(self.env)
        self.secondary.set_flags(db.DB_DUP)
        self.secondary.set_get_returns_none(2)
        self.secondary.open(self.filename, "secondary", db.DB_BTREE,
                            db.DB_CREATE | db.DB_THREAD | self.dbflags,
                            txn=self.txn)

        self.primary.associate(self.secondary, get_secondary_key,
                               db.DB_CREATE, txn=self.txn)

    def add_node(self, key, data):
        return self.primary.put(key, data, txn=self.txn)

    def get_node(self, key):
        return self.primary.get(key)

    def del_node(self, key):
        return self.primary.delete(key, txn=self.txn)

    def get_nodes_by_uid(self, uid, start=-1, limit=-1):
        return self.secondary.pget(uid, dlen=limit, doff=start)

    def del_nodes_by_uid(self, uid):
        pass

    def commit(self):
        return self.txn and self.txn.commit()

    def close(self):
        if self.secondary:
            self.secondary.close()
            self.primary = None
        if self.primary:
            self.primary.close()
            self.primary = None
        if self.env:
            self.env.close()
            self.env = None
        return


if __name__ == '__main__':
    import random
    import time
    nodesdb = NodesDB()
    nodesdb.open()

    count = 10
    timestamp = int(time.time())*100

    def create_kv():
        return (str(timestamp + random.randint(1, 100)),
                {'uid': str(random.randint(1, 3)), 'info': 'a little info'})

    datas = {}
    _key = None
    _val = None
    while count >= 0:
        key, val = create_kv()
        if key in datas:
            continue
        _key = key
        _uid = val.get('uid')
        datas[key] = val
        count -= 1
    print datas
    # add_node
    for k, v in datas.items():
        nodesdb.add_node(k, json.dumps(v))

    # get_node
    for k,  v in datas.items():
        print 'v:', v
        print nodesdb.get_node(k)

    # nodesdb.commit()
    # get_nodes_by_uid
    for k, v in datas.items():
        uid = v.get('uid')
        print nodesdb.get_nodes_by_uid(uid)
        print "#"*10

    # del_node
    nodesdb.del_node(_key)
    print nodesdb.get_nodes_by_uid(_uid)

    nodesdb.close()



