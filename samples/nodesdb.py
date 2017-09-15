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


def get_third_key(key, data):
    _key = db.DB_DONOTINDEX
    try:
        _data = json.loads(data)
        _key = _data.get('no').encode('utf-8')
    except e:
        pass
    return _key


def cmp_func(key1, key2):
    print 'cmp_func'
    if key1 > key2:
        return -1
    if key1 < key2:
        return 1
    return 0


def cmp_dup_func_secondary(key1, key2):
    print 'cmp_dup_func_secondary'
    if key1 > key2:
        return -1
    if key1 < key2:
        return 1
    return 0


def cmp_dup_func_third(key1, key2):
    print 'cmp_dup_func_third'
    if key1 > key2:
        return -1
    if key1 < key2:
        return 1
    return 0


class NodesDB(object):
    def __init__(self, filename="nodes", homedir='storage/data'):
        self.filename = filename + '.db'
        self.homedir = homedir
        self.env = db.DBEnv()
        self.envflags = 0
        self.dbflags = 0
        self.primary = None
        self.secondary = None
        self.thrid = None
        self.txn = None

    def open(self):
        self.env.open(self.homedir,
                      db.DB_CREATE | db.DB_INIT_MPOOL | db.DB_INIT_TXN |
                      db.DB_INIT_LOCK | db.DB_THREAD | self.envflags)
        # self.txn = self.env.txn_begin()

        self.primary = db.DB(self.env)
        self.primary.set_bt_compare(cmp_func)
        self.primary.open(self.filename, "primary", db.DB_BTREE,
                          db.DB_CREATE | db.DB_THREAD | self.dbflags,
                          txn=self.txn)

        self.secondary = db.DB(self.env)
        self.secondary.set_flags(db.DB_DUPSORT)
        self.secondary.set_dup_compare(cmp_dup_func_secondary)
        self.secondary.open(self.filename, "secondary", db.DB_BTREE,
                            db.DB_CREATE | db.DB_THREAD | self.dbflags,
                            txn=self.txn)

        self.primary.associate(self.secondary, get_secondary_key,
                               db.DB_CREATE, txn=self.txn)

        self.third = db.DB(self.env)
        self.third.set_flags(db.DB_DUPSORT)
        self.third.set_dup_compare(cmp_dup_func_third)
        self.third.open(self.filename, "third", db.DB_BTREE,
                        db.DB_CREATE | db.DB_THREAD | self.dbflags,
                        txn=self.txn)

        self.primary.associate(self.third, get_third_key,
                               db.DB_CREATE, txn=self.txn)

    def add_node(self, key, data):
        return self.primary.put(key, data, txn=self.txn)

    def get_node(self, key):
        return self.primary.get(key)

    def del_node(self, key):
        return self.primary.delete(key, txn=self.txn)

    def get_nodes_by_uid(self, uid, start=-1, limit=-1):
        return self.secondary.pget(uid, dlen=limit, doff=start)

    def get_nodes_by_no(self, no, start=-1, limit=-1):
        return self.third.pget(no, dlen=limit, doff=start)

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

    timestamp = int(time.time())*100

    def create_kv(count):
        id = str(timestamp + random.randint(1, 100))
        return (id,
                {'id': id,
                 'no': str(count),
                 'uid': str(random.randint(1, 3)),
                 'info': 'a little info'})

    datas = []
    _key = None
    _uid = None
    _no = None
    count = 0
    while count <= 20:
        key, val = create_kv(count)
        if key in datas:
            continue
        _key = key
        _uid = val.get('uid')
        _no = val.get('no')
        datas.append((key, val))
        count += 1
    # add_node
    for node in datas:
        nodesdb.add_node(node[0], json.dumps(node[1]))
    print datas
    print "#"*20

    # get_node
    for node in datas:
        print node[0],  node[1], nodesdb.get_node(node[0])
    print "#"*20

    # get_nodes_by_uid
    print nodesdb.get_nodes_by_uid(_uid)
    print "#"*20

    # get_nodes_by_no
    print nodesdb.get_nodes_by_no(_no)
    print "#"*20

    cursor_2 = nodesdb.secondary.cursor()
    cursor_3 = nodesdb.third.cursor()
    pdb.set_trace()

    # del_node
    nodesdb.del_node(_key)
    print nodesdb.get_nodes_by_uid(_uid)
    print "#"*20

    nodesdb.close()



