from py2neo import Graph, Node, Relationship
from passlib.hash import bcrypt
from datetime import datetime
import os
import uuid
import pymssql

url = os.environ.get('GRAPHENEDB_URL', 'http://localhost:7474')
username = os.environ.get('NEO4J_USERNAME')
password = os.environ.get('NEO4J_PASSWORD')

graph = Graph(url + '/db/data/', username=username, password=password)

class StoredProcedure:
    def create_or_find(self, sp_name):
        sp = graph.find_one('StoredProcedure', 'name', sp_name)
        if sp:
            return sp
        else:
            return Node('StoredProcedure', name=sp_name)

    def usetable_rel(self, sp1, sp2):
        sp1_node = self.create_or_find(sp1)
        sp2_node = self.create_or_find(sp2)
        rel = Relationship(sp1_node, 'USE', sp2_node)
        graph.create(rel)

    def call_rel(self, sp1, sp2):
        sp1_node = self.create_or_find(sp1)
        sp2_node = self.create_or_find(sp2)
        rel = Relationship(sp1_node, 'CALL', sp2_node)
        graph.create(rel)


class MsSqlHandler:
    def __init__(self, argv):
        self.conn = pymssql.connect(server='betaruby.woowahan.com',
                                    user=argv[0],
                                    password=argv[1],
                                    port=6436)

        self.graph = StoredProcedure()
        self.total_nums = 0
        self.hashes = {}

    def get_sp_list(self):
        query = "select specific_name from information_schema.routines where routine_type='PROCEDURE' and specific_name not like 'sp_%';"

        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
        # return cursor.fetchmany(10)

    def store_deps(self, sp_name):
        cursor = self.conn.cursor()
        cursor.callproc('sp_depends', (sp_name,))
        hashes = {}
        for row in cursor:
            sym_name = row[0]
            try:
                hashes[sym_name]
                continue
            except:
                hashes[sym_name] = sym_name

            print "'%s' is using '%s' %s" % (sp_name, sym_name, self.total_nums)
            self.graph.usetable_rel(sp_name, sym_name)
            self.total_nums += 1


if __name__ == '__main__':

    import sys
    db = MsSqlHandler(sys.argv[1:])

    for sp in db.get_sp_list():
        db.store_deps(sp[0])
