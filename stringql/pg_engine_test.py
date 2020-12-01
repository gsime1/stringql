import unittest
import testing.postgresql
from psycopg2 import DatabaseError
from psycopg2 import sql
from pg_engine import MyDb, parameterize_query
from defined_exceptions import WrongDataArgumentType
from defined_exceptions import WrongModeArgument
from defined_exceptions import WrongNumberOfPlaceholders
from defined_exceptions import QueryMissingElements
from defined_exceptions import TooManyKwargs
from test_data import SCHEMA, TABLE
from test_data import TUPLE_TO_INSERT, DICT_TO_INSERT


class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        self.pgsql = testing.postgresql.Postgresql()
        self.params = self.pgsql.dsn()
        # print(self.params)
        self.engine = MyDb(**self.params)
        self.conn = self.engine.connect()
        create = sql.SQL('create table if not exists {table}('
                         'id serial, '
                         'num int, '
                         'data text)').format(table=sql.Identifier(TABLE))
        cur = self.conn.cursor()
        # we haven't tested MyDb.do_query() method yet,
        # so use native psycopg2 to drop the schema.
        cur.execute(create)
        cur.close()

    def tearDown(self):
        cur = self.conn.cursor()
        # we haven't tested MyDb.do_query() method yet,
        # so use native psycopg2 to drop the schema.
        drop = sql.SQL('drop table if exists {table}').format(
            table=sql.Identifier(TABLE))
        # print(drop.as_string(self.conn))
        cur.execute(drop)
        cur.close()
        self.pgsql.stop()

    # TEST CONNECTION
    def test_connect_with_dsn_kwargs(self):
        self.assertIsNotNone(self.conn)

    def test_connect_with_string(self):
        # test connection with libpq string
        libpq = (' '.join([k + '=' + str(v)
                           for k, v in self.params.items()])
                    .replace('database', 'dbname'))  # database deprecated
        # print(libpq)
        engine = MyDb(libpq_string=libpq)
        conn = engine.connect()
        self.assertIsNotNone(conn)
        conn.close()

    def test_default_to_public_schema(self):

        # test conn defaults to public schema w/out schema arg.
        with self.conn as cn:
            cur = cn.cursor()
            cur.execute('select current_schema()')
            schema = cur.fetchone()
            # print(schema_1)
            self.assertEqual(schema, ('public',))

    def test_connect_to_given_schema(self):

        # test connection to a specific schema
        conn_to_test_schema = self.engine.connect(schema=SCHEMA)
        with conn_to_test_schema as cn:
            cur = cn.cursor()
            cur.execute('select current_schema()')
            schema = cur.fetchone()
            # print(schema)
            self.assertEqual(schema, ('test_schema',))

    # TEST PARAMETERIZATION
    def test_no_parameterization_needed(self):

        select = 'select * from name_table'
        q = parameterize_query(select)
        self.assertEqual(q.as_string(self.conn), select)

    def test_parameterization_of_string(self):

        draft_select = 'select {col} from {table}'
        param_select = 'select "name" from "name_table"'
        q_str = parameterize_query(draft_select, col='name',
                                               table='name_table')

        self.assertEqual(q_str.as_string(self.conn), param_select)

    def test_parameterization_of_collection(self):

        draft_select = 'select {cols} from name_table'
        cols = ['name', 'surname']
        param_select = 'select "name", "surname" from name_table'
        q_coll = parameterize_query(draft_select,
                                                cols=cols)

        self.assertEqual(q_coll.as_string(self.conn), param_select)

    def test_mixed_parameterization(self):
        draft_select = 'select {cols} from {table}'
        cols = ['name', 'surname']
        table = 'name_table'
        param_select = 'select "name", "surname" from "name_table"'
        q_both = parameterize_query(draft_select,
                                                cols=cols, table=table)

        self.assertEqual(q_both.as_string(self.conn), param_select)

    def test_parameterization_from_dict(self):
        # (data is a dic to insert) -> parameterize its fields
        #                           -> create placeholders
        insert_dict = 'insert into {table} ({fields}) values ({placeholders})'
        param_insert_dict = ('insert into "test_table" ("num", "data") '
                             'values (%(num)s, %(data)s)')
        drop_keys = ['ignore_me', 'ignore_me_too!']
        q_dict = parameterize_query(insert_dict, DICT_TO_INSERT,
            table=TABLE, drop_keys=drop_keys)

        self.assertEqual(q_dict.as_string(self.conn), param_insert_dict)

    def test_wrong_parameters_for_dict_insertion(self):

        wrong_insert_from_dict = (
            'insert into {table} ({columns}) '  # MUST be "fields"
            'values ({data_values})')           # MUST be "placeholders"

        self.assertRaises(QueryMissingElements, parameterize_query,
                          wrong_insert_from_dict, DICT_TO_INSERT, table=TABLE)

    # # TEST EXECUTION

    # -- catch errors --
    def test_wrong_mode(self):

        select_star = 'select * from t'
        mode = 'wrong_mode'
        # catch wrong mode argument string
        self.assertRaises(WrongModeArgument, self.engine.do_query,
                          self.conn, mode, select_star)

    def test_wrong_data_type(self):
        select_from = 'select * from t where name = %s'
        mode = 'r'
        data = 'gianny'

        # catch wrong data argument type
        # data can only be a dict or a collection.
        self.assertRaises(WrongDataArgumentType, self.engine.do_query,
                          self.conn, mode, select_from, data)

    def test_wrong_mode_for_dict_data(self):
        # catch wrong data argument type
        # data can only be a dict when inserting into db.
        insert_dict = ('insert into table_name {fields} '
                       'values ({placeholders})')
        mode = 'r'

        self.assertRaises(WrongDataArgumentType, self.engine.do_query,
                          self.conn, mode, insert_dict, DICT_TO_INSERT)

    def test_wrong_placeholders_count(self):
        select_from = 'select * from t where name = %s and age=%s'
        mode = 'r'
        data = 'gianny',  # missing value for second placeholder

        # catch wrong number of placeholders
        self.assertRaises(WrongNumberOfPlaceholders, self.engine.do_query,
                          self.conn, mode, select_from, data)

    def test_wrong_insert_from_dict(self):
        # when inserting from a dict, placeholders and fields are
        # automatically generated.

        insert_from_dict = (
            'insert into {table} ({fields}) '
            'values ({placeholders})')
        mode = 'w'

        self.assertRaises(TooManyKwargs, self.engine.do_query,
                          self.conn, mode, insert_from_dict, DICT_TO_INSERT,
                          table=TABLE, fields=["num", "data"],
                          placeholders="%s %s")

    def test_wrong_sql_syntax(self):

        wrong_select = 'select * from t when name = %s and age=%s'
        mode = 'r'
        data = ('gianni', 34)
        # catch wrong sql syntax
        self.assertRaises(DatabaseError, self.engine.do_query,
                          self.conn, mode, wrong_select, data)

    # -- statements --
    #    create, insert, update, select, insert returning.
    def test_create_table(self):
        create_stmt = ('create table if not exists {table}( '
                       'id serial, '
                       'num int, '
                       'data text)')
        mode = 'w'
        self.engine.do_query(self.conn, mode, create_stmt,
                             table='name_table')

        # check that create stmt worked.
        exists_stmt = ("select exists (select from information_schema.tables" 
                       "               where table_schema = 'public' "
                       "               and table_name = 'name_table')")

        cur = self.engine.do_query(self.conn, 'r', exists_stmt)
        res = cur.fetchone()
        self.assertEqual(res, (True,))
        cur.close()

    def test_insert_from_tuple(self):
        insert_stmt = 'insert into {table} ({cols}) values (%s, %s)'
        mode = 'w'
        cols = ['num', 'data']
        self.engine.do_query(self.conn, mode, insert_stmt,
                             TUPLE_TO_INSERT, table=TABLE, cols=cols)

        # check that insert stmt worked.
        select_stmt = "select {col_1} from {table} where {col_2} = %s"
        data = 100,
        cur = self.engine.do_query(self.conn, 'r', select_stmt, data,
                                   col_1='data', table=TABLE, col_2='num')
        res = cur.fetchone()
        self.assertEqual(res, ('insert from tuple: OK',))
        cur.close()

    def test_insert_from_dict(self):

        insert_dict = ('insert into {table} ({fields}) '
                       'values ({placeholders})')
        mode = 'w'
        drop_keys = ['ignore_me', 'ignore_me_too!']
        self.engine.do_query(self.conn, mode, insert_dict, DICT_TO_INSERT,
                             table=TABLE, drop_keys=drop_keys)

        # check that insert stmt worked.
        select_stmt = "select {col_1} from {table} where {col_2} = %s"
        data = 101,
        cur = self.engine.do_query(self.conn, 'r', select_stmt, data,
                                   col_1='data', table=TABLE, col_2='num')
        res = cur.fetchone()
        self.assertEqual(res, ('insert from dictionary: OK',))
        cur.close()

    def test_update(self):

        # insert
        insert_stmt = 'insert into {table} ({cols}) values (%s, %s)'
        mode = 'w'
        cols = ['num', 'data']
        self.engine.do_query(self.conn, mode, insert_stmt,
                             TUPLE_TO_INSERT, table=TABLE, cols=cols)
        # update
        update_stmt = 'update {table} set {col_1} = %s where {col_2} = %s;'
        mode = 'w'
        data = ('updated data', 100)
        col_1 = 'data'
        col_2 = 'num'
        self.engine.do_query(self.conn, mode, update_stmt, data,
                             col_1=col_1, table=TABLE, col_2=col_2)

        # check update worked
        select_stmt = "select {col_1} from {table} where {col_2} = %s"
        data = 100,
        cur = self.engine.do_query(self.conn, 'r', select_stmt, data,
                                   col_1='data', table=TABLE, col_2='num')
        res = cur.fetchone()
        self.assertEqual(res, ('updated data',))
        cur.close()

    def test_insert_returning(self):
        insert_r_stmt = ('insert into {table} ({cols}) '
                         'values (%s, %s) returning {p_key}')
        mode = 'wr'
        data = (103, 'insert from tuple: OK')
        cols = ['num', 'data']
        p_key = ['id', 'num']
        cur = self.engine.do_query(self.conn, mode, insert_r_stmt, data,
                                   table=TABLE, cols=cols, p_key=p_key)
        res = cur.fetchone()
        self.assertEqual(res, (1, 103))


if __name__ == '__main__':
    unittest.main(verbosity=2)
