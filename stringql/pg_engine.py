# pg_engine.py

import psycopg2
from psycopg2 import sql
from psycopg2 import DatabaseError
from psycopg2.extensions import parse_dsn
from functools import partial
from stringql.pg_utils import psycopg2_exception_enhanced
from stringql.pg_utils import reduce_fns_on_dict
from stringql.pg_utils import create_sql_ids_from_list, create_sql_id
from stringql.defined_exceptions import validate_data_arg_type
from stringql.defined_exceptions import validate_mode_arg
from stringql.defined_exceptions import validate_query_placeholders

# todo recipes: insert_many, upsert, upsert_many.


class MyDb:
    """
    Class holding connection to a postgresql instance via the .connect()
    method and allowing for execution of queries automatically parameterised
    based on arguments passed to the .execute() method.
    Consutructor's parameters:

    - *libpq_string*: e.g. MydB("dbname=test user=postgres password=secret")
    - *kwargs*: MyDb(dbname="test", user="postgres", password="secret")

    **How to use**

    >>> import stringql
    >>> libpq_string = "dbname=test user=postgres password=secret"
    >>> schema = "my_test_schema"
    >>> q = "select name from {table} where {col} = %s"
    >>> where = ("smith",)
    >>> engine = stringql.start_engine(libpq_string=libpq_string)
    >>> conn = engine.connect(schema=schema)
    >>> curs = engine.do_query('r',q,where,table="ppl",col="fname")
    >>> for name in curs:  # when reading do_query returns iterable cursor
    >>>     print(name)  # prints tuples, like: ("john",) ...
    >>> curs.close()  # always close your cursor

    See here for dsn kwargs/libpq https://www.psycopg.org/docs/module.html
    """

    def __init__(self, libpq_string=None, **kwargs):

        self.libpq_string = libpq_string
        self.kwargs = kwargs

    def connect(self, schema=None):
        """
        Method which attempts connection to postresql instance using either
        a series of dsn kwargs or a libpq string. If schema is passed it
        will set path to that schema so all subsequent queries will be
        executed there.

        - *schema*: schema to connect to. If not provided, then public.

        It returns a Psycpog2 connection class. If it cannot connect it will 
        return None.

        See here for conn class https://www.psycopg.org/docs/connection.html
        """

        try:
            if self.libpq_string is not None:
                connection = psycopg2.connect(**parse_dsn(self.libpq_string))
            else:
                connection = psycopg2.connect(**self.kwargs)

        except DatabaseError as error:

            raise DatabaseError(psycopg2_exception_enhanced(error))

        if connection is not None and schema is not None:
            with connection:
                with connection.cursor() as cur:
                    schema = sql.Identifier(schema)
                    try:
                        cur.execute(sql.SQL(
                            "create schema if not exists {schema}; "
                            "set search_path to {schema}, public;").
                            format(schema=schema))

                    except Exception as error:
                        raise DatabaseError(psycopg2_exception_enhanced(error))

        return connection

    @staticmethod
    def do_query(conn, mode: str, query: str, data=None, **kwargs):
        """
        Func that executes query string after having parameterised it.

        - *conn*: connection object returned by .connect() method. 
        - *mode*: r (SELECT), w (INSERT), wr (INSERT RETURNING).
        - *query*: string query to be parameterised and executed.
        - *data*: collection or dictionary containing data for placeholders.
        - *kwargs*: kwarg to be parameterised and used to form query string.

        It returns a psycopg2 iterable cursor object if r or wr mode. 
        Otherwise None.

        See here for cursor object https://www.psycopg.org/docs/cursor.html
        """

        validate_mode_arg(mode)
        (data is not None and validate_data_arg_type(data, mode))

        if conn is not None:

            with conn:
                curs = conn.cursor()

                parameterized_query = parameterize_query(
                    data=data, query=query, **kwargs)

                if mode in ["r", "wr"]:
                    try:
                        curs.execute(parameterized_query, data)
                    except DatabaseError as error:
                        raise DatabaseError(
                            psycopg2_exception_enhanced(error))

                    return curs  # user to curs.close() after reading from it.

                else:
                    try:
                        curs.execute(parameterized_query, data)
                    except DatabaseError as error:
                        raise DatabaseError(
                            psycopg2_exception_enhanced(error))

                    # 'with' context manager should close or rollback,
                    # let's close anyways in case something went wrong.
                    finally:
                        curs.close()
        else:
            raise(ValueError, "The \"conn\" argument seems to be None.")


def parameterize_query(query, data=None, **kwargs):
    """
    It forms a composable representing a snippet of SQL statement.

    - *query*: query string.
    String doesn't undergo any form of escaping,
    so it is not suitable to represent variable identifiers or values.

    see https://www.psycopg.org/docs/sql.html#psycopg2.sql.SQL

    - *data*: collection or dictionary containing data to pass to placeholders.

    see https://www.psycopg.org/docs/usage.html

    - *kwargs*: the fields to pass to the string formatter.

    Returns parameterised string query, where needed, or original query.

    **How to use**

    >>> import stringql
    >>> from stringql.pg_engine import parameterize_query
    >>> libpq_string = "dbname=test user=postgres password=secret"
    >>> schema = "my_test_schema"
    >>> engine = stringql.start_engine(libpq_string=libpq_string)
    >>> conn = engine.connect(schema=schema)
    >>> q = "select {cols} from {tbl} where {fltr} = %s"
    >>> cols = ["name", "dob"]
    >>> tbl = "people"
    >>> fltr = "family_name"
    >>> paramed_q = parameterize_query(q, cols,
    ...              cols=cols, tbl=tbl, fltr=fltr
    >>> print(paramed_q.as_string(conn))
    >>> 'select "name", "dob" fom "people" where "family_name" = %s'
    """

    (data is not None and validate_query_placeholders(data, query, kwargs))

    sql_ids = {}
    kwargs_sql_ids = {}

    if data:

        if isinstance(data, dict):
            drop = list() if kwargs.get("drop_keys", None) is None \
                else kwargs["drop_keys"]

            fields = [k for k in data if k not in drop]

            sql_ids["fields"] = sql.SQL(', ').join(
                map(sql.Identifier, fields))
            sql_ids["placeholders"] = sql.SQL(', ').join(
                map(sql.Placeholder, fields))

    # if kwargs have been passed
    # turn every kwarg to its SQL identifier.
    # kwarg can be collections or strings.
    if kwargs:

        # TODO kwargs should only be string or list of strings.
        # kwargs being optional, we need to compose the
        # functions iteratively not declaratively (because we
        # just don't know before hand what the kwarg is).

        # here we'll store the curried function below to be
        # reduced on the kwargs dictionary.
        fns_to_reduce = []

        for k, v in kwargs.items():
            if isinstance(v, str):
                # if optional argument is a string, curry the
                # the create_sql_id func passing that as arg.
                fns_to_reduce.append(partial(create_sql_id, on_param=k))

            elif isinstance(v, (list, tuple)):
                # if optional argument is a collection,
                # curry the the create_sql_ids_from_list func
                # passing the list as arg.

                fns_to_reduce.append(
                    partial(create_sql_ids_from_list, on_list_param=k))

        # each func in fns_to_reduce will be applied to the
        # kwargs dictionary and the kwarg K:V will be
        # overwritten with the K:sql.Identifier(V) ready to
        # be passed to the execute() function after being
        # merged with the sql_id dictionary.

        kwargs_sql_ids = reduce_fns_on_dict(fns=fns_to_reduce, dic=kwargs)

        # merge the sql_ids dictionary.
        # sql_ids_merged = {**kwargs_sql_ids, **sql_ids}

    parameterized_query = sql.SQL(query).\
        format(**kwargs_sql_ids, **sql_ids)

    return parameterized_query
