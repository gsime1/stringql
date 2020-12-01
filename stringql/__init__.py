# __init__.py

from stringql.pg_engine import MyDb

__version__ = "1.0.0"
__all__ = ["MyDb", "start_engine"]


def start_engine(libpq_string=None, **kwargs):
    """
    Opens a connection to a PostgreSQL database instance.

    - *conn_string* must be a valid libpq connection string.

    >>> import stringql
    >>> engine = stringql.start_engine(
    ... libpq_string="dbname=a_db user=a_user password=secret")
    >>> conn = engine.connect()
    >>> engine.do_query("w","CREATE IF NOT EXISTS {table} "
    ...
    ...                 table='names')
    """

    if libpq_string is not None:
        engine = MyDb(libpq_string=libpq_string)
    else:
        engine = MyDb(**kwargs)
    return engine
