import sys
from psycopg2 import sql
from functools import reduce
from psycopg2 import __version__ as psycopg2_version

error_appendix = "https://www.postgresql.org/docs/10/errcodes-appendix.html"


def psycopg2_exception_enhanced(err):
    """
    Func to enhance the error happening in psycopg2 library.
    """

    # get details about the exception.
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occurred.
    line_num = traceback.tb_lineno
    return (f"\nYou are using psycopg2 version: {psycopg2_version}\n"
            f"psycopg2 ERROR: {err} on line number: {line_num}.\n"
            f"psycopg2 traceback: {traceback} -- type: {err_type}.\n"
            f"extensions.Diagnostics: {err.diag}\n"
            f"pgerror: {err.pgerror}\n"
            f"pgcode: {err.pgcode}\n"
            f"Go here: {error_appendix} for more info on this pgcode (if "
            f"any).")


# utils
def reduce_fns_on_dict(fns, dic):
    return reduce(lambda d, f: f(d), fns, dic)


def create_sql_id(dic, on_param):
    param = dic.get(on_param, None)
    if param is not None:
        # overwrite with sql id
        dic[on_param] = sql.Identifier(param)
        return dic


def create_sql_ids_from_list(dic, on_list_param):
    list_param = dic.get(on_list_param, None)
    if list_param is not None:
        # overwrite with sql id
        dic[on_list_param] = sql.SQL(', ').join(map(
            sql.Identifier, list_param))
        return dic

