import sys
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
