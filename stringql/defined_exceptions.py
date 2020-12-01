from stringql.errors_txt import bad_data_error, bad_query_error
from stringql.errors_txt import bad_kwarg_error
from stringql.errors_txt import wrong_number_of_placeholders_error


class BaseValidationError(ValueError):
    pass


class WrongModeArgument(BaseValidationError):
    pass


class WrongDataArgumentType(BaseValidationError):
    pass


class WrongNumberOfPlaceholders(BaseValidationError):
    pass


class QueryMissingElements(BaseValidationError):
    pass


class TooManyKwargs(BaseValidationError):
    pass


def validate_data_arg_type(data, mode):
    """
    func that runs checks on data argument.
    data can only be a list, tuple or dictionary.
    dictionaries can only be passed if inserting (mode = "w" or "wr").
    """

    if not isinstance(data, dict):
        if not isinstance(data, (list, tuple)):
            raise WrongDataArgumentType(
                f"\nYou passed a {type(data)} the \"data\" argument.\n"
                + bad_data_error)

    else:  # dictionaries are only for insertion.
        if mode not in ["w", "wr"]:
            raise WrongDataArgumentType(
                f"\nYou passed a {type(data)} the \"data\" argument while in\n"
                f"{mode} mode. You can only pass dictionaries when inserting."
                + bad_data_error)


def validate_mode_arg(mode):
    """
    func that runs checks on mode argument. Mode can only be:
    "r" for read, "w" for write and "wr" for write and read.
    """
    if mode not in ["r", "w", "wr"]:
        raise WrongModeArgument(f"\nYou passed {mode} to the \"mode\" "
                                f"argument.\n"
                                "\"mode\" must be: \"r\" for read, "
                                "\"w\" for write or \"wr\" for both.")


def validate_query_placeholders(data, query, kwargs):
    """
    func that runs checks on the do_query placeholders.
    """
    # placeholders hold a spot in the do_query for a datum
    # -> there must be the same num of placeholders as there are data items.

    if isinstance(data, (list, tuple)):
        placeholders_count = query.count("%s")
        data_count = len(data)
        if placeholders_count != data_count:
            raise WrongNumberOfPlaceholders(
                wrong_number_of_placeholders_error.format(
                    placeholders_count=placeholders_count,
                    data_count=data_count,
                    query=query,
                    data=data,
                    kwargs=kwargs
                ))
    # if inserting from a dict:
    # -> placeholders + fields must be in do_query.
    # -> placeholders + fields cannot be kwargs.
    if isinstance(data, dict):
        mandatory_in_query = bad_kwargs = ["placeholders", "fields"]

        if any(str_ not in query for str_ in mandatory_in_query):
            raise QueryMissingElements(bad_query_error)

        if any(kwarg in kwargs.keys() for kwarg in bad_kwargs):
            raise TooManyKwargs(bad_kwarg_error)
