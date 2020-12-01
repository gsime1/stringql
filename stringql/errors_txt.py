# error_text.py

wrong_number_of_placeholders_error = (
    "\nYou seem to have a different number "
    "of placeholders {{placeholders_count}} "
    "and data {{data_count}}.\n"
    "Here's the do_query you passed {{do_query}}\n"
    "Here's the data you passed {{data}}\n"
    "Here's the kwargs you passed {{kwargs}}\n")


drop_keys_example = (
    "\nIf you want to drop some keys from the dictionary just "
    "add an additional optional kwarg to your execute call and \n"
    "name it \"drop_keys\" and pass it a list of the keys you "
    "don't want to be parameterized.\n"
    "example:\n\n"
    "dict_to_insert = {'num' : 101,\n"
    "                  'data': 'bla',\n"
    "                  'ignore_me': [],\n"
    "                  'ignore_me_too!': {'hey': foo}\n"

    "engine.execute(do_query='''INSERT INTO {table} ({fields}) \n"
    "                        VALUES ({placeholders})''',\n"
    "               mode='write',\n"
    "               table=table,\n"
    "               data=dict_to_insert,\n"
    "               drop_keys=['ignore_me', 'ignore_me_too!'])\n")

bad_kwarg_error = (
    "\nWhen passing data from a dictionary, you do not pass the"
    " \"fields\" and \"placeholders\" arguments. \n"
    "They are implicitly taken from the dictionary.\n "
    + drop_keys_example)

bad_query_error = (
    "\nWhen passing data from a dictionary, the do_query must "
    "contain the {placeholders} and the {fields} \n"
    "to know where the keys and the values should be inserted.\n"
    + drop_keys_example)

bad_data_error = (
    "\n\"data\" must always be passed as either a tuple or a dictionary. \n"
    "If you want to pass a single value - str or digit - put it in a tuple.\n"
    "Examples:\n"
    "data=(1,) # don't forget the comma!\n"
    "data=('string',)")
