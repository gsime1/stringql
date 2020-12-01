# stringql

Support for string queries parameterisation and execution, building on [`Psycopg2.sql`](https://www.psycopg.org/docs/sql.html) module. 

## How to to use
```
import stringql

schema = "a_schema"
libpq_string = "dbname=a_db user=a_user password=a_secret"
engine = stringql.start_engine(libpq_string=libpq_string)
conn = engine.connect(schema=schema)  # created if doesn't exist.

q = "select name from {table} where {filter_col} = %s"
filter_val = ("smith",)

curs = engine.do_query(conn, 
                       mode='r', 
                       query=q, 
                       data=filter_val, 
                       table="people", 
                       filter_col="family_name")

for name in curs:  # when reading do_query returns an iterable cursor object
    print(name)  # prints tuples, like: ("john",) ...
curs.close()  # always close your cursor
```

### the connect method
You can connect to the postgres instance by using the libpq string, or the dsn keyword arguments: 
- *libpq_string*: MydB("dbname=test user=postgres password=secret")
- *dsn kwargs*: MyDb(dbname="test", user="postgres", password="secret")
- *schema*: defaulted to None, in which case you'll connect to the public schema. Otherwise it'll create the schema and 
set `search_path` to it. 

### the parameterise_query method
The `parameterize_query()` function forms a [Composable object](https://www.psycopg.org/docs/sql.html#psycopg2.sql.Composable)
 representing a snippet of SQL statement.
It all happens behind the curtains when you call the `.do_query()` method. 

- *query*: query string. see [here](https://www.psycopg.org/docs/sql.html#psycopg2.sql.SQL) for basic rules on how to form 
strings for the Psycopg2's sql module. 
- *data*: defaulted to None, pass a dictionary to format an insert statement. 
- *kwargs*: the fields to pass to the string format method.

Returns parameterised string query, where needed, or original query.


Should you want to just parameterise your query and use Psycopg2 cusror's `.execute()` method,
do the following:

```
from stringql.pg_engine import parameterize_query

q = "select {cols} from {table} where {filter} = %s"
cols = ["name", "dob"]
table = "people"
filter = "family_name"
paramed_q = parameterize_query(query=q, cols=cols, table=table, filter=filter)

# You need the conn to reprint the Composed object as string
print(paramed_q.as_string(conn))  

# will print to console:
'select "name", "dob" fom "people" where "family_name" = %s'

# execute yourself with your curs object.
data = ("smith", )
curs.execute(parameterized_query, data)
```

You can also use `parameterize_query()` with the `"data"` argument *only* to prepare a dictionary insert statement.

- Include in the string query `"placeholders"` and `"fields"` so that `parameterize_query()` can replace 
them with the parameterised dictionary keys.
- Use the `"drop_keys"` optional keyword argument if there are certain keys of the dictionary that you want to drop.

```
insert_stmt = "insert into {table} ({fields}) values ({placeholders})"

data = {"name": "gianny", "dob": "23/01/1900", "ignore": [1, 2, 3]}
table = "people"

paramed_q = parameterize_query(insert_stmt, data, drop_keys=["ignore"])

print(paramed_q.as_string(conn))  

# will print to console:
'insert into "people" ("name", "dob") values (%(name)s, %(dob)s)'

curs.execute(parameterized_query, data)
```

### The do_query method 

The class `MyDb` would not be complete without a support for execution. Once you started your engine, established a 
connection - you probably want to run some queries, and you can, with the `.do_query()` method.

- *conn*: connection object returned by `.connect()` method. 
- *mode*: "r"(ead) for SELECT, "w"(rite) for INSERT, "wr"(write and read) for INSERT RETURNING.
- *query*: string query to be parameterised and executed.
- *data*: collection or dictionary containing data for placeholders (if tuple) or fields and placeholders (if dict).
- *kwargs*: kwarg to be parameterised and used to form query string.

It returns a psycopg2 iterable cursor object if r or wr mode otherwise None.

##### Some examples

###### insert from either tuple or dictionary
```
get_french_ppl = "select {cols} from {table} where {nat} = % or {place} = %"
cols = ["name", "family_name", "dob", "marital_status"]
data = ("french", "france")
curs = engine.do_query(conn, "r", get_french_ppl, data, nat="nationality", place="place_of_birth")
# curs is now an iterable with your data in it. 

# insert from tuple

insert_from_t = "insert into {table} ({cols}) values (%s, %s)"
data_t = (1, 2)
engine.do_query(conn, "w", insert_from_t, data_t, table="test", cols=['a', 'b'])

# insert from a dict dropping some k:v pairs. 

insert_from_d = "insert into {table} ({fields}) values ({placeholders})"
data_d = {"name": "gianny", "dob": "23/01/1900", "ignore": [1, 2, 3]}
engine.do_query(conn, "w", insert_from_d, data_d, table="test", drop_keys=["ignore"])
```

###### do a batch insert

For example, build a statement for a [multivalue](https://www.postgresql.org/docs/12/dml-insert.html) insert statement, given a list of dictionaries. 

```
from more_itertools import flatten

draft_stmt = "insert into {{table}} ({{placeholders}}) values {multivals}"
d1 = {"a":1, "b":2}
d2 = {"a":3, "b":4}
d_coll = [d1, d2]

def make_multivals(collection):
    height = len(collection)
    length = len(collection[0])
    val_line = "(" + ("%s, " * length).rstrip(", ") + "),"
    multivals = (val_line * height).rstrip(",") + ";"

def make_stmt(draft, d_coll):
    m = make_multivals(collection=d_coll)
    return draft.format(multivals=m)

data = flatten([[x for x in d.values()] for d in d_coll])
q = make_stmt(draft=draft_stmt, d_coll=d_coll)
cols = list(d)

# start your engine and your connection
engine.do_query(conn, "w", q, data, table="a_table", placeholders=cols)
```

###### curry do_query to always refer to the same table, with the same connection. 

```
import stringql
from functools import partial

data = {"name": "gianny", "dob": "13/01/2009", "ignore": []}

conn_string = "dbname=postgres user=postgres password=secreeeeetuuuh"
engine = stringql.start_engine(libpq_string=conn_string)
conn = engine.connect(schema="test")
create = ("create table if not exists {table} ("
          "id serial, "
          "name varchar,"
          "dob date)")

insert = "insert into {table} ({fields}) values({placeholders})"
select = "select {col} from {table}"

peopleTable_writer = partial(
    engine.do_query, conn=conn, mode="w", table="people")

peopleTable_reader = partial(
    engine.do_query, conn=conn, mode="r", table="people")

if __name__ == "__main__":
    peopleTable_writer(query=create)
    peopleTable_writer(query=insert, data=data, drop_keys=["ignore"])
    curs = peopleTable_reader(query=select, col="name")

    with curs:
        for record in curs:
            print(record)
```
