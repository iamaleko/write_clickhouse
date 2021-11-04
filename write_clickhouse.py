import collectd
import clickhouse_driver
from datetime import datetime

PLUGIN_NAME = 'write_clickhouse'

OPTION_WRITE = 'write'
OPTION_URL = 'url'
OPTION_PORT = 'port'
OPTION_USER = 'user'
OPTION_PASSWORD = 'password'
OPTION_DATABASE = 'database'
OPTION_TABLE = 'table'

CLICKHOUSE_URL = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''
CLICKHOUSE_DATABASE = 'default'
CLICKHOUSE_TABLE = ''

FIELD_DT = 'dt'
FIELD_TIME = 'time'
FIELD_TYPE = 'type'
FIELD_TYPE_INSTANCE = 'type_instance'
FIELD_VALUES = 'values'

# expected table structure:
# dt Date,
# time DateTime,
# type LowCardinality(String), -- metrics type
# type_instance LowCardinality(String), -- metrics type instance
# values Array(String) -- array of string values

WRITE = {}
WRITE_ANY = '*'

def config(config):
  for kv in config.children:
    if kv.key == OPTION_WRITE:
      global WRITE
      for pair in kv.values:
        [type, type_instance] = pair.split('.')
        type = str(type)
        type_instance = str(type_instance)
        if type not in WRITE:
          WRITE[type] = set()
        WRITE[type].add(type_instance)

    if kv.key == OPTION_URL:
      global CLICKHOUSE_URL
      CLICKHOUSE_URL = str(kv.values[0])

    if kv.key == OPTION_PORT:
      global CLICKHOUSE_PORT
      CLICKHOUSE_PORT = int(kv.values[0])

    if kv.key == OPTION_USER:
      global CLICKHOUSE_USER
      CLICKHOUSE_USER = str(kv.values[0])

    if kv.key == OPTION_PASSWORD:
      global CLICKHOUSE_PASSWORD
      CLICKHOUSE_PASSWORD = str(kv.values[0])

    if kv.key == OPTION_DATABASE:
      global CLICKHOUSE_DATABASE
      CLICKHOUSE_DATABASE = str(kv.values[0])

    if kv.key == OPTION_TABLE:
      global CLICKHOUSE_TABLE
      CLICKHOUSE_TABLE = str(kv.values[0])

def write(data):
  (time, type, type_instance, values) = prepare(data)

  # write only if the type and type_instance have been filtered,
  # or the filtering parameter is empty
  if len(WRITE) == 0 or type in WRITE and (WRITE_ANY in WRITE[type] or type_instance in WRITE[type]):
    # clickhouse_driver does not support multithreading since the native clickhouse
    # protocol only supports one request at a time within a single connection,
    # so you have to create a new connection for each request
    ch = clickhouse_driver.Client(
      host = CLICKHOUSE_URL,
      user = CLICKHOUSE_USER,
      port = CLICKHOUSE_PORT,
      password = CLICKHOUSE_PASSWORD,
      database = CLICKHOUSE_DATABASE
    )
    query = 'INSERT INTO %s (%s, %s, %s, %s, %s) VALUES' % (CLICKHOUSE_TABLE, FIELD_DT, FIELD_TIME, FIELD_TYPE, FIELD_TYPE_INSTANCE, FIELD_VALUES)
    ch.execute(query, [(time, time, type, type_instance, values)])
    ch.disconnect()

def prepare(data):
  type = str(data.type)
  type_instance = str(data.type_instance)
  time = datetime.fromtimestamp(int(data.time))

  # we go through and convert the values of the array to strings
  # since we cannot guarantee a certain type
  values = []
  for i, value in enumerate(data.values):
    values.append(str(value))
  return (time, type, type_instance, values)

collectd.register_config(config)
collectd.register_write(write)
