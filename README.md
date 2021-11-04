# write_clickhouse
Collectd python plugin for streaming data into clickhouse

## Installation

1. Download write_clickhouse.py from this repo.
2. Install Python clickhouse driver [from here](https://github.com/mymarilyn/clickhouse-driver).
3. Create table in clickhouse. Field names can't be configured yet. Overwrite them in plugin if needed.

```
CREATE TABLE my_server_stats (
    dt Date,
    time DateTime,
    type LowCardinality(String), -- metrics type
    type_instance LowCardinality(String), -- metrics type instance
    values Array(String) -- metric values as array of strings
)
ENGINE = MergeTree()
ORDER BY (time)
TTL time + INTERVAL 1 MONTH -- rows older than one month will be truncated, suitable for logs
```
4. Stop collectd processing with `service collectd stop`
5. Change collectd configuration file `/etc/collectd/collectd.conf`:

```
LoadPlugin python

<Plugin python>
        ModulePath "/path/to/write_clickhouse/plugin/file/directory"
        LogTraces true
        Import "write_clickhouse"
        <Module write_clickhouse>
                url "localhost" # host where clickhouse server is located
                port 9000 # clickhouse native TCP/IP protocol endpoint port
                user "default"
                password ""
                database "default" # clickhouse database name
                table "my_server_stats" # clickhouse table name
                write "df_complex.free" "df_complex.used" # optional, type.type_instance list you want to collect, allow all types if empty
        </Module>
</Plugin>
```
6. Start collectd again with `service collectd start`
