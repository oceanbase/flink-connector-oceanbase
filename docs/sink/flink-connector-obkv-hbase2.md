## Flink Connector OBKV HBase2

English | [简体中文](flink-connector-obkv-hbase2_cn.md)

This is a Flink connector for OBKV HBase that provides flat table structure and usage patterns. It writes data to OceanBase through [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java).

## Key Features

- **Flat Table Structure**: Clean flat table definitions without nested ROW types
- **Dynamic Column Support**: Flexible handling of unknown column names at runtime
- **Partial Column Updates**: Support for ignoring null values and excluding specific columns
- **Timestamp Control**: Set different timestamps for different columns
- **Batch Writing**: Buffer and batch flush for improved performance

## Differences from [flink-connector-obkv-hbase](./flink-connector-obkv-hbase.md) Connector

### [flink-connector-obkv-hbase](./flink-connector-obkv-hbase.md)  (Nested Structure)

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  family1 ROW <column1 STRING, column2 STRING>,  -- Nested ROW structure
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  ...
);
```

### flink-connector-obkv-hbase2 (Flat Structure)

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,       -- Flat structure, direct column definitions
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'columnFamily' = 'f',  -- Column family specified in configuration
  'schema-name' = 'test',
  'table-name' = 'htable1',
  ...
);
```

## Getting Started

You can find official releases on the [Releases page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-obkv-hbase2).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase2</artifactId>
    <version>${project.version}</version>
</dependency>
```

For the latest snapshot versions, configure the Maven snapshot repository:

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase2</artifactId>
    <version>${project.version}</version>
</dependency>

<repositories>
    <repository>
        <id>sonatype-snapshots</id>
        <name>Sonatype Snapshot Repository</name>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

You can also build from source:

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### SQL JAR

To use this connector directly with Flink SQL, download the fat JAR `flink-sql-connector-obkv-hbase2-${project.version}.jar`:

- Release versions: https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-obkv-hbase2
- Snapshot versions: https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-obkv-hbase2

## Usage Examples

### Preparation

Create a table in OceanBase named `htable1$f`, where `htable1` is the HBase table name and `f` is the column family.

```mysql
use test;
CREATE TABLE `htable1$f`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);
```

### Flink SQL Examples

Place the required JAR files in Flink's lib directory, then create the target table using SQL Client.

#### 1. Basic Usage (Config URL Connection)

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456'
);

-- Insert data
INSERT INTO t_sink VALUES ('row1', 'value1', 'value2');
INSERT INTO t_sink VALUES ('row2', 'value3', 'value4');
```

#### 2. Using ODP Connection

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'odp-mode' = 'true',
  'odp-ip' = '127.0.0.1',
  'odp-port' = '2885',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321'
);
```

#### 3. Partial Column Updates

The connector provides flexible partial column update capabilities, supporting different ways to control which columns to update:

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,
  column2 STRING,
  column3 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'ignoreNullWhenUpdate' = 'true',  -- Skip NULL columns for partial updates
  'excludeUpdateColumns' = 'column3'  -- Permanently exclude column3 from updates
);

-- Only update column1, column2 and column3 remain unchanged
INSERT INTO t_sink (rowkey, column1) VALUES ('1', 'new_value');
```

**Two update control methods:**
- `ignoreNullWhenUpdate=true`: Skip columns with NULL values, any NULL column will not be written to OBKV-HBase
- `excludeUpdateColumns`: Permanently exclude specified columns, these columns will never be updated

#### 4. Dynamic Column Mode

Dynamic column mode allows you to specify column names at runtime, suitable for scenarios with unknown column names.

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  columnKey STRING,    -- Dynamic column name
  columnValue STRING,  -- Dynamic column value
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'dynamicColumnSink' = 'true'  -- Enable dynamic column mode
);

-- Insert dynamic column data
INSERT INTO t_sink VALUES ('row1', 'dynamic_col_1', 'value1');
INSERT INTO t_sink VALUES ('row1', 'dynamic_col_2', 'value2');
```

#### 5. Timestamp Control

##### 5.1 Use tsColumn for Unified Timestamp

**Note**: Timestamp columns are not written to HBase by default and are only used to control HBase version time. If you need to store the timestamp column as a data column, do not configure it in `tsColumn` or `tsMap`.

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  ts_col BIGINT,       -- Timestamp column (not written to HBase)
  column1 STRING,
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'tsColumn' = 'ts_col',  -- Use ts_col as timestamp for all columns
  'tsInMills' = 'true'    -- Timestamp unit is milliseconds
);
-- Result: Only 2 columns in HBase: column1, column2 (ts_col is automatically excluded)
```

##### 5.2 Use tsMap for Different Timestamps

**Note**: Timestamp columns are not written to HBase by default and are only used to control HBase version time.

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  ts_col1 BIGINT,      -- Timestamp column 1 (not written to HBase)
  ts_col2 BIGINT,      -- Timestamp column 2 (not written to HBase)
  column1 STRING,
  column2 STRING,
  column3 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'tsMap' = 'ts_col1:column1;ts_col1:column2;ts_col2:column3',  -- column1 and column2 use ts_col1, column3 uses ts_col2
  'tsInMills' = 'true'
);
-- Result: Only 3 columns in HBase: column1, column2, column3 (ts_col1 and ts_col2 are automatically excluded)
```

## Configuration Options

|     Option Name      | Required | Default |   Type   |                                                                                                  Description                                                                                                   |
|----------------------|----------|---------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| connector            | Yes      |         | String   | Must be set to 'obkv-hbase2' to use this connector.                                                                                                                                                            |
| schema-name          | Yes      |         | String   | The database name in OceanBase.                                                                                                                                                                                |
| table-name           | Yes      |         | String   | HBase table name (without column family suffix).                                                                                                                                                               |
| username             | Yes      |         | String   | Username.                                                                                                                                                                                                      |
| password             | Yes      |         | String   | Password.                                                                                                                                                                                                      |
| odp-mode             | No       | false   | Boolean  | Whether to connect to OBKV via ODP. Set to 'true' to connect via ODP, otherwise connect via config url.                                                                                                        |
| url                  | No       |         | String   | Cluster config url, can be queried by `SHOW PARAMETERS LIKE 'obconfig_url'`. Required when 'odp-mode' is 'false'.                                                                                              |
| sys.username         | No       |         | String   | Username of sys tenant. Required when 'odp-mode' is 'false'.                                                                                                                                                   |
| sys.password         | No       |         | String   | Password of sys tenant. Required when 'odp-mode' is 'false'.                                                                                                                                                   |
| odp-ip               | No       |         | String   | IP address of ODP. Required when 'odp-mode' is 'true'.                                                                                                                                                         |
| odp-port             | No       | 2885    | Integer  | RPC port of ODP. Optional when 'odp-mode' is 'true'.                                                                                                                                                           |
| hbase.properties     | No       |         | String   | Properties to configure 'obkv-hbase-client-java', separated by semicolons. Format: 'key1=value1;key2=value2'.                                                                                                  |
| columnFamily         | No       | f       | String   | HBase column family name.                                                                                                                                                                                      |
| rowkeyDelimiter      | No       | :       | String   | Delimiter for composite primary keys.                                                                                                                                                                          |
| writePkValue         | No       | false   | Boolean  | Whether to write primary key values as column values in HBase.                                                                                                                                                 |
| bufferSize           | No       | 5000    | Integer  | Buffer size for batch writing.                                                                                                                                                                                 |
| flushIntervalMs      | No       | 2000    | Duration | Flush interval for batch writing (milliseconds). Set to '0' to disable scheduled flushing.                                                                                                                     |
| ignoreNullWhenUpdate | No       | true    | Boolean  | Whether to ignore null values when updating. When set to 'true', columns with null values are skipped for partial updates; when set to 'false', null values are written to HBase.                              |
| ignoreDelete         | No       | false   | Boolean  | Whether to ignore delete operations. When set to 'true', delete operations are not executed.                                                                                                                   |
| excludeUpdateColumns | No       |         | String   | Column names to exclude from updates, separated by commas. These columns will not be updated.                                                                                                                  |
| dynamicColumnSink    | No       | false   | Boolean  | Whether to enable dynamic column mode. When enabled, non-PK columns must be exactly 2 columns (columnKey and columnValue), both must be VARCHAR type.                                                          |
| tsColumn             | No       |         | String   | Timestamp column name. When specified, the value of this column will be used as the timestamp for all columns. If this is set, 'tsMap' will be ignored.                                                        |
| tsMap                | No       |         | String   | Timestamp mapping configuration. Format: 'tsColumn0:column0;tsColumn0:column1;tsColumn1:column2', meaning column0 and column1 use tsColumn0's value as timestamp, column2 uses tsColumn1's value as timestamp. |
| tsInMills            | No       | true    | Boolean  | Whether timestamp unit is milliseconds. When set to 'false', timestamp unit is seconds.                                                                                                                        |   |

## Core Features

This connector provides the following core features:

- ✅ Flat table structure definitions
- ✅ Dynamic column support
- ✅ Partial column updates (ignoreNull, excludeUpdateColumns)
- ✅ Timestamp control (tsColumn, tsMap)
- ✅ Batch writing and buffering
- ❌ Source functionality (this connector only supports Sink)
- ❌ Lookup Join functionality (this connector only supports Sink)

## Notes

1. **Table Requirements**: Flink table must define a PRIMARY KEY.
2. **Dynamic Column Mode**: When enabled, non-PK columns must be exactly 2 VARCHAR columns.
3. **Timestamp Priority**: If both `tsColumn` and `tsMap` are set, `tsColumn` takes precedence.
4. **OceanBase Table Naming**: In OceanBase, the actual HBase table name is in `tableName$columnFamily` format.
5. **Encoding**: Uses HBase's `Bytes.toBytes()` for encoding, compatible with OBKV HBase Client SDK.

## References

- [OceanBase Official Website](https://www.oceanbase.com/)
- [OBKV HBase Client Java](https://github.com/oceanbase/obkv-hbase-client-java)
- [Flink Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/)

