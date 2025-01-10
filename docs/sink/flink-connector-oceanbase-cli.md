# Flink Connector OceanBase CLI

English | [简体中文](flink-connector-oceanbase-cli_cn.md)

The project is a set of CLI (command line interface) tools that supports submitting Flink jobs to migrate data from other data sources to OceanBase.

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase-cli)，or get the latest snapshot packages at [Sonatype Snapshot](https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-connector-oceanbase-cli).

You can also manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### Using Flink CDC as Source

#### Dependencies

This project is based on the SQL Client JAR of [Flink CDC Source Connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/overview/).

We do not provide Flink CDC Source Connector in the JAR package of this project, so you need to manually download the used Flink CDC SQL JAR. Note that this project requires Flink CDC to be 3.2.0 or later version.

If you're using Flink Oracle CDC as source, you need also download the dependencies of the source connector, see the *Dependencies* chapter of [Oracle CDC Connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/oracle-cdc/#sql-client-jar).

#### Demo: Migrate from Flink MySQL CDC to OceanBase

##### Preparation

Add the CLI JAR `flink-connector-oceanbase-cli-xxx.jar` and dependency JAR `flink-sql-connector-mysql-cdc-xxx.jar` to `$FLINK_HOME/lib`.

Then prepare tables and data in MySQL database.

```mysql
use test_db;
CREATE TABLE test_history_str (
	itemid                   bigint                                    NOT NULL,
	clock                    integer         DEFAULT '0'               NOT NULL,
	value                    varchar(255)    DEFAULT ''                NOT NULL,
	ns                       integer         DEFAULT '0'               NOT NULL,
	PRIMARY KEY (itemid,clock,ns)
);
CREATE TABLE test_history_text (
	itemid                   bigint                                    NOT NULL,
	clock                    integer         DEFAULT '0'               NOT NULL,
	value                    text                                      NOT NULL,
	ns                       integer         DEFAULT '0'               NOT NULL,
	PRIMARY KEY (itemid,clock,ns)
);

INSERT INTO test_db.test_history_text (itemid,clock,value,ns) VALUES
	 (1,21131,'ces1',21321);
```

##### Submit Job via CLI

Replace the following command with your real database information, and execute it to submit a Flink job.

```shell
$FLINK_HOME/bin/flink run \
    -Dexecution.checkpointing.interval=10s \
    -Dparallelism.default=1 \
    -c com.oceanbase.connector.flink.tools.cdc.CdcTools \
    lib/flink-connector-oceanbase-cli-xxx.jar \
    mysql-sync-database \
    --database test_db \
    --mysql-conf hostname=xxxx \
    --mysql-conf port=3306 \
    --mysql-conf username=root \
    --mysql-conf password=xxxx \
    --mysql-conf database-name=test_db \
    --including-tables "tbl1|test.*" \
    --sink-conf username=xxxx \
    --sink-conf password=xxxx \
    --sink-conf url=jdbc:mysql://xxxx:xxxx
```

##### Check and Verify

Check the target OceanBase database, you should find out these two tables and one row data.

You can go on insert test data to MySQL database as below:

```sql
INSERT INTO test_db.test_history_str (itemid,clock,value,ns) VALUES
	 (1,2,'ces1',1123);
INSERT INTO test_db.test_history_text (itemid,clock,value,ns) VALUES
	 (2,21321,'ces2',12321);
```

Since it is a CDC task, after data is inserted in MySQL, you can query and verify the synchronized data in OceanBase.

#### Options

|         Option         |                                                                                                                                                                                                                        Comment                                                                                                                                                                                                                        |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| {identifier}           | Data source identifier, only mysql type `mysql-sync-database` is verified now.                                                                                                                                                                                                                                                                                                                                                                        |
| --job-name             | Flink task name, optional.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| --database             | Database name synchronized to OceanBase.                                                                                                                                                                                                                                                                                                                                                                                                              |
| --table-prefix         | OceanBase table prefix name, such as --table-prefix ods_.                                                                                                                                                                                                                                                                                                                                                                                             |
| --table-suffix         | Same as above, the suffix name of the OceanBase table.                                                                                                                                                                                                                                                                                                                                                                                                |
| --including-tables     | For MySQL tables that need to be synchronized, you can use｜to separate multiple tables and support regular expressions. For example --including-tables table1.                                                                                                                                                                                                                                                                                        |
| --excluding-tables     | For tables that do not need to be synchronized, the usage is the same as above.                                                                                                                                                                                                                                                                                                                                                                       |
| --mysql-conf           | MySQL CDC Source configuration,where hostname/username/password/database-name is required. When the synchronized library table contains a non-primary key table, `scan.incremental.snapshot.chunk.key-column` must be set, and only one field of non-null type can be selected.<br/>For example: `scan.incremental.snapshot.chunk.key-column=database.table:column,database.table1:column...`, different database table columns are separated by `,`. |
| --oracle-conf          | Oracle CDC Source configuration,where hostname/username/password/database-name/schema-name is required.                                                                                                                                                                                                                                                                                                                                               |
| --postgres-conf        | Postgres CDC Source configuration,where hostname/username/password/database-name/schema-name/slot.name is required.                                                                                                                                                                                                                                                                                                                                   |
| --sqlserver-conf       | SQLServer CDC Source configuration,  where hostname/username/password/database-name/schema-name is required.                                                                                                                                                                                                                                                                                                                                          |
| --db2-conf             | Db2 CDC Source configuration,  where hostname/username/password/database-name/schema-name is required.                                                                                                                                                                                                                                                                                                                                                |
| --sink-conf            | See below --sink-conf configuration items.                                                                                                                                                                                                                                                                                                                                                                                                            |
| --ignore-default-value | Turn off synchronization of MySQL table structures by default. It is suitable for the situation when synchronizing MySQL data to oceanbase, the field has a default value, but the actual inserted data is null.                                                                                                                                                                                                                                      |
| --create-table-only    | Whether to only synchronize the structure of the table.                                                                                                                                                                                                                                                                                                                                                                                               |

`--sink-conf` option

|  Option  | Default Value | Required |                              Comment                              |
|----------|---------------|----------|-------------------------------------------------------------------|
| url      | --            | N        | jdbc connection information, such as: jdbc:mysql://127.0.0.1:2881 |
| username | --            | Y        | Username to access oceanbase                                      |
| password | --            | Y        | Password to access oceanbase                                      |

## Reference information

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector](https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector)

