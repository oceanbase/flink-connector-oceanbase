# Flink Connector OceanBase By Tools CDC

English | [简体中文](flink-connector-oceanbase-tools-cdc_cn.md)

This project is a flink command line tool that supports the synchronization of CDC tasks to oceanbase through the Flink command line, which greatly simplifies the command writing of data synchronization to oceanbase through flink.

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase-directload).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-tools-cdc</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-tools-cdc</artifactId>
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

You can also manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### Notes:

* Currently, the project supports using Flink CDC to access multiple tables or the entire database. During synchronization, you need to add the corresponding Flink CDC dependency in the `$FLINK_HOME/lib` directory, such as flink-sql-connector-mysql-cdc-\${version}. jar, flink-sql-connector-oracle-cdc-\${version}.jar, flink-sql-connector-sqlserver-cdc-\${version}.jar
* The dependent Flink CDC version needs to be above 3.1. If you need to use Flink CDC to synchronize MySQL and Oracle, you also need to add the relevant JDBC driver under `$FLINK_HOME/lib`
* When synchronizing to oceanbase, the url connection string of oceanbase needs to use the mysql protocol.

### MySQL  Synchronous OceanBase Example

#### Geting Ready

Create a table test_history_strt_sink in a MySql database test_db library, test_history_text.

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
```

#### Build A Flink Task

##### An example of the Flink command line

```shell
$FLIN
K_HOME/bin/flink run \
    -Dexecution.checkpointing.interval=10s \
    -Dparallelism.default=1 \
    -c com.oceanbase.connector.flink.tools.cdc.CdcTools \
    lib/flink-connector-oceanbase-tools-cdc-${version}.jar \
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

Replace the above database information with your real database information, and when a message similar to the following appears, the task is successfully built and submitted.

```shell
Job has been submitted with JobID 0177b201a407045a17445aa288f0f111
```

The tool automatically parses the information on the command line and creates a table, which can be queried and verified in OceanBase.

MySQL to insert test data

```sql
INSERT INTO test_db.test_history_str (itemid,clock,value,ns) VALUES
	 (1,2,'ces1',1123);
INSERT INTO test_db.test_history_text (itemid,clock,value,ns) VALUES
	 (1,21131,'ces1',21321),
	 (2,21321,'ces2',12321);
```

Since it is a CDC task, after data is inserted in MySQL, you can query and verify the synchronized data in OceanBase.

### Parameter parsing

This configuration is the program configuration information of flink

```shell
-Dexecution.checkpointing.interval=10s
-Dparallelism.default=1
```

Specify the JAR package of the program and the entry of the program

```shell
-c com.oceanbase.connector.flink.tools.cdc.CdcTools \
lib/flink-connector-oceanbase-tools-cdc-${version}.jar \
```

The name of the database

```shell
--database test_db
```

This name is customized, meaning the name given to this database, and ultimately serves as the naming rule for flink tasks.

## Configuration Items

#### Supported data sources

| Data source identifier  |     Data source      |
|-------------------------|----------------------|
| mysql-sync-database     | mysql datasource     |
| oracle-sync-database    | oracle datasource    |
| postgres-sync-database  | postgres datasource  |
| sqlserver-sync-database | sqlserver datasource |
| db2-sync-database       | db2 datasource       |

#### Configuration Items

|  Configuration Items   |                                                                                                                                                                                                                        Comment                                                                                                                                                                                                                        |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
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

#### Configuration items of sink-conf

| Configuration Items | Default Value | Required |                              Comment                              |
|---------------------|---------------|----------|-------------------------------------------------------------------|
| url                 | --            | N        | jdbc connection information, such as: jdbc:mysql://127.0.0.1:2881 |
| username            | --            | Y        | Username to access oceanbase                                      |
| password            | --            | Y        | Password to access oceanbase                                      |

## Reference information

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector](https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector)

