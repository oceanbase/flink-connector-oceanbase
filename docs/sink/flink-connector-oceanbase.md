## Flink Connector OceanBase

English | [简体中文](flink-connector-oceanbase_cn.md)

This is the Flink connector for OceanBase, which can be used to sink data to OceanBase via JDBC driver.

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase</artifactId>
    <version>${project.version}</version>
</dependency>
```

If you'd rather use the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase</artifactId>
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

### SQL JAR

To use this connector through Flink SQL directly, you need to download the shaded jar file named `flink-sql-connector-oceanbase-${project.version}.jar`:

- Release versions: https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-oceanbase
- Snapshot versions: https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-oceanbase

This project has built-in MySQL driver 8.0.28. For users of OceanBase EE who want to use OceanBase JDBC driver, they need to manually introduce the following dependencies:

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Dependency Item</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client/2.4.9">com.oceanbase:oceanbase-client:2.4.9</a></td>
        <td>Used for connecting to OceanBase EE.</td>
      </tr>
    </tbody>
</table>
</div>

### Demo

#### Preparation

Create the destination table 't_sink' under the 'test' database of the OceanBase MySQL mode.

```mysql
USE test;
CREATE TABLE `t_sink` (
  `id` int(10) NOT NULL,
  `username` varchar(20) DEFAULT NULL,
  `score` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
```

#### Flink SQL Demo

Put the JAR files of dependencies to the 'lib' directory of Flink, and then create the destination table with Flink SQL through the sql client.

```sql
CREATE TABLE t_sink
(
  id       INT,
  username VARCHAR,
  score    INT,
  PRIMARY KEY (id) NOT ENFORCED
) with (
  'connector' = 'oceanbase',
  'url' = 'jdbc:mysql://127.0.0.1:2881/test',
  'schema-name' = 'test',
  'table-name' = 't_sink',
  'username' = 'root@test#obcluster',
  'password' = 'pswd',
  'druid-properties' = 'druid.initialSize=10;druid.maxActive=100;',
  'buffer-flush.interval' = '1s',
  'buffer-flush.buffer-size' = '5000',
  'max-retries' = '3');
```

Insert records by Flink SQL.

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

Once executed, the records should have been written to OceanBase.

For users of OceanBase EE, you need to specify the `url` and `driver-class-name` corresponding to the OceanBase JDBC driver.

```sql
CREATE TABLE t_sink
(
    id       INT,
    username VARCHAR,
    score    INT,
    PRIMARY KEY (id) NOT ENFORCED
) with (
    'connector' = 'oceanbase',
    'url' = 'jdbc:oceanbase://127.0.0.1:2881/SYS',
    'driver-class-name' = 'com.oceanbase.jdbc.Driver',
    'schema-name' = 'SYS',
    'table-name' = 'T_SINK',
    'username' = 'SYS@test#obcluster',
    'password' = 'pswd',
    'druid-properties' = 'druid.initialSize=10;druid.maxActive=100;',
    'buffer-flush.interval' = '1s',
    'buffer-flush.buffer-size' = '5000',
    'max-retries' = '3'
    );
```

## Configuration

|             Option             | Required by Table API | Required by DataStream |         Default          |   Type   |                                                                                                                                                                                                                                                        Description                                                                                                                                                                                                                                                         |
|--------------------------------|-----------------------|------------------------|--------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                            | Yes                   | Yes                    |                          | String   | JDBC url.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| username                       | Yes                   | Yes                    |                          | String   | The username.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| password                       | Yes                   | Yes                    |                          | String   | The password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| schema-name                    | Yes                   | Not supported          |                          | String   | The schema name or database name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| table-name                     | Yes                   | Not supported          |                          | String   | The table name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| driver-class-name              | No                    | No                     | com.mysql.cj.jdbc.Driver | String   | The driver class name, use 'com.mysql.cj.jdbc.Driver' by default. If other value is set, you need to introduce the driver manually.                                                                                                                                                                                                                                                                                                                                                                                        |
| druid-properties               | No                    | No                     |                          | String   | Druid connection pool properties, multiple values are separated by semicolons.                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| sync-write                     | No                    | No                     | false                    | Boolean  | Whether to write data synchronously, will not use buffer if it's set to 'true'.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| buffer-flush.interval          | No                    | No                     | 1s                       | Duration | Buffer flush interval. Set '0' to disable scheduled flushing.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| buffer-flush.buffer-size       | No                    | No                     | 1000                     | Integer  | Buffer size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| max-retries                    | No                    | No                     | 3                        | Integer  | Max retry times on failure.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| memstore-check.enabled         | No                    | No                     | true                     | Boolean  | Whether enable memstore check.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| memstore-check.threshold       | No                    | No                     | 0.9                      | Double   | Memstore usage threshold ratio relative to the limit value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| memstore-check.interval        | No                    | No                     | 30s                      | Duration | Memstore check interval.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition.enabled              | No                    | No                     | false                    | Boolean  | Whether to enable partition calculation and flush records by partitions. Only works when 'sync-write' and 'direct-load.enabled' are 'false'.                                                                                                                                                                                                                                                                                                                                                                               |
| direct-load.enabled            | No                    | No                     | false                    | Boolean  | Whether to enable direct load. Note that direct load task requires the sink parallelism to be 1.                                                                                                                                                                                                                                                                                                                                                                                                                           |
| direct-load.host               | No                    | No                     |                          | String   | The hostname or IP address used in direct load task. Required when 'direct-load.enabled' is true.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| direct-load.port               | No                    | No                     | 2882                     | Integer  | The rpc port used in direct load task. Required when 'direct-load.enabled' is true.                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| direct-load.parallel           | No                    | No                     | 8                        | Integer  | Parallelism of direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| direct-load.max-error-rows     | No                    | No                     | 0                        | Long     | Maximum tolerable number of error rows of direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| direct-load.dup-action         | No                    | No                     | REPLACE                  | STRING   | Action when there is duplicated record of direct load task. Can be 'STOP_ON_DUP', 'REPLACE' or 'IGNORE'.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| direct-load.timeout            | No                    | No                     | 7d                       | Duration | Timeout for direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| direct-load.heartbeat-timeout  | No                    | No                     | 60s                      | Duration | Client heartbeat timeout in direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| direct-load.heartbeat-interval | No                    | No                     | 10s                      | Duration | Client heartbeat interval in direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| direct-load.load-method        | No                    | No                     | full                     | String   | Direct load load mode: full, inc, inc_replace. full: full direct load, default value. inc: normal incremental direct load, primary key conflict check will be performed, observer-4.3.2 and above support, dupAction REPLACE is not supported for the time being.inc_replace: special replace mode incremental direct load, no primary key conflict check will be performed, directly overwrite the old data (equivalent to the effect of replace), dupAction parameter will be ignored, observer-4.3.2 and above support. |

## References

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)

