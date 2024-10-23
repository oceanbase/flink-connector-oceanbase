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

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 8%">Required by Table API</th>
                <th class="text-left" style="width: 7%">Required by DataStream</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>url</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>JDBC url.</td>
            </tr>
            <tr>
                <td>username</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The username.</td>
            </tr>
            <tr>
                <td>password</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The password.</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td>Yes</td>
                <td>Not supported</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The schema name or database name.</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>Yes</td>
                <td>Not supported</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The table name.</td>
            </tr>
            <tr>
                <td>driver-class-name</td>
                <td>No</td>
                <td>No</td>
                <td>com.mysql.cj.jdbc.Driver</td>
                <td>String</td>
                <td>The driver class name, use 'com.mysql.cj.jdbc.Driver' by default. If other value is set, you need to introduce the driver manually.</td>
            </tr>
            <tr>
                <td>druid-properties</td>
                <td>No</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Druid connection pool properties, multiple values are separated by semicolons.</td>
            </tr>
            <tr>
                <td>sync-write</td>
                <td>No</td>
                <td>No</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Whether to write data synchronously, will not use buffer if it's set to 'true'.</td>
            </tr>
            <tr>
                <td>buffer-flush.interval</td>
                <td>No</td>
                <td>No</td>
                <td>1s</td>
                <td>Duration</td>
                <td>Buffer flush interval. Set '0' to disable scheduled flushing.</td>
            </tr>
            <tr>
                <td>buffer-flush.buffer-size</td>
                <td>No</td>
                <td>No</td>
                <td>1000</td>
                <td>Integer</td>
                <td>Buffer size.</td>
            </tr>
            <tr>
                <td>max-retries</td>
                <td>No</td>
                <td>No</td>
                <td>3</td>
                <td>Integer</td>
                <td>Max retry times on failure.</td>
            </tr>
            <tr>
                <td>memstore-check.enabled</td>
                <td>No</td>
                <td>No</td>
                <td>true</td>
                <td>Boolean</td>
                <td>Whether enable memstore check.</td>
            </tr>
            <tr>
                <td>memstore-check.threshold</td>
                <td>No</td>
                <td>No</td>
                <td>0.9</td>
                <td>Double</td>
                <td>Memstore usage threshold ratio relative to the limit value.</td>
            </tr>
            <tr>
                <td>memstore-check.interval</td>
                <td>No</td>
                <td>No</td>
                <td>30s</td>
                <td>Duration</td>
                <td>Memstore check interval.</td>
            </tr>
            <tr>
                <td>partition.enabled</td>
                <td>No</td>
                <td>No</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Whether to enable partition calculation and flush records by partitions. Only works when 'sync-write' and 'direct-load.enabled' are 'false'.</td>
            </tr>
            <tr>
                <td>table.oracle-tenant-case-insensitive</td>
                <td>No</td>
                <td>No</td>
                <td>true</td>
                <td>Boolean</td>
                <td>By default, under the Oracle tenant, schema names and column names are case-insensitive.</td>
            </tr>
        </tbody>
    </table>
</div>

## References

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)

