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

#### Java Demo

Take Maven project for example, add the required dependencies to the pom.xml, and then use the following code.

```java
package com.oceanbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TABLE t_sink ( "
                        + "  id       INT,"
                        + "  username VARCHAR,"
                        + "  score    INT,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") with ("
                        + "    'connector' = 'oceanbase',"
                        + "    'url' = 'jdbc:mysql://127.0.0.1:2881/test',"
                        + "    'schema-name'= 'test',"
                        + "    'table-name' = 't_sink',"
                        + "    'username' = 'root@test#obcluster',"
                        + "    'password' = 'pswd',"
                        + "    'druid-properties' = 'druid.initialSize=10;druid.maxActive=100',"
                        + "    'buffer-flush.interval' = '1s',"
                        + "    'buffer-flush.buffer-size' = '5000',"
                        + "    'max-retries' = '3'"
                        + "    );");

        tEnv.executeSql(
                        "INSERT INTO t_sink VALUES "
                                + "(1, 'Tom', 99),"
                                + "(2, 'Jerry', 88),"
                                + "(1, 'Tom', 89);")
                .await();
    }
}

```

Once executed, the records should have been written to OceanBase.

For more information please refer to [OceanBaseConnectorITCase.java](../../flink-connector-oceanbase/src/test/java/com/oceanbase/connector/flink/OceanBaseConnectorITCase.java).

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

## Configuration

|          Option          | Required by Table API | Required by DataStream |         Default          |   Type   |                                                             Description                                                             |
|--------------------------|-----------------------|------------------------|--------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------|
| url                      | Yes                   | Yes                    |                          | String   | JDBC url.                                                                                                                           |
| username                 | Yes                   | Yes                    |                          | String   | The username.                                                                                                                       |
| password                 | Yes                   | Yes                    |                          | String   | The password.                                                                                                                       |
| schema-name              | Yes                   | Not supported          |                          | String   | The schema name or database name.                                                                                                   |
| table-name               | Yes                   | Not supported          |                          | String   | The table name.                                                                                                                     |
| compatible-mode          | No                    | No                     | mysql                    | String   | The compatible mode of OceanBase, can be 'mysql' or 'oracle'.                                                                       |
| driver-class-name        | No                    | No                     | com.mysql.cj.jdbc.Driver | String   | The driver class name, use 'com.mysql.cj.jdbc.Driver' by default. If other value is set, you need to introduce the driver manually. |
| cluster-name             | No                    | No                     |                          | String   | The cluster name of OceanBase, required when 'partition.enabled' is 'true'.                                                         |
| tenant-name              | No                    | No                     |                          | String   | The tenant name of OceanBase, required when 'partition.enabled' is 'true'.                                                          |
| druid-properties         | No                    | No                     |                          | String   | Druid connection pool properties, multiple values are separated by semicolons.                                                      |
| sync-write               | No                    | No                     | false                    | Boolean  | Whether to write data synchronously, will not use buffer if it's set to 'true'.                                                     |
| buffer-flush.interval    | No                    | No                     | 1s                       | Duration | Buffer flush interval. Set '0' to disable scheduled flushing.                                                                       |
| buffer-flush.buffer-size | No                    | No                     | 1000                     | Integer  | Buffer size.                                                                                                                        |
| max-retries              | No                    | No                     | 3                        | Integer  | Max retry times on failure.                                                                                                         |
| memstore-check.enabled   | No                    | No                     | true                     | Boolean  | Whether enable memstore check.                                                                                                      |
| memstore-check.threshold | No                    | No                     | 0.9                      | Double   | Memstore usage threshold ratio relative to the limit value.                                                                         |
| memstore-check.interval  | No                    | No                     | 30s                      | Duration | Memstore check interval.                                                                                                            |
| partition.enabled        | No                    | No                     | false                    | Boolean  | Whether to enable partition calculation and flush records by partitions.                                                            |

## References

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)

