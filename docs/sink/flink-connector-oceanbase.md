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
                        + "    'url' = 'jdbc:oceanbase://127.0.0.1:2881/test',"
                        + "    'schema-name'= 'test',"
                        + "    'table-name' = 't_sink',"
                        + "    'username' = 'root@test#obcluster',"
                        + "    'password' = 'pswd',"
                        + "    'compatible-mode' = 'mysql',"
                        + "    'connection-pool-properties' = 'druid.initialSize=10;druid.maxActive=100',"
                        + "    'upsert-mode' = 'true',"
                        + "    'buffer-flush.interval' = '1s',"
                        + "    'buffer-flush.buffer-size' = '5000',"
                        + "    'buffer-flush.batch-size' = '100',"
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
      'url' = 'jdbc:oceanbase://127.0.0.1:2881/test',
      'schema-name' = 'test',
      'table-name' = 't_sink',
      'username' = 'root@test#obcluster',
      'password' = 'pswd',
      'compatible-mode' = 'mysql',
      'connection-pool-properties' = 'druid.initialSize=10;druid.maxActive=100;',
      'upsert-mode' = 'true',
      'buffer-flush.interval' = '1s',
      'buffer-flush.buffer-size' = '5000',
      'buffer-flush.batch-size' = '100',
      'max-retries' = '3'
      );
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

|           Option           | Required | Default |   Type   |                                                              Description                                                               |
|----------------------------|----------|---------|----------|----------------------------------------------------------------------------------------------------------------------------------------|
| url                        | Yes      |         | String   | JDBC url, should be prefixed with 'jdbc:oceanbase://'                                                                                  |
| schema-name                | Yes      |         | String   | The schema name or database name                                                                                                       |
| table-name                 | Yes      |         | String   | The table name                                                                                                                         |
| username                   | Yes      |         | String   | The username                                                                                                                           |
| password                   | Yes      |         | String   | The password                                                                                                                           |
| compatible-mode            | Yes      |         | String   | The compatible mode of OceanBase, can be 'mysql' or 'oracle'                                                                           |
| cluster-name               | No       |         | String   | The cluster name of OceanBase, required when partition calculation is enabled                                                          |
| tenant-name                | No       |         | String   | The tenant name of OceanBase, required when partition calculation is enabled                                                           |
| connection-pool-properties | No       |         | String   | Druid connection pool properties, multiple values are separated by semicolons                                                          |
| upsert-mode                | No       | true    | Boolean  | Whether to use upsert mode                                                                                                             |
| buffer-flush.interval      | No       | 1s      | Duration | Buffer flush interval                                                                                                                  |
| buffer-flush.buffer-size   | No       | 1000    | Integer  | Buffer size                                                                                                                            |
| buffer-flush.batch-size    | No       | 100     | Integer  | Buffer flush batch size                                                                                                                |
| max-retries                | No       | 3       | Integer  | Max retry times on failure                                                                                                             |
| memstore-check.enabled     | No       | true    | Boolean  | Whether enable memstore check                                                                                                          |
| memstore-check.threshold   | No       | 0.9     | Double   | Memstore usage threshold ratio relative to the limit value                                                                             |
| memstore-check.interval    | No       | 30s     | Duration | Memstore check interval                                                                                                                |
| partition.enabled          | No       | false   | Boolean  | Whether to enable partition calculation and flush records by partitions                                                                |
| partition.number           | No       | 1       | Integer  | The number of partitions. When the 'partition.enabled' is 'true', the same number of threads will be used to flush records in parallel |

## References

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)

