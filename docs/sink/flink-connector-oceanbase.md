## Flink Connector OceanBase

English | [简体中文](flink-connector-oceanbase_cn.md)

This is the Flink connector for OceanBase, which can be used to sink data to OceanBase via JDBC driver.

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://mvnrepository.com/artifact/com.oceanbase/flink-connector-oceanbase).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase</artifactId>
    <version>${project.version}</version>
</dependency>
```

You can also manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### Download Dependencies

Now the connector supports Alibaba Druid or HikariCP as the database connection pool, you can choose one of them to use in the application system.
- Druid ：[https://mvnrepository.com/artifact/com.alibaba/druid](https://mvnrepository.com/artifact/com.alibaba/druid)
- HikariCP：[https://mvnrepository.com/artifact/com.zaxxer/HikariCP](https://mvnrepository.com/artifact/com.zaxxer/HikariCP)

It should be noted that HikariCP no longer supports JDK 8 starting from 5.0.x, so only 4.0.x or earlier versions is compatible here.

The MySQL mode of the OceanBase database is compatible with the MySQL protocol, so the MySQL JDBC driver can be used directly. OceanBase also provides an official JDBC driver, which supports OceanBase's Oracle mode and MySQL mode.

- MySQL JDBC：[https://mvnrepository.com/artifact/mysql/mysql-connector-java](https://mvnrepository.com/artifact/mysql/mysql-connector-java)
- OceanBase JDBC：[https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client)

### Package with Dependencies

The JAR file of this program does not contain the dependencies mentioned above by default. If you want to package the JAR file with dependencies, you can use [maven-shade-plugin](https://maven.apache.org/plugins/maven-shade-plugin).

Here we provide an [example](../../tools/maven/shade/pom.xml), you can use the following command to generate a JAR file which contains all dependencies:

```shell
sh tools/maven/build.sh
```

After that the corresponding JAR file will be output to the `tools/maven/shade/target` directory, and the name format is `flink-sql-connector-oceanbase-${version}-shaded.jar`.

### Demo

Prepare Alibaba Druid and MySQL JDBC driver packages, and create the destination table 't_sink' under the 'test' database of the OceanBase.

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
                        + "    'table-name' = 't_sink',"
                        + "    'username' = 'root@test',"
                        + "    'password' = 'pswd',"
                        + "    'driver-class' = 'com.mysql.jdbc.Driver',"
                        + "    'connection-pool' = 'druid',"
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
      'cluster-name' = 'obcluster',
      'tenant-name' = 'test',
      'schema-name' = 'test',
      'table-name' = 't_sink',
      'username' = 'root@test#obcluster',
      'password' = 'pswd',
      'compatible-mode' = 'mysql',
      'driver-class' = 'com.mysql.jdbc.Driver',
      'connection-pool' = 'druid',
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
| url                        | Yes      |         | String   | JDBC url                                                                                                                               |
| schema-name                | Yes      |         | String   | The schema name or database name                                                                                                       |
| table-name                 | Yes      |         | String   | The table name                                                                                                                         |
| username                   | Yes      |         | String   | The username                                                                                                                           |
| password                   | Yes      |         | String   | The password                                                                                                                           |
| compatible-mode            | Yes      |         | String   | The compatible mode of OceanBase, can be 'mysql' or 'oracle'                                                                           |
| driver-class               | Yes      |         | String   | JDBC driver class name, like 'com.mysql.jdbc.Driver'                                                                                   |
| connection-pool            | Yes      |         | String   | Database connection pool type, can be 'druid' or 'hikari'                                                                              |
| cluster-name               | No       |         | String   | The cluster name of OceanBase, required when partition calculation is enabled                                                          |
| tenant-name                | No       |         | String   | The tenant name of OceanBase, required when partition calculation is enabled                                                           |
| connection-pool-properties | No       |         | String   | Database connection pool properties, need to correspond to pool type, and multiple values are separated by semicolons                  |
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

