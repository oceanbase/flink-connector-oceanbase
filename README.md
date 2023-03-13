# OceanBase Connector for Apache Flink

English | [简体中文](README_CN.md)

This repository contains the OceanBase sink connector for Apache Flink.

## Getting Started

Prerequisites

- JDK 8
- Flink 1.15 or later version
- Database connection pool, can be Alibaba Druid or HikariCP
- JDBC driver, can be MySQL JDBC driver or OceanBase JDBC driver

You can get the release packages at [Releases](https://github.com/oceanbase/flink-connector-oceanbase/releases) page, or manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

Now the connector supports Alibaba Druid or HikariCP as the database connection pool, you can choose one of them to use in the application system.
- Druid ：[https://mvnrepository.com/artifact/com.alibaba/druid](https://mvnrepository.com/artifact/com.alibaba/druid)
- HikariCP：[https://mvnrepository.com/artifact/com.zaxxer/HikariCP](https://mvnrepository.com/artifact/com.zaxxer/HikariCP)

It should be noted that HikariCP no longer supports JDK 8 starting from 5.0.x, so only 4.0.x or earlier versions is compatible here.

The MySQL mode of the OceanBase database is compatible with the MySQL protocol, so the MySQL JDBC driver can be used directly. OceanBase also provides an official JDBC driver, which supports OceanBase's Oracle mode and MySQL mode.

- MySQL JDBC：[https://mvnrepository.com/artifact/mysql/mysql-connector-java](https://mvnrepository.com/artifact/mysql/mysql-connector-java)
- OceanBase JDBC：[https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client)

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
      'table-name' = 't_sink',
      'username' = 'root@test',
      'password' = 'pswd',
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

| Option                     | Required | Default | Type     | Description                                                                                                           |
|----------------------------|----------|---------|----------|-----------------------------------------------------------------------------------------------------------------------|
| url                        | Yes      |         | String   | JDBC url, database name is also required here                                                                         |
| table-name                 | Yes      |         | String   | Table name                                                                                                            |
| username                   | Yes      |         | String   | User name                                                                                                             |
| password                   | Yes      |         | String   | Password                                                                                                              |
| driver-class               | Yes      |         | String   | JDBC driver class name, like 'com.mysql.jdbc.Driver'                                                                  |
| connection-pool            | Yes      |         | String   | Database connection pool type, can be 'druid' or 'hikari'                                                             |
| connection-pool-properties | No       |         | String   | Database connection pool properties, need to correspond to pool type, and multiple values are separated by semicolons |
| upsert-mode                | No       | true    | Boolean  | Whether to use upsert mode                                                                                            |
| buffer-flush.interval      | No       | 1s      | Duration | Buffer flush interval                                                                                                 |
| buffer-flush.buffer-size   | No       | 1000    | Integer  | Buffer size                                                                                                           |
| buffer-flush.batch-size    | No       | 100     | Integer  | Buffer flush batch size                                                                                               |
| max-retries                | No       | 3       | Integer  | Max retry times on failure                                                                                            |

## Community

Don’t hesitate to ask!

Contact the developers and community at [https://ask.oceanbase.com](https://ask.oceanbase.com) if you need any help.

[Open an issue](https://github.com/oceanbase/flink-connector-oceanbase/issues) if you found a bug.

## Licensing

See [LICENSE](LICENSE) for more information.

## References

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)
