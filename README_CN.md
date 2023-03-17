# OceanBase Connector for Apache Flink

[English](README.md) | 简体中文

本仓库包含 OceanBase 的 Flink Sink Connector。

## 开始上手

运行环境需要准备

- JDK 8
- Flink 1.15 或后续版本
- 数据库连接池。可选 Alibaba Druid 或 HikariCP
- JDBC 驱动。可选 MySQL JDBC 驱动或 OceanBase JDBC 驱动

您可以在 [Releases](https://github.com/oceanbase/flink-connector-oceanbase/releases) 页面找到正式的发布版本，也可以通过源码构建的方式获得程序包。

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

数据库连接池目前支持 Alibaba Druid 和 HikariCP，您可以根据需要选择其中一种加入到应用系统。

- Druid ：[https://mvnrepository.com/artifact/com.alibaba/druid](https://mvnrepository.com/artifact/com.alibaba/druid)
- HikariCP：[https://mvnrepository.com/artifact/com.zaxxer/HikariCP](https://mvnrepository.com/artifact/com.zaxxer/HikariCP)

需要注意的是，HikariCP 从 5.0.x 开始不再支持 JDK 8，因此这里需要使用 4.0.x 或更早的版本。

OceanBase 数据库的 MySQL 模式兼容了 MySQL 协议，因此可以直接使用 MySQL JDBC 驱动。OceanBase 也提供了官方的 JDBC 驱动，同时支持 OceanBase 的 Oracle 模式和 MySQL 模式。

- MySQL JDBC：[https://mvnrepository.com/artifact/mysql/mysql-connector-java](https://mvnrepository.com/artifact/mysql/mysql-connector-java)
- OceanBase JDBC：[https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client)

### 示例

准备好程序包和 Alibaba Druid、MySQL JDBC 驱动，并在 OceanBase 数据库的 test 库下创建目的表 t_sink。

```mysql
USE test;
CREATE TABLE `t_sink` (
  `id` int(10) NOT NULL,
  `username` varchar(20) DEFAULT NULL,
  `score` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
```

#### Java 应用示例

以 Maven 项目为例，将需要的依赖加入到应用的 pom.xml 文件中，然后使用以下代码。

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

执行完成后，即可在 OceanBase 中检索验证。

#### Flink SQL 示例

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。

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

插入测试数据

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

执行完成后，即可在 OceanBase 中检索验证。

## 配置项

| 参数名                        | 是否必需 | 默认值  | 类型       | 描述                                   |
|----------------------------|------|------|----------|--------------------------------------|
| url                        | 是    |      | String   | 数据库的 JDBC url，需要包含库名                 |
| table-name                 | 是    |      | String   | 表名                                   |
| username                   | 是    |      | String   | 连接用户名                                |
| password                   | 是    |      | String   | 连接密码                                 |
| driver-class               | 是    |      | String   | JDBC 驱动的类名，如 'com.mysql.jdbc.Driver' |
| connection-pool            | 是    |      | String   | 连接池类型，可以是 'druid' 或 'hikari'         |
| connection-pool-properties | 否    |      | String   | 连接池属性，需要根据连接池类型进行配置，多个值用分号分隔         |
| upsert-mode                | 否    | true | Boolean  | 是否使用 upsert 模式                       |
| buffer-flush.interval      | 否    | 1s   | Duration | 缓冲区刷新周期                              |
| buffer-flush.buffer-size   | 否    | 1000 | Integer  | 缓冲区大小                                |
| buffer-flush.batch-size    | 否    | 100  | Integer  | 刷新批量数据的批大小                           |
| max-retries                | 否    | 3    | Integer  | 失败重试次数                               |

## 社区

当你需要帮助时，你可以在 [https://ask.oceanbase.com](https://ask.oceanbase.com) 上找到开发者和其他的社区伙伴。

当你发现项目缺陷时，请在 [issues](https://github.com/oceanbase/flink-connector-oceanbase/issues) 页面创建一个新的 issue。

## 许可证

更多信息见 [LICENSE](LICENSE)。

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)