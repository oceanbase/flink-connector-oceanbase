# Flink Connector OceanBase

[English](flink-connector-oceanbase.md) | 简体中文

本项目是一个 OceanBase 的 Flink Connector，可以在 Flink 中通过 JDBC 驱动将数据写入到 OceanBase。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

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

您也可以通过源码构建的方式获得程序包。

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### SQL JAR

要直接通过 Flink SQL 使用此连接器，您需要下载名为`flink-sql-connector-oceanbase-${project.version}.jar`的包含所有依赖的 jar 文件：
- 正式版本：https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-oceanbase
- 快照版本：https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-oceanbase

### 示例

#### 准备

在 OceanBase 数据库 MySQL 模式下的 test 库中创建目的表 t_sink。

```mysql
USE test;
CREATE TABLE `t_sink`
(
    `id`       int(10) NOT NULL,
    `username` varchar(20) DEFAULT NULL,
    `score`    int(10)     DEFAULT NULL,
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
                        + "    'url' = 'jdbc:oceanbase://127.0.0.1:2881/test',"
                        + "    'table-name' = 't_sink',"
                        + "    'username' = 'root@test',"
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
)
with (
    'connector' = 'oceanbase',
    'url' = 'jdbc:oceanbase://127.0.0.1:2881/test',
    'cluster-name' = 'obcluster',
    'tenant-name' = 'test',
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

插入测试数据

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

执行完成后，即可在 OceanBase 中检索验证。

## 配置项

|            参数名             | 是否必需 |  默认值  |    类型    |                            描述                             |
|----------------------------|------|-------|----------|-----------------------------------------------------------|
| url                        | 是    |       | String   | 数据库的 JDBC url, 应当以 'jdbc:oceanbase://' 作为前缀               |
| schema-name                | 是    |       | String   | 连接的 Schema 名或库名                                           |
| table-name                 | 是    |       | String   | 表名                                                        |
| username                   | 是    |       | String   | 连接用户名                                                     |
| password                   | 是    |       | String   | 连接密码                                                      |
| compatible-mode            | 是    |       | String   | 兼容模式，可以是 'mysql' 或 'oracle'                               |
| cluster-name               | 否    |       | String   | 集群名，开启分区计算功能时为必填                                          |
| tenant-name                | 否    |       | String   | 租户名，开启分区计算功能时为必填                                          |
| connection-pool-properties | 否    |       | String   | Druid 连接池属性，多个值用分号分隔                                      |
| upsert-mode                | 否    | true  | Boolean  | 是否使用 upsert 模式                                            |
| buffer-flush.interval      | 否    | 1s    | Duration | 缓冲区刷新周期                                                   |
| buffer-flush.buffer-size   | 否    | 1000  | Integer  | 缓冲区大小                                                     |
| buffer-flush.batch-size    | 否    | 100   | Integer  | 刷新批量数据的批大小                                                |
| max-retries                | 否    | 3     | Integer  | 失败重试次数                                                    |
| memstore-check.enabled     | 否    | true  | Boolean  | 是否开启内存检查                                                  |
| memstore-check.threshold   | 否    | 0.9   | Double   | 内存使用的阈值相对最大限制值的比例                                         |
| memstore-check.interval    | 否    | 30s   | Duration | 内存使用检查周期                                                  |
| partition.enabled          | 否    | false | Boolean  | 是否启用分区计算功能,按照分区来写数据                                       |
| partition.number           | 否    | 1     | Integer  | 该表的分区数，当 'partition.enabled' 设为 true 时，将使用该数量的线程根据分区并行写数据 |

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)
