## Flink Connector OBKV HBase

[English](flink-connector-obkv-hbase.md) | 简体中文

本项目是一个 OBKV HBase 的 Flink Connector，可以在 Flink 中通过 [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java) 将数据写入到 OceanBase。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-obkv-hbase) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
  <groupId>com.oceanbase</groupId>
  <artifactId>flink-connector-obkv-hbase</artifactId>
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

要直接通过 Flink SQL 使用此连接器，您需要下载名为`flink-sql-connector-obkv-hbase-${project.version}.jar`的包含所有依赖的 jar 文件：
- 正式版本：https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-obkv-hbase
- 快照版本：https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-obkv-hbase

### 示例

在 OceanBase 中创建一张表 `htable1$family1`, 该表的名称中， `htable1` 对应 HBase 的表名，`family1` 对应 HBase 中的 column family。

```mysql
use test;
CREATE TABLE `htable1$family1`
(
    `K` varbinary(1024)    NOT NULL,
    `Q` varbinary(256)     NOT NULL,
    `T` bigint(20)         NOT NULL,
    `V` varbinary(1048576) NOT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
)
```

#### Java 应用示例

以 Maven 项目为例，将需要的依赖加入到应用的 pom.xml 文件中，然后使用以下代码

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
                        + " rowkey STRING,"
                        + " family1 ROW<column1 STRING, column2 STRING>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='obkv-hbase',"
                        + "  'url'='http://127.0.0.1:8080/services?...&database=test',"
                        + "  'table-name'='htable1',"
                        + "  'username'='root@test',"
                        + "  'password'='',"
                        + "  'sys.username'='root',"
                        + "  'sys.password'=''"
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO t_sink VALUES "
                                + "('1', ROW('r1f1c1', 'r1f1c2')),"
                                + "('2', ROW('r2f1c1', 'r2f1c2')),"
                                + "('2', ROW('r3f1c1', 'r3f1c2'));")
                .await();
    }
}
```

执行完成后，即可在 OceanBase 中检索验证。

更多信息请参考 [OBKVHBaseConnectorITCase.java](../../flink-connector-obkv-hbase/src/test/java/com/oceanbase/connector/flink/OBKVHBaseConnectorITCase.java)。

#### Flink SQL 示例

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。

```sql
CREATE TABLE t_sink
(
    rowkey STRING,
    family1 ROW <column1 STRING,
    column2 STRING >,
    PRIMARY KEY (rowkey) NOT ENFORCED
)
with (
    'connector'='obkv-hbase',
    'url'='http://127.0.0.1:8080/services?...&database=test',
    'table-name'='htable1',
    'username'='root@test',
    'password'='',
    'sys.username'='root',
    'sys.password'='');
```

插入测试数据

```sql
INSERT INTO t_sink
VALUES ('1', ROW ('r1f1c1', 'r1f1c2')),
       ('2', ROW ('r2f1c1', 'r2f1c2')),
       ('2', ROW ('r3f1c1', 'r3f1c2'));
```

执行完成后，即可在 OceanBase 中检索验证。

## 配置项

|           参数名            | 是否必需 | 默认值  |    类型    |             描述              |
|--------------------------|------|------|----------|-----------------------------|
| url                      | 是    |      | String   | 集群的 config url，需要带 database |
| table-name               | 是    |      | String   | HBase 表名                    |
| username                 | 是    |      | String   | 用户名                         |
| password                 | 是    |      | String   | 密码                          |
| sys.username             | 是    |      | String   | sys 租户的用户名                  |
| sys.password             | 是    |      | String   | sys 租户用户的密码                 |
| buffer-flush.interval    | 否    | 1s   | Duration | 缓冲区刷新周期                     |
| buffer-flush.buffer-size | 否    | 1000 | Integer  | 缓冲区大小                       |
| buffer-flush.batch-size  | 否    | 100  | Integer  | 刷新批量数据的批大小                  |
| max-retries              | 否    | 3    | Integer  | 失败重试次数                      |

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-hbase](https://github.com/apache/flink-connector-hbase)

[https://github.com/oceanbase/obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java)

