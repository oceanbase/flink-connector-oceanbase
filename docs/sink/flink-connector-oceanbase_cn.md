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

本项目内置了 MySQL 驱动 8.0.28，对于想使用 OceanBase JDBC 驱动的 OceanBase 数据库企业版的用户，需要手动引入以下依赖：

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">依赖名称</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client/2.4.9">com.oceanbase:oceanbase-client:2.4.9</a></td>
        <td>用于连接到 OceanBase 数据库企业版。</td>
      </tr>
    </tbody>
</table>
</div>

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
    'schema-name' = 'test',
    'table-name' = 't_sink',
    'username' = 'root@test#obcluster',
    'password' = 'pswd',
    'druid-properties' = 'druid.initialSize=10;druid.maxActive=100;',
    'buffer-flush.interval' = '1s',
    'buffer-flush.buffer-size' = '5000',
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

对于 OceanBase 数据库企业版的用户，需要指定 OceanBase JDBC 驱动对应的 `url` 和 `driver-class-name`。

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

## 配置项

|              参数名               | Table API 必需 | DataStream 必需 |           默认值            |    类型    |                                                                                                           描述                                                                                                           |
|--------------------------------|--------------|---------------|--------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                            | 是            | 是             |                          | String   | 数据库的 JDBC url。                                                                                                                                                                                                         |
| username                       | 是            | 是             |                          | String   | 连接用户名。                                                                                                                                                                                                                 |
| password                       | 是            | 是             |                          | String   | 连接密码。                                                                                                                                                                                                                  |
| schema-name                    | 是            | 不支持           |                          | String   | 连接的 schema 名或 db 名。                                                                                                                                                                                                    |
| table-name                     | 是            | 不支持           |                          | String   | 表名。                                                                                                                                                                                                                    |
| driver-class-name              | 否            | 否             | com.mysql.cj.jdbc.Driver | String   | 驱动类名，默认为 'com.mysql.cj.jdbc.Driver'，如果设置了其他值，需要手动引入对应的依赖。                                                                                                                                                              |
| druid-properties               | 否            | 否             |                          | String   | Druid 连接池属性，多个值用分号分隔。                                                                                                                                                                                                  |
| sync-write                     | 否            | 否             | false                    | Boolean  | 是否开启同步写，设置为 true 时将不使用 buffer 直接写入数据库。                                                                                                                                                                                 |
| buffer-flush.interval          | 否            | 否             | 1s                       | Duration | 缓冲区刷新周期。设置为 '0' 时将关闭定期刷新。                                                                                                                                                                                              |
| buffer-flush.buffer-size       | 否            | 否             | 1000                     | Integer  | 缓冲区大小。                                                                                                                                                                                                                 |
| max-retries                    | 否            | 否             | 3                        | Integer  | 失败重试次数。                                                                                                                                                                                                                |
| memstore-check.enabled         | 否            | 否             | true                     | Boolean  | 是否开启内存检查。                                                                                                                                                                                                              |
| memstore-check.threshold       | 否            | 否             | 0.9                      | Double   | 内存使用的阈值相对最大限制值的比例。                                                                                                                                                                                                     |
| memstore-check.interval        | 否            | 否             | 30s                      | Duration | 内存使用检查周期。                                                                                                                                                                                                              |
| partition.enabled              | 否            | 否             | false                    | Boolean  | 是否启用分区计算功能，按照分区来写数据。仅当 'sync-write' 和 'direct-load.enabled' 都为 false 时生效。                                                                                                                                              |
| direct-load.enabled            | 否            | 否             | false                    | Boolean  | 是否开启旁路导入。需要注意旁路导入需要将 sink 的并发度设置为1。                                                                                                                                                                                    |
| direct-load.host               | 否            | 否             |                          | String   | 旁路导入使用的域名或 IP 地址，开启旁路导入时为必填项。                                                                                                                                                                                          |
| direct-load.port               | 否            | 否             | 2882                     | Integer  | 旁路导入使用的 RPC 端口，开启旁路导入时为必填项。                                                                                                                                                                                            |
| direct-load.parallel           | 否            | 否             | 8                        | Integer  | 旁路导入任务的并发度。                                                                                                                                                                                                            |
| direct-load.max-error-rows     | 否            | 否             | 0                        | Long     | 旁路导入任务最大可容忍的错误行数目。                                                                                                                                                                                                     |
| direct-load.dup-action         | 否            | 否             | REPLACE                  | STRING   | 旁路导入任务中主键重复时的处理策略。可以是 'STOP_ON_DUP'（本次导入失败），'REPLACE'（替换）或 'IGNORE'（忽略）。                                                                                                                                               |
| direct-load.timeout            | 否            | 否             | 7d                       | Duration | 旁路导入任务的超时时间。                                                                                                                                                                                                           |
| direct-load.heartbeat-timeout  | 否            | 否             | 60s                      | Duration | 旁路导入任务客户端的心跳超时时间。                                                                                                                                                                                                      |
| direct-load.heartbeat-interval | 否            | 否             | 10s                      | Duration | 旁路导入任务客户端的心跳间隔时间。                                                                                                                                                                                                      |
| direct-load.load-method        | 否            | 否             | full                     | String   | 旁路导入导入模式：full、inc、inc_replace。full：全量旁路导入，默认值。inc：普通增量旁路导入，会进行主键冲突检查，observer-4.3.2及以上支持，暂时不支持dupAction为REPLACE。inc_replace: 特殊replace模式的增量旁路导入，不会进行主键冲突检查，直接覆盖旧数据（相当于replace的效果），dupAction参数会被忽略，observer-4.3.2及以上支持。 |

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)
