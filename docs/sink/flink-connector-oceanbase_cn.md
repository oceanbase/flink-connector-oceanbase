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

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数名</th>
                <th class="text-left" style="width: 8%">Table API 必需</th>
                <th class="text-left" style="width: 7%">DataStream 必需</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>url</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>数据库的 JDBC url。</td>
            </tr>
            <tr>
                <td>username</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接用户名。</td>
            </tr>
            <tr>
                <td>password</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接密码。</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td>是</td>
                <td>不支持</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接的 schema 名或 db 名。</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>是</td>
                <td>不支持</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>表名。</td>
            </tr>
            <tr>
                <td>driver-class-name</td>
                <td>否</td>
                <td>否</td>
                <td>com.mysql.cj.jdbc.Driver</td>
                <td>String</td>
                <td>驱动类名，默认为 'com.mysql.cj.jdbc.Driver'，如果设置了其他值，需要手动引入对应的依赖。</td>
            </tr>
            <tr>
                <td>druid-properties</td>
                <td>否</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Druid 连接池属性，多个值用分号分隔。</td>
            </tr>
            <tr>
                <td>sync-write</td>
                <td>否</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否开启同步写，设置为 true 时将不使用 buffer 直接写入数据库。</td>
            </tr>
            <tr>
                <td>buffer-flush.interval</td>
                <td>否</td>
                <td>否</td>
                <td>1s</td>
                <td>Duration</td>
                <td>缓冲区刷新周期。设置为 '0' 时将关闭定期刷新。</td>
            </tr>
            <tr>
                <td>buffer-flush.buffer-size</td>
                <td>否</td>
                <td>否</td>
                <td>1000</td>
                <td>Integer</td>
                <td>缓冲区大小。</td>
            </tr>
            <tr>
                <td>max-retries</td>
                <td>否</td>
                <td>否</td>
                <td>3</td>
                <td>Integer</td>
                <td>失败重试次数。</td>
            </tr>
            <tr>
                <td>memstore-check.enabled</td>
                <td>否</td>
                <td>否</td>
                <td>true</td>
                <td>Boolean</td>
                <td>是否开启内存检查。</td>
            </tr>
            <tr>
                <td>memstore-check.threshold</td>
                <td>否</td>
                <td>否</td>
                <td>0.9</td>
                <td>Double</td>
                <td>内存使用的阈值相对最大限制值的比例。</td>
            </tr>
            <tr>
                <td>memstore-check.interval</td>
                <td>否</td>
                <td>否</td>
                <td>30s</td>
                <td>Duration</td>
                <td>内存使用检查周期。</td>
            </tr>
            <tr>
                <td>partition.enabled</td>
                <td>否</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否启用分区计算功能，按照分区来写数据。仅当 'sync-write' 和 'direct-load.enabled' 都为 false 时生效。</td>
            </tr>
             <tr>
                <td>table.oracle-tenant-case-insensitive</td>
                <td>否</td>
                <td>否</td>
                <td>true</td>
                <td>Boolean</td>
                <td>默认情况下，在 Oracle 租户下，Schema名和列名不区分大小写。</td>
            </tr>
        </tbody>
    </table>
</div>

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)

[https://github.com/oceanbase/obconnector-j](https://github.com/oceanbase/obconnector-j)
