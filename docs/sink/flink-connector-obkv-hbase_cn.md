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

#### 准备

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

#### Flink SQL 示例

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。

##### 使用 Config Url 连接

```sql
CREATE TABLE t_sink
(
  rowkey STRING,
  family1 ROW <column1 STRING, column2 STRING >,
  PRIMARY KEY (rowkey) NOT ENFORCED
) with (
  'connector'='obkv-hbase',
  'url'='http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name'='test',
  'table-name'='htable1',
  'username'='root@test#obcluster',
  'password'='654321',
  'sys.username'='root',
  'sys.password'='123456');
```

##### 使用 ODP 连接

```sql
CREATE TABLE t_sink
(
  rowkey STRING,
  family1 ROW <column1 STRING, column2 STRING >,
  PRIMARY KEY (rowkey) NOT ENFORCED
) with (
  'connector'='obkv-hbase',
  'odp-mode'='true',
  'odp-ip'='127.0.0.1',
  'odp-port'='2885',
  'schema-name'='test',
  'table-name'='htable1',
  'username'='root@test#obcluster',
  'password'='654321');
```

##### 写入数据

插入测试数据

```sql
INSERT INTO t_sink
VALUES ('1', ROW ('r1f1c1', 'r1f1c2')),
       ('2', ROW ('r2f1c1', 'r2f1c2')),
       ('2', ROW ('r3f1c1', 'r3f1c2'));
```

执行完成后，即可在 OceanBase 中检索验证。

## 配置项

|           参数名            | 是否必需 |  默认值  |    类型    |                                                 描述                                                  |
|--------------------------|------|-------|----------|-----------------------------------------------------------------------------------------------------|
| schema-name              | 是    |       | String   | OceanBase 的 db 名。                                                                                   |
| table-name               | 是    |       | String   | HBase 表名，注意在 OceanBase 中表名的结构是 <code>hbase_table_name$family_name</code>。                           |
| username                 | 是    |       | String   | 非 sys 租户的用户名。                                                                                       |
| password                 | 是    |       | String   | 非 sys 租户的密码。                                                                                        |
| odp-mode                 | 否    | false | Boolean  | 如果设置为 'true'，连接器将通过 ODP 连接到 OBKV，否则通过 config url 连接。                                                |
| url                      | 否    |       | String   | 集群的 config url，可以通过 <code>SHOW PARAMETERS LIKE 'obconfig_url'</code> 查询。当 'odp-mode' 为 'false' 时必填。 |
| sys.username             | 否    |       | String   | sys 租户的用户名，当 'odp-mode' 为 'false' 时必填。                                                              |
| sys.password             | 否    |       | String   | sys 租户用户的密码，当 'odp-mode' 为 'false' 时必填。                                                             |
| odp-ip                   | 否    |       | String   | ODP 的 IP，当 'odp-mode' 为 'true' 时必填。                                                                 |
| odp-port                 | 否    | 2885  | Integer  | ODP 的 RPC 端口，当 'odp-mode' 为 'true' 时必填。                                                             |
| hbase.properties         | 否    |       | String   | 配置 'obkv-hbase-client-java' 的属性，多个值用分号分隔。                                                           |
| sync-write               | 否    | false | Boolean  | 是否开启同步写，设置为 true 时将不使用 buffer 直接写入数据库。                                                              |
| buffer-flush.interval    | 否    | 1s    | Duration | 缓冲区刷新周期。设置为 '0' 时将关闭定期刷新。                                                                           |
| buffer-flush.buffer-size | 否    | 1000  | Integer  | 缓冲区大小。                                                                                              |
| max-retries              | 否    | 3     | Integer  | 失败重试次数。                                                                                             |

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-hbase](https://github.com/apache/flink-connector-hbase)

[https://github.com/oceanbase/obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java)

