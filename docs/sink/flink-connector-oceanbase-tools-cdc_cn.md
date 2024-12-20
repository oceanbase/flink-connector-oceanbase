# Flink Connector OceanBase By Tools CDC

[English](flink-connector-oceanbase-tools-cdc.md) | 简体中文

本项目是一个可以支持通过Flink命令行构建cdc任务同步到oceanbase的flink命令行工具，极大的简化了通过flink同步数据到oceanbase的命令书写。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbas-directload) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-tools-cdc</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-tools-cdc</artifactId>
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

### 注意事项：

* 目前项目支持使用Flink CDC接入多表或整库。同步时需要在 `$FLINK_HOME/lib` 目录下添加对应的 Flink CDC 依赖，比如 flink-sql-connector-mysql-cdc-\${version}.jar，flink-sql-connector-oracle-cdc-\${version}.jar ，flink-sql-connector-sqlserver-cdc-\${version}.jar
* 依赖的 Flink CDC 版本需要在 3.1 以上，如果需使用 Flink CDC 同步 MySQL 和 Oracle，还需要在 `$FLINK_HOME/lib` 下增加相关的 JDBC 驱动。
* 同步至oceanbase时，oceanbase的url连接串需要使用mysql的协议。

### MySQL同步至OceanBase示例

#### 准备

在 MySql 数据库test_db库中创建表 test_history_strt_sink，test_history_text。

```mysql
use test_db;
CREATE TABLE test_history_str (
	itemid                   bigint                                    NOT NULL,
	clock                    integer         DEFAULT '0'               NOT NULL,
	value                    varchar(255)    DEFAULT ''                NOT NULL,
	ns                       integer         DEFAULT '0'               NOT NULL,
	PRIMARY KEY (itemid,clock,ns)
);
CREATE TABLE test_history_text (
	itemid                   bigint                                    NOT NULL,
	clock                    integer         DEFAULT '0'               NOT NULL,
	value                    text                                      NOT NULL,
	ns                       integer         DEFAULT '0'               NOT NULL,
	PRIMARY KEY (itemid,clock,ns)
);
```

#### 构建Flink任务

##### Flink命令行示例

```shell
$FLIN
K_HOME/bin/flink run \
    -Dexecution.checkpointing.interval=10s \
    -Dparallelism.default=1 \
    -c com.oceanbase.connector.flink.tools.cdc.CdcTools \
    lib/flink-connector-oceanbase-tools-cdc-${version}.jar \
    mysql-sync-database \
    --database test_db \
    --mysql-conf hostname=xxxx \
    --mysql-conf port=3306 \
    --mysql-conf username=root \
    --mysql-conf password=xxxx \
    --mysql-conf database-name=test_db \
    --including-tables "tbl1|test.*" \
    --sink-conf username=xxxx \
    --sink-conf password=xxxx \
    --sink-conf url=jdbc:mysql://xxxx:xxxx
```

请将以上的数据库信息替换为您真实的数据库信息，当出现类似于以下的信息时，任务构建成功并提交。

```shell
Job has been submitted with JobID 0177b201a407045a17445aa288f0f111
```

工具会自动解析命令行的信息并进行建表，可在OceanBase 中查询验证。

MySQl中插入测试数据

```sql
INSERT INTO test_db.test_history_str (itemid,clock,value,ns) VALUES
	 (1,2,'ces1',1123);
INSERT INTO test_db.test_history_text (itemid,clock,value,ns) VALUES
	 (1,21131,'ces1',21321),
	 (2,21321,'ces2',12321);
```

由于是CDC任务，MySQL中插入数据后，即可在 OceanBase 中查询验证同步过来的数据。

### 参数解析

该配置是flink的程序配置信息

```shell
-Dexecution.checkpointing.interval=10s
-Dparallelism.default=1
```

指定程序的jar包和程序的入口

```shell
-c com.oceanbase.connector.flink.tools.cdc.CdcTools \
lib/flink-connector-oceanbase-tools-cdc-${version}.jar \
```

数据库名称

```shell
--database test_db
```

这个名称是自定义的，意为给这个数据库取的名字，最终作为flink任务的命名规则。

## 配置项

#### 支持的数据源

|          数据源标识          |     数据源      |
|-------------------------|--------------|
| mysql-sync-database     | mysql数据源     |
| oracle-sync-database    | oracle数据源    |
| postgres-sync-database  | postgres数据源  |
| sqlserver-sync-database | sqlserver数据源 |
| db2-sync-database       | db2数据源       |

#### 配置项

|          配置项           |                                                                                                                                  描述                                                                                                                                  |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| --job-name             | Flink 任务名称，非必需。                                                                                                                                                                                                                                                      |
| --database             | 同步到 OceanBase 的数据库名。                                                                                                                                                                                                                                                 |
| --table-prefix         | OceanBase表前缀名，例如 --table-prefix ods_。                                                                                                                                                                                                                                |
| --table-suffix         | 同上，OceanBase表的后缀名。                                                                                                                                                                                                                                                   |
| --including-tables     | 需要同步的 MySQL 表，可以使用｜分隔多个表，并支持正则表达式。比如--including-tables table1。                                                                                                                                                                                                       | `分隔多个表，并支持正则表达式。比如--including-tables table1 |
| --excluding-tables     | 不需要同步的表，用法同上。                                                                                                                                                                                                                                                        |
| --mysql-conf           | MySQL CDCSource 配置，其中 hostname/username/password/database-name 是必需的。同步的库表中含有非主键表时，必须设置scan.incremental.snapshot.chunk.key-column，且只能选择非空类型的一个字段。<br/>例如：`scan.incremental.snapshot.chunk.key-column=database.table:column,database.table1:column...`，不同的库表列之间用`,`隔开。 |
| --oracle-conf          | Oracle CDCSource 配置，其中 hostname/username/password/database-name/schema-name 是必需的。                                                                                                                                                                                    |
| --postgres-conf        | Postgres CDCSource 配置，其中 hostname/username/password/database-name/schema-name/slot.name 是必需的。                                                                                                                                                                        |
| --sqlserver-conf       | SQLServer CDCSource 配置，其中 hostname/username/password/database-name/schema-name 是必需的。                                                                                                                                                                                 |
| --db2-conf             | SQLServer CDCSource 配置，其中 hostname/username/password/database-name/schema-name 是必需的。                                                                                                                                                                                 |
| --sink-conf            | 见下面--sink-conf的配置项。                                                                                                                                                                                                                                                  |
| --ignore-default-value | 关闭同步 MySQL 表结构的默认值。适用于同步 MySQL 数据到 OceanBase 时，字段有默认值，但实际插入数据为 null 情况。                                                                                                                                                                                              |
| --create-table-only    | 是否只仅仅同步表的结构。                                                                                                                                                                                                                                                         |

#### sink-conf的配置项

|   配置项    | 默认值 | 是否需要 |                   描述                    |
|----------|-----|------|-----------------------------------------|
| url      | --  | N    | jdbc 连接信息，如：jdbc:mysql://127.0.0.1:2881 |
| username | --  | Y    | 访问 oceanbase 的用户名                       |
| password | --  | Y    | 访问 oceanbase 的密码                        |

## 参考信息

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector](https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector)

