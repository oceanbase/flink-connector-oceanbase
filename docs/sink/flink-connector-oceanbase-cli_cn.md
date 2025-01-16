# Flink Connector OceanBase CLI

[English](flink-connector-oceanbase-cli.md) | 简体中文

本项目是一套 CLI（命令行界面）工具，支持提交 Flink 作业将数据从其他数据源迁移到 OceanBase。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase-cli) 找到正式的发布版本，或者从 [Sonatype Snapshot](https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-connector-oceanbase-cli) 获取最新的快照版本。

您也可以通过源码构建的方式获得程序包。

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### 使用 Flink CDC 作为源端

#### 依赖

本项目基于 [Flink CDC Source 连接器](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/overview/) 的 SQL 客户端 JAR。

本项目的 JAR 包中未包含 Flink CDC Source 连接器，因此您需要手动下载使用的 Flink CDC SQL JAR。请注意，本项目要求 Flink CDC 为 3.2.0 或更高版本。

如果您使用 Flink Oracle CDC 作为源端，您还需要下载源连接器的依赖项，请参阅 [Oracle CDC 连接器](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/oracle-cdc/#sql-client-jar) 的 *依赖项* 章节。

#### 示例：从 Flink MySQL CDC 迁移数据到 OceanBase

#### 准备

将 CLI JAR `flink-connector-oceanbase-cli-xxx.jar` 和依赖 JAR `flink-sql-connector-mysql-cdc-xxx.jar` 添加到 `$FLINK_HOME/lib`。

然后在 MySQL 数据库中准备表和数据。

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

INSERT INTO test_db.test_history_text (itemid,clock,value,ns) VALUES
  (1,21131,'ces1',21321);
```

##### 通过 CLI 提交作业

将以下命令替换为您的真实数据库信息，并执行它以提交 Flink 作业。

```shell
$FLINK_HOME/bin/flink run \
    -Dexecution.checkpointing.interval=10s \
    -Dparallelism.default=1 \
    -c com.oceanbase.connector.flink.CdcCli \
    lib/flink-connector-oceanbase-cli-xxx.jar \
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

##### 检查和验证

检查目标 OceanBase 数据库，你应该找到这两个表和一行数据。

你可以继续将测试数据插入到 MySQL 数据库，如下所示：

```sql
INSERT INTO test_db.test_history_str (itemid,clock,value,ns) VALUES
	 (1,2,'ces1',1123);
INSERT INTO test_db.test_history_text (itemid,clock,value,ns) VALUES
	 (2,21321,'ces2',12321);
```

由于是CDC任务，MySQL中插入数据后，即可在 OceanBase 中查询验证同步过来的数据。

#### 配置项

|          配置项           |                                                                                                                                  描述                                                                                                                                  |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| {identifier}           | 数据源标识，目前经过验证的只有 MySQL 源端 `mysql-sync-database`。                                                                                                                                                                                                                      |
| --job-name             | Flink 任务名称，非必需。                                                                                                                                                                                                                                                      |
| --database             | 同步到 OceanBase 的数据库名。                                                                                                                                                                                                                                                 |
| --table-prefix         | OceanBase表前缀名，例如 --table-prefix ods_。                                                                                                                                                                                                                                |
| --table-suffix         | 同上，OceanBase表的后缀名。                                                                                                                                                                                                                                                   |
| --including-tables     | 需要同步的 MySQL 表，可以使用｜分隔多个表，并支持正则表达式。比如--including-tables table1。                                                                                                                                                                                                       |
| --excluding-tables     | 不需要同步的表，用法同上。                                                                                                                                                                                                                                                        |
| --mysql-conf           | MySQL CDCSource 配置，其中 hostname/username/password/database-name 是必需的。同步的库表中含有非主键表时，必须设置scan.incremental.snapshot.chunk.key-column，且只能选择非空类型的一个字段。<br/>例如：`scan.incremental.snapshot.chunk.key-column=database.table:column,database.table1:column...`，不同的库表列之间用`,`隔开。 |
| --oracle-conf          | Oracle CDCSource 配置，其中 hostname/username/password/database-name/schema-name 是必需的。                                                                                                                                                                                    |
| --postgres-conf        | Postgres CDCSource 配置，其中 hostname/username/password/database-name/schema-name/slot.name 是必需的。                                                                                                                                                                        |
| --sqlserver-conf       | SQLServer CDCSource 配置，其中 hostname/username/password/database-name/schema-name 是必需的。                                                                                                                                                                                 |
| --db2-conf             | SQLServer CDCSource 配置，其中 hostname/username/password/database-name/schema-name 是必需的。                                                                                                                                                                                 |
| --sink-conf            | 见下面--sink-conf的配置项。                                                                                                                                                                                                                                                  |
| --ignore-default-value | 关闭同步 MySQL 表结构的默认值。适用于同步 MySQL 数据到 OceanBase 时，字段有默认值，但实际插入数据为 null 情况。                                                                                                                                                                                              |
| --create-table-only    | 是否只仅仅同步表的结构。                                                                                                                                                                                                                                                         |

`--sink-conf` 配置项：

|   配置项    | 默认值 | 是否需要 |                   描述                    |
|----------|-----|------|-----------------------------------------|
| url      | --  | N    | jdbc 连接信息，如：jdbc:mysql://127.0.0.1:2881 |
| username | --  | Y    | 访问 oceanbase 的用户名                       |
| password | --  | Y    | 访问 oceanbase 的密码                        |

## 参考信息

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector](https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector)

