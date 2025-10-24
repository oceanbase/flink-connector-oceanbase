## Flink Connector OBKV HBase2

[English](flink-connector-obkv-hbase2.md) | 简体中文

本项目是一个 OBKV HBase 的 Flink Connector，提供扁平表结构和使用方式，可以在 Flink 中通过 [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java) 将数据写入到 OceanBase。

## 核心特性

- **扁平表结构**：使用简洁的扁平表结构定义，无需嵌套 ROW 类型
- **动态列支持**：支持动态列模式，灵活处理未知列名的场景
- **部分列更新**：支持忽略空值和排除指定列的更新
- **时间戳控制**：支持为不同列设置不同的时间戳
- **批量写入**：支持缓冲和批量刷新提升性能

## 与 [flink-connector-obkv-hbase](./flink-connector-obkv-hbase_cn.md) 的区别

### [flink-connector-obkv-hbase](./flink-connector-obkv-hbase_cn.md) (嵌套结构)

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  family1 ROW <column1 STRING, column2 STRING>,  -- 嵌套 ROW 结构
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  ...
);
```

### flink-connector-obkv-hbase2 (扁平结构)

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,       -- 扁平结构，直接定义列
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'columnFamily' = 'f',  -- 通过配置指定列族
  'schema-name' = 'test',
  'table-name' = 'htable1',
  ...
);
```

## 开始使用

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-obkv-hbase2) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase2</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase2</artifactId>
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

要直接通过 Flink SQL 使用此连接器，您需要下载名为 `flink-sql-connector-obkv-hbase2-${project.version}.jar` 的包含所有依赖的 jar 文件：

- 正式版本：https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-obkv-hbase2
- 快照版本：https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-obkv-hbase2

## 使用示例

### 准备

在 OceanBase 中创建一张表 `htable1$f`，该表的名称中，`htable1` 对应 HBase 的表名，`f` 对应 HBase 中的 column family。

```mysql
use test;
CREATE TABLE `htable1$f`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);
```

### Flink SQL 示例

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。

#### 1. 基本使用（Config URL 连接）

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456'
);

-- 插入数据
INSERT INTO t_sink VALUES ('row1', 'value1', 'value2');
INSERT INTO t_sink VALUES ('row2', 'value3', 'value4');
```

#### 2. 使用 ODP 连接

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'odp-mode' = 'true',
  'odp-ip' = '127.0.0.1',
  'odp-port' = '2885',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321'
);
```

#### 3. 部分列更新

连接器提供了灵活的部分列更新能力，支持通过不同方式控制哪些列需要更新：

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  column1 STRING,
  column2 STRING,
  column3 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'ignoreNullWhenUpdate' = 'true',  -- 跳过 NULL 列，实现部分列更新
  'excludeUpdateColumns' = 'column3'  -- 永久排除 column3，不更新此列
);

-- 只更新 column1，column2 和 column3 保持不变
INSERT INTO t_sink (rowkey, column1) VALUES ('1', 'new_value');
```

**两种更新控制方式：**
- `ignoreNullWhenUpdate=true`：跳过值为 NULL 的列，灵活实现部分列更新
- `excludeUpdateColumns`：永久排除指定列，这些列永远不会被更新

#### 4. 动态列模式

动态列模式允许你在运行时动态指定列名，适用于列名不固定的场景。

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  columnKey STRING,    -- 动态列名
  columnValue STRING,  -- 动态列值
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'dynamicColumnSink' = 'true'  -- 启用动态列模式
);

-- 插入动态列数据
INSERT INTO t_sink VALUES ('row1', 'dynamic_col_1', 'value1');
INSERT INTO t_sink VALUES ('row1', 'dynamic_col_2', 'value2');
```

#### 5. 时间戳控制

##### 5.1 使用 tsColumn 为所有列指定统一时间戳

**注意**：时间戳列默认不会被写入 HBase，只用于控制 HBase 版本时间。如果需要将时间戳列也作为数据列存储，请不要在 `tsColumn` 或 `tsMap` 中配置该列。

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  ts_col BIGINT,       -- 时间戳列（不会被写入 HBase）
  column1 STRING,
  column2 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'tsColumn' = 'ts_col',  -- 使用 ts_col 作为所有列的时间戳
  'tsInMills' = 'true'    -- 时间戳单位为毫秒
);
-- 结果：HBase 中只有 2 列：column1, column2（ts_col 自动被排除）
```

##### 5.2 使用 tsMap 为不同列指定不同时间戳

**注意**：时间戳列默认不会被写入 HBase，只用于控制 HBase 版本时间。

```sql
CREATE TABLE t_sink (
  rowkey STRING,
  ts_col1 BIGINT,      -- 时间戳列1（不会被写入 HBase）
  ts_col2 BIGINT,      -- 时间戳列2（不会被写入 HBase）
  column1 STRING,
  column2 STRING,
  column3 STRING,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase2',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'columnFamily' = 'f',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'tsMap' = 'ts_col1:column1;ts_col1:column2;ts_col2:column3',  -- column1和column2使用ts_col1，column3使用ts_col2
  'tsInMills' = 'true'
);
-- 结果：HBase 中只有 3 列：column1, column2, column3（ts_col1 和 ts_col2 自动被排除）
```

## 配置项

|         参数名          | 是否必需 |  默认值  |    类型    |                                                                     描述                                                                     |
|----------------------|------|-------|----------|--------------------------------------------------------------------------------------------------------------------------------------------|
| connector            | 是    |       | String   | 必须设置为 'obkv-hbase2' 以使用此连接器。                                                                                                               |
| schema-name          | 是    |       | String   | OceanBase 的 database 名。                                                                                                                    |
| table-name           | 是    |       | String   | HBase 表名（不含列族后缀）。                                                                                                                          |
| username             | 是    |       | String   | 用户名。                                                                                                                                       |
| password             | 是    |       | String   | 密码。                                                                                                                                        |
| odp-mode             | 否    | false | Boolean  | 是否通过 ODP 连接到 OBKV。设置为 'true' 时通过 ODP 连接，否则通过 config url 直连。                                                                                |
| url                  | 否    |       | String   | 集群的 config url，可以通过 `SHOW PARAMETERS LIKE 'obconfig_url'` 查询。当 'odp-mode' 为 'false' 时必填。                                                   |
| sys.username         | 否    |       | String   | sys 租户的用户名，当 'odp-mode' 为 'false' 时必填。                                                                                                     |
| sys.password         | 否    |       | String   | sys 租户用户的密码，当 'odp-mode' 为 'false' 时必填。                                                                                                    |
| odp-ip               | 否    |       | String   | ODP 的 IP 地址，当 'odp-mode' 为 'true' 时必填。                                                                                                     |
| odp-port             | 否    | 2885  | Integer  | ODP 的 RPC 端口，当 'odp-mode' 为 'true' 时可选。                                                                                                    |
| hbase.properties     | 否    |       | String   | 配置 'obkv-hbase-client-java' 的属性，多个值用分号分隔，格式：'key1=value1;key2=value2'。                                                                     |
| columnFamily         | 否    | f     | String   | HBase 列族名称。                                                                                                                                |
| rowkeyDelimiter      | 否    | :     | String   | 复合主键的分隔符。                                                                                                                                  |
| writePkValue         | 否    | false | Boolean  | 是否将主键值也写入 HBase 列值中。                                                                                                                       |
| bufferSize           | 否    | 5000  | Integer  | 批量写入的缓冲区大小。                                                                                                                                |
| flushIntervalMs      | 否    | 2000  | Duration | 批量刷新的时间间隔（毫秒）。设置为 '0' 时将关闭定期刷新。                                                                                                            |
| ignoreNullWhenUpdate | 否    | true  | Boolean  | 是否忽略空值更新。设置为 'true' 时，跳过值为 null 的列，实现部分列更新；设置为 'false' 时，会将 null 值写入 HBase。                                                                |
| ignoreDelete         | 否    | false | Boolean  | 是否忽略删除操作。设置为 'true' 时，不会执行删除操作。                                                                                                            |
| excludeUpdateColumns | 否    |       | String   | 排除更新的列名，多个列用逗号分隔。这些列不会被更新。                                                                                                                 |
| dynamicColumnSink    | 否    | false | Boolean  | 是否启用动态列模式。启用后，非主键列必须恰好为 2 列（columnKey 和 columnValue），都必须是 VARCHAR 类型。                                                                      |
| tsColumn             | 否    |       | String   | 时间戳列名。指定后，该列的值将作为所有列的时间戳。如果设置了此项，'tsMap' 将被忽略。                                                                                             |
| tsMap                | 否    |       | String   | 时间戳映射配置，格式：'tsColumn0:column0;tsColumn0:column1;tsColumn1:column2'，表示 column0 和 column1 使用 tsColumn0 的值作为时间戳，column2 使用 tsColumn1 的值作为时间戳。 |
| tsInMills            | 否    | true  | Boolean  | 时间戳的单位是否为毫秒。设置为 'false' 时，时间戳单位为秒。                                                                                                         |

## 核心功能

本连接器提供以下核心功能：

- ✅ 扁平表结构定义
- ✅ 动态列支持
- ✅ 部分列更新（ignoreNull, excludeUpdateColumns）
- ✅ 时间戳控制（tsColumn, tsMap）
- ✅ 批量写入和缓冲
- ❌ Source 功能（本连接器仅支持 Sink）
- ❌ Lookup Join 功能（本连接器仅支持 Sink）

## 注意事项

1. **表结构要求**：Flink 表必须定义主键（PRIMARY KEY）。
2. **动态列模式**：启用动态列模式时，非主键列必须恰好为 2 列，且都必须是 VARCHAR 类型。
3. **时间戳优先级**：如果同时设置了 `tsColumn` 和 `tsMap`，`tsColumn` 优先生效。
4. **OceanBase 表命名**：在 OceanBase 中，HBase 表的实际名称是 `tableName$columnFamily` 格式。
5. **编码方式**：使用 HBase 的 `Bytes.toBytes()` 进行编码，与 OBKV HBase Client SDK 兼容。

## 参考信息

- [OceanBase 官网](https://www.oceanbase.com/)
- [OBKV HBase Client Java](https://github.com/oceanbase/obkv-hbase-client-java)
- [Flink Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/)

