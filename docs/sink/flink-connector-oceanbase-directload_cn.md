# Flink Connector OceanBase Direct Load

[English](flink-connector-oceanbase-directload.md) | 简体中文

本项目是一个基于 OceanBase 旁路导入功能的 Flink Connector，可以在 Flink 中通过旁路导入的方式将数据高效写入到 OceanBase。

## 重要说明

**本连接器专为批处理场景设计，具有以下特点：**

- ✅ **仅支持有界流（Bounded Stream）**：数据源必须是有界的，不支持无界流。推荐使用 Flink Batch 模式以获得更好的性能
- ✅ **高吞吐量写入**：适合大批量数据导入场景，支持多节点并行写入
- ⚠️ **导入期间锁表**：旁路导入执行期间会对目标表加锁，此时该表仅支持查询（SELECT）操作，无法进行写入（INSERT/UPDATE/DELETE）操作
- ⚠️ **不适合实时写入**：如果您需要实时/流式写入无界流场景，请使用 [flink-connector-oceanbase](flink-connector-oceanbase_cn.md) 连接器

关于 OceanBase 的旁路导入功能，见 [旁路导入文档](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbas-directload) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-directload</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-directload</artifactId>
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

要直接通过 Flink SQL 使用此连接器，您需要下载名为`flink-sql-connector-oceanbase-directload-${project.version}.jar`的包含所有依赖的 jar 文件：

- 正式版本：https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-oceanbase-directload
- 快照版本：https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-oceanbase-directload

### 使用前提条件

**数据源必须是有界流（Bounded Stream）**，旁路导入 Connector 不支持无界流。

**推荐使用 Flink Batch 模式**以获得更好的性能：

- **Table API / Flink SQL**：

  ```sql
  SET 'execution.runtime-mode' = 'BATCH';
  ```
- **DataStream API**：

  ```java
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  env.setRuntimeMode(RuntimeExecutionMode.BATCH);
  ```

> **注意**：虽然也可以在 Streaming 模式下使用有界数据源，但 Batch 模式通常能提供更好的性能和资源利用率。

### 性能调优

- **并行度调整**：支持多节点并行写入，可以通过调整 Flink Task 的并行度来提高写入吞吐量

  ```sql
  SET 'parallelism.default' = '8';  -- 根据数据量调整并行度
  ```
- **服务端并行度**：通过 `parallel` 参数配置 OceanBase 服务端处理导入任务的 CPU 资源

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
-- 推荐设置为 BATCH 模式以获得更好性能
SET 'execution.runtime-mode' = 'BATCH';

-- 可选：根据数据量调整并行度以提高吞吐量
SET 'parallelism.default' = '8';

CREATE TABLE t_sink
(
    id       INT,
    username VARCHAR,
    score    INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'oceanbase-directload',
    'host' = '127.0.0.1',
    'port' = '2882',
    'schema-name' = 'test',
    'table-name' = 't_sink',
    'username' = 'root',
    'tenant-name' = 'test',
    'password' = 'password',
    'parallel' = '8'  -- OceanBase 服务端并行度
);
```

插入测试数据：

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (3, 'Alice', 95);
```

执行完成后，即可在 OceanBase 中查询验证。

**注意**：在 `INSERT` 语句执行期间（旁路导入进行中），目标表 `t_sink` 会被锁定，此时只能对该表执行查询操作，无法执行其他写入操作。

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
                <td>host</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>OceanBase数据库的host地址。</td>
            </tr>
            <tr>
                <td>port</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>Integer</td>
                <td>旁路导入使用的RPC端口。</td>
            </tr>
            <tr>
                <td>username</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>数据库用户名，比如 'root'。注意：而不是像 'root@sys' 格式的连接用户名。</td>
            </tr>
            <tr>
                <td>tenant-name</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>租户名。</td>
            </tr>
            <tr>
                <td>password</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>密码。</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>schema名或DB名。</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>是</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>表名。</td>
            </tr>
            <tr>
                <td>parallel</td>
                <td>否</td>
                <td>否</td>
                <td>8</td>
                <td>Integer</td>
                <td>旁路导入服务端的并发度。该参数决定了服务端使用多少cpu资源来处理本次导入任务。</td>
            </tr>
            <tr>
                <td>buffer-size</td>
                <td>否</td>
                <td>否</td>
                <td>1024</td>
                <td>Integer</td>
                <td>一次写入OceanBase的缓冲区大小。</td>
            </tr>
            <tr>
                <td>max-error-rows</td>
                <td>否</td>
                <td>否</td>
                <td>0</td>
                <td>Long</td>
                <td>旁路导入任务最大可容忍的错误行数目。</td>
            </tr>
            <tr>
                <td>dup-action</td>
                <td>否</td>
                <td>否</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>旁路导入任务中主键重复时的处理策略。可以是 <code>STOP_ON_DUP</code>（本次导入失败），<code>REPLACE</code>（替换）或 <code>IGNORE</code>（忽略）。</td>
            </tr>
            <tr>
                <td>timeout</td>
                <td>否</td>
                <td>否</td>
                <td>7d</td>
                <td>Duration</td>
                <td>旁路导入任务的超时时间。</td>
            </tr>
            <tr>
                <td>heartbeat-timeout</td>
                <td>否</td>
                <td>否</td>
                <td>60s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳超时时间。</td>
            </tr>
            <tr>
                <td>heartbeat-interval</td>
                <td>否</td>
                <td>否</td>
                <td>10s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳间隔时间。</td>
            </tr>
            <tr>
                <td>direct-load.load-method</td>
                <td>否</td>
                <td>否</td>
                <td>full</td>
                <td>String</td>
                <td>旁路导入导入模式：<code>full</code>, <code>inc</code>, <code>inc_replace</code>。
                <ul>
                    <li><code>full</code>：全量旁路导入，默认值。</li>
                    <li><code>inc</code>：普通增量旁路导入，会进行主键冲突检查，observer-4.3.2及以上支持，暂时不支持direct-load.dup-action为REPLACE。</li>
                    <li><code>inc_replace</code>: 特殊replace模式的增量旁路导入，不会进行主键冲突检查，直接覆盖旧数据（相当于replace的效果），direct-load.dup-action参数会被忽略，observer-4.3.2及以上支持。</li>
                </ul>
                </td>
            </tr>
        </tbody>
    </table>
</div>

## 参考信息

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/)

