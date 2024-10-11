# Flink Connector OceanBase Direct Load

[English](flink-connector-oceanbase-directload.md) | 简体中文

本项目是一个基于 OceanBase 旁路导入功能的 Flink Connector，可以在 Flink 中通过旁路导入的方式将数据写入到 OceanBase。

关于 OceanBase 的旁路导入功能，见 [obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)项目。

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

### 使用说明：

- 目前旁路导入 Flink Connector 仅支持在Flink Batch执行模式下运行, 参考下列方式启用Flink Batch执行模式。
  - Table-API/Flink-SQL: `SET 'execution.runtime-mode' = 'BATCH';`
  - DataStream API:

    ```
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    ```
- 目前旁路导入 Flink Connector支持单节点写入和多节点写入两种方式：
  - 单节点写入: 此方式下Flink Task有且仅有一个并行度进行写入。适用于中小规模的数据量导入。此方式简单易用，推荐使用。
  - 多节点写入：此方式下可以根据要导入数据量的大小自由调节Flink Task的并行度，以提高写入吞吐量。

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

#### 单节点写入

##### Flink SQL 示例

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。

```sql
SET 'execution.runtime-mode' = 'BATCH';

CREATE TABLE t_sink
(
    id       INT,
    username VARCHAR,
    score    INT,
    PRIMARY KEY (id) NOT ENFORCED
) with (
    'connector' = 'oceanbase-directload',
    'host' = '127.0.0.1',
    'port' = '2882',
    'schema-name' = 'test',
    'table-name' = 't_sink',
    'username' = 'root',
    'tenant-name' = 'test',
    'password' = 'password'
    );
```

插入测试数据

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

执行完成后，即可在 OceanBase 中查询验证。

#### 多节点写入

##### 1、代码中创建旁路导入任务，并获取execution id

- 创建一个Java Maven项目，其中POM文件如下：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.oceanbase.directload</groupId>
    <artifactId>multi-node-write-demo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>com.oceanbase</groupId>
            <artifactId>obkv-table-client</artifactId>
            <version>1.2.13</version>
        </dependency>
        <dependency>
          <groupId>com.alibaba.fastjson2</groupId>
          <artifactId>fastjson2</artifactId>
          <version>2.0.53</version>
        </dependency>
    </dependencies>
</project>
```

- 代码中创建旁路导入任务，并获取execution id

代码示例见下面的完整示例代码。

#### 2、在上述步骤中获取execution id后，提交Flink任务

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。
注意，将`enable-multi-node-write`设为true，同时将`execution-id`设置为上述步骤获取到execution id。

```sql
SET 'execution.runtime-mode' = 'BATCH';
SET 'parallelism.default' = '3';

CREATE TABLE t_sink
(
    id       INT,
    username VARCHAR,
    score    INT,
    PRIMARY KEY (id) NOT ENFORCED
) with (
    'connector' = 'oceanbase-directload',
    'host' = '127.0.0.1',
    'port' = '2882',
    'schema-name' = 'test',
    'table-name' = 't_sink',
    'username' = 'root',
    'tenant-name' = 'test',
    'password' = 'password',
    'enable-multi-node-write' = 'true',
    'execution-id' = '5cIeLwELBIWAxOAKAAAAwhY='
    );
```

插入测试数据

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

#### 3、等待上述提交的Flink任务执行完成，最后在代码中进行旁路导入任务最后的提交动作

代码示例见下面的完整示例代码。

#### 完整的示例代码

```java
public class MultiNode {
    private static String host = "127.0.0.1";
    private static int port = 2882;

    private static String userName = "root";
    private static String tenantName = "test";
    private static String password = "password";
    private static String dbName = "test";
    private static String tableName = "t_sink";

    public static void main(String[] args) throws ObDirectLoadException, IOException, InterruptedException {
        // 1、构建旁路导入任务，并获取execution id。
        ObDirectLoadConnection connection = ObDirectLoadManager.getConnectionBuilder()
                        .setServerInfo(host, port)
                        .setLoginInfo(tenantName, userName, password, dbName)
                        .build();
        ObDirectLoadStatement statement = connection.getStatementBuilder()
                        .setTableName(tableName)
                        .build();
        statement.begin();
        ObDirectLoadStatementExecutionId statementExecutionId =
                statement.getExecutionId();
        byte[] executionIdBytes = statementExecutionId.encode();
        // 将byte[]形式的execution id转换为字符串形式，方便作为参数传递给Flink-SQL作业。
        String executionId = java.util.Base64.getEncoder().encodeToString(executionIdBytes);
        System.out.println(executionId);

        // 2、获取到executionId后，提交Flink SQL作业。

        // 3、命令行输入第二歩提交的Flink作业的id，等待Flink作业完成。
        Scanner scanner = new Scanner((System.in));
        String flinkJobId = scanner.nextLine();

        while (true) {
            // 循环检测Flink作业的运行状态，见：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/
            JSONObject jsonObject = JSON.parseObject(new URL("http://localhost:8081/jobs/" + flinkJobId));
            String status = jsonObject.getString("state");
            if ("FINISHED".equals(status)) {
                break;
            }
            Thread.sleep(3_000);
        }

        // 4、等待Flink作业执行完成后，进行旁路导入任务的最后提交动作。
        statement.commit();

        statement.close();
        connection.close();
    }
}
```

执行完成后，即可在 OceanBase 中查询验证。

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
                <td>用户名。</td>
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
            <tr>
                <td>enable-multi-node-write</td>
                <td>否</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否启用支持多节点写入的旁路导入。默认不开启。</td>
            </tr>
            <tr>
                <td>execution-id</td>
                <td>否</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>旁路导入任务的 execution id。仅当 <code>enable-multi-node-write</code>参数为true时生效。</td>
            </tr>
        </tbody>
    </table>
</div>

## 参考信息

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/)

