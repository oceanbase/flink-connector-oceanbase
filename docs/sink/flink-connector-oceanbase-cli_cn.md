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
USE test_db;

CREATE TABLE products
(
  id          INTEGER      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight      FLOAT
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default, "scooter", "Small 2-wheel scooter", 3.14),
       (default, "car battery", "12V car battery", 8.1),
       (default, "12-pack drill bits", "12-pack of drill bits with sizes ranging from #40 to #3", 0.8),
       (default, "hammer", "12oz carpenter's hammer", 0.75),
       (default, "hammer", "14oz carpenter's hammer", 0.875),
       (default, "hammer", "16oz carpenter's hammer", 1.0),
       (default, "rocks", "box of assorted rocks", 5.3),
       (default, "jacket", "water resistent black wind breaker", 0.1),
       (default, "spare tire", "24 inch spare tire", 22.2);

CREATE TABLE customers
(
  id         INTEGER      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name  VARCHAR(255) NOT NULL,
  email      VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT = 1001;

INSERT INTO customers
VALUES (default, "Sally", "Thomas", "sally.thomas@acme.com"),
       (default, "George", "Bailey", "gbailey@foobar.com"),
       (default, "Edward", "Walker", "ed@walker.com"),
       (default, "Anne", "Kretchmar", "annek@noanswer.org");
```

##### 通过 CLI 提交作业

将以下命令替换为您的真实数据库信息，并执行它以提交 Flink 作业。

```shell
$FLINK_HOME/bin/flink run \
    -Dexecution.checkpointing.interval=10s \
    -Dparallelism.default=1 \
    -c com.oceanbase.connector.flink.CdcCli \
    lib/flink-connector-oceanbase-cli-xxx.jar \
    mysql-cdc \
    --database test_db \
    --source-conf hostname=xxxx \
    --source-conf port=3306 \
    --source-conf username=root \
    --source-conf password=xxxx \
    --source-conf database-name=test_db \
    --source-conf table-name=.* \
    --including-tables ".*" \
    --sink-conf username=xxxx \
    --sink-conf password=xxxx \
    --sink-conf url=jdbc:mysql://xxxx:xxxx
```

请将以上的数据库信息替换为您真实的数据库信息，当出现类似于以下的信息时，任务构建成功并提交。

##### 检查和验证

检查目标 OceanBase 数据库，你应该找到这两个表和多行数据。

你可以继续将测试数据插入到 MySQL 数据库，由于是CDC任务，MySQL中插入数据后，即可在 OceanBase 中查询验证同步过来的数据。

#### 配置项

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数</th>
                <th class="text-left" style="width: 5%">是否必需</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 50%">说明</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>${job-type}</td>
                <td>是</td>
                <td>枚举值</td>
                <td style="word-wrap: break-word;"></td>
                <td>任务类型，可以是 <code>mysql-cdc</code>。</td>
            </tr>
            <tr>
                <td>--source-conf</td>
                <td>是</td>
                <td>多值参数</td>
                <td style="word-wrap: break-word;"></td>
                <td>指定类型的 Flink CDC 源端连接器的配置参数。</td>
            </tr>
            <tr>
                <td>--sink-conf</td>
                <td>是</td>
                <td>多值参数</td>
                <td style="word-wrap: break-word;"></td>
                <td>OceanBase 写连接器的配置参数。</td>
            </tr>
            <tr>
                <td>--job-name</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;">${job-type} Sync</td>
                <td>Flink 任务名称。</td>
            </tr>
            <tr>
                <td>--database</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>OceanBase 中目标 db 的名称，不设置时将使用源端的 db 名称。</td>
            </tr>
            <tr>
                <td>--table-prefix</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>OceanBase 中目标表名称的前缀。</td>
            </tr>
            <tr>
                <td>--table-suffix</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>OceanBase 中目标表名称的后缀。</td>
            </tr>
            <tr>
                <td>--including-tables</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>源端表的白名单模式。</td>
            </tr>
            <tr>
                <td>--excluding-tables</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>源端表的黑名单模式。</td>
            </tr>
            <tr>
                <td>--multi-to-one-origin</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>将多个源表映射到一个目标表的源表名称模式，多个值以<code>|</code>分隔。</td>
            </tr>
            <tr>
                <td>--multi-to-one-target</td>
                <td>否</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td><code>--multi-to-one-origin</code>对应的目标表名，多个值之间以<code>|</code>分隔，长度必须等于<code>--multi-to-one-origin</code>。</td>
            </tr>
            <tr>
                <td>--create-table-only</td>
                <td>否</td>
                <td>Boolean</td>
                <td style="word-wrap: break-word;">false</td>
                <td>是否只同步库表结构。</td>
            </tr>
            <tr>
                <td>--ignore-default-value</td>
                <td>否</td>
                <td>Boolean</td>
                <td style="word-wrap: break-word;">false</td>
                <td>是否忽略默认值。</td>
            </tr>
            <tr>
                <td>--ignore-incompatible</td>
                <td>否</td>
                <td>Boolean</td>
                <td style="word-wrap: break-word;">false</td>
                <td>是否忽略不兼容的数据类型。</td>
            </tr>
        </tbody>
    </table>
</div>

