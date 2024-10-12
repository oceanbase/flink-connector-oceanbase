# Flink Connector OceanBase Direct Load

English | [简体中文](flink-connector-oceanbase-directload_cn.md)

This Flink Connector based on the direct-load feature of OceanBase. It can write data to OceanBase through direct-load in Flink.

For OceanBase's direct-load feature, see the [direct-load document](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001375568).

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase-directload).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-oceanbase-directload</artifactId>
    <version>${project.version}</version>
</dependency>
```

If you'd rather use the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

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

You can also manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### SQL JAR

To use this connector through Flink SQL directly, you need to download the shaded jar file named `flink-sql-connector-oceanbase-directload-${project.version}.jar`:

- Release versions: https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-oceanbase-directload
- Snapshot versions: https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-oceanbase-directload

### Instructions for use:

- Currently, the direct-load Flink Connector only supports running in Flink Batch execution mode. Refer to the following method to enable Flink Batch execution mode.
  - Table-API/Flink-SQL: `SET 'execution.runtime-mode' = 'BATCH';`
  - DataStream API:

    ```
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    ```
- Currently, the direct-load Flink Connector supports two modes: single-node write and multi-node write:
  - Single-node write: In this mode, the Flink Task has only one parallelism for writing. It is suitable for small and medium-sized data import. This method is simple and easy to use and is recommended.
  - Multi-node write: In this mode, the parallelism of the Flink Task can be freely adjusted according to the amount of data to be imported to improve the write throughput.

### Demo

#### Preparation

Create the destination table 't_sink' under the 'test' database of the OceanBase MySQL mode.

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

#### single-node write

#### Flink SQL Demo

Put the JAR files of dependencies to the 'lib' directory of Flink, and then create the destination table with Flink SQL through the sql client.

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

Insert records by Flink SQL.

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

Once executed, the records should have been written to OceanBase.

#### Multi-node write

##### 1. Create a direct-load task in the code and obtain the execution id

- Create a Java Maven project with the following POM file:

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

- Create a direct-load task in the code and obtain the execution id

For code examples, see the complete sample code below.

#### 2. After obtaining the execution id in the above steps, submit the Flink task

Put the JAR files of dependencies to the 'lib' directory of Flink, and then create the destination table with Flink SQL through the sql client.

Note, set `enable-multi-node-write` to true and set `execution-id` to the execution id obtained in the above steps.

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

Insert records by Flink SQL.

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (1, 'Tom', 89);
```

#### 3、Wait for the execution of the Flink task submitted above to be completed, and finally perform the final submission action of the direct-load task in the code.

For code examples, see the complete sample code below.

#### Complete sample code

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
        // 1. Create a direct-load task and obtain the execution id.
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
        // Convert the execution id in byte[] form to string form so that it can be passed to the Flink-SQL job as a parameter.
        String executionId = java.util.Base64.getEncoder().encodeToString(executionIdBytes);
        System.out.println(executionId);

        // 2. After obtaining the executionId, submit the Flink SQL job.

        // 3. Enter the id of the Flink job submitted in the second step on the command line and wait for the Flink job to be completed.
        Scanner scanner = new Scanner((System.in));
        String flinkJobId = scanner.nextLine();

        while (true) {
            // Loop to check the running status of Flink jobs, see: https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/
            JSONObject jsonObject = JSON.parseObject(new URL("http://localhost:8081/jobs/" + flinkJobId));
            String status = jsonObject.getString("state");
            if ("FINISHED".equals(status)) {
                break;
            }
            Thread.sleep(3_000);
        }

        // 4. After waiting for the Flink job execution to FINISHED, perform the final submission action of the direct-load task.
        statement.commit();

        statement.close();
        connection.close();
    }
}
```

Once executed, the records should have been written to OceanBase.

## Configuration

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 8%">Required by Table API</th>
                <th class="text-left" style="width: 7%">Required by DataStream</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>host</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Hostname used in direct-load.</td>
            </tr>
            <tr>
                <td>port</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>Integer</td>
                <td>Rpc port number used in direct-load.</td>
            </tr>
            <tr>
                <td>username</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The name of the database user, like 'root'. NOTE: Not the connection username like 'root@sys'.</td>
            </tr>
            <tr>
                <td>tenant-name</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The tenant name.</td>
            </tr>
            <tr>
                <td>password</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The password.</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The schema name or database name.</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>Yes</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The table name.</td>
            </tr>
            <tr>
                <td>parallel</td>
                <td>No</td>
                <td>No</td>
                <td>8</td>
                <td>Integer</td>
                <td>The parallel of the direct-load server. This parameter determines how much CPU resources the server uses to process this import task.</td>
            </tr>
            <tr>
                <td>buffer-size</td>
                <td>No</td>
                <td>No</td>
                <td>1024</td>
                <td>Integer</td>
                <td>The size of the buffer that is written to the OceanBase at one time.</td>
            </tr>
            <tr>
                <td>max-error-rows</td>
                <td>No</td>
                <td>No</td>
                <td>0</td>
                <td>Long</td>
                <td>Maximum tolerable number of error rows.</td>
            </tr>
            <tr>
                <td>dup-action</td>
                <td>No</td>
                <td>No</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>Action when there is duplicated record of direct-load task. Can be <code>STOP_ON_DUP</code>, <code>REPLACE</code> or <code>IGNORE</code>.</td>
            </tr>
            <tr>
                <td>timeout</td>
                <td>No</td>
                <td>No</td>
                <td>7d</td>
                <td>Duration</td>
                <td>The timeout for direct-load task.</td>
            </tr>
            <tr>
                <td>heartbeat-timeout</td>
                <td>No</td>
                <td>No</td>
                <td>60s</td>
                <td>Duration</td>
                <td>Client heartbeat timeout in direct-load task.</td>
            </tr>
            <tr>
                <td>heartbeat-interval</td>
                <td>No</td>
                <td>No</td>
                <td>10s</td>
                <td>Duration</td>
                <td>Client heartbeat interval in direct-load task.</td>
            </tr>
            <tr>
                <td>direct-load.load-method</td>
                <td>No</td>
                <td>No</td>
                <td>full</td>
                <td>String</td>
                <td>The direct-load load mode: <code>full</code>, <code>inc</code>, <code>inc_replace</code>.
                <ul>
                    <li><code>full</code>: full direct-load, default value.</li>
                    <li><code>inc</code>: normal incremental direct-load, primary key conflict check will be performed, observer-4.3.2 and above support, direct-load.dup-action REPLACE is not supported for the time being.</li>
                    <li><code>inc_replace</code>: special replace mode incremental direct-load, no primary key conflict check will be performed, directly overwrite the old data (equivalent to the effect of replace), direct-load.dup-action parameter will be ignored, observer-4.3.2 and above support.</li>
                </ul>
                </td>
            </tr>
            <tr>
                <td>enable-multi-node-write</td>
                <td>No</td>
                <td>No</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Whether to enable direct-load that supports multi-node writing. Not enabled by default.</td>
            </tr>
            <tr>
                <td>execution-id</td>
                <td>No</td>
                <td>No</td>
                <td></td>
                <td>String</td>
                <td>The execution id of the direct-load task. This parameter is only valid when the <code>enable-multi-node-write</code> parameter is true.</td>
            </tr>
        </tbody>
    </table>
</div>

## References

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/)

