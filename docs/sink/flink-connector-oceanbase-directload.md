# Flink Connector OceanBase Direct Load

English | [简体中文](flink-connector-oceanbase-directload_cn.md)

This Flink Connector is based on the direct-load feature of OceanBase, enabling high-performance bulk data loading from Flink to OceanBase.

## Important Notes

**This connector is specifically designed for batch processing scenarios with the following characteristics:**

- ✅ **Bounded Streams Only**: Data sources must be bounded; unbounded streams are not supported. Flink Batch mode is recommended for better performance
- ✅ **High Throughput**: Ideal for large-scale data import with multi-node parallel writing capability
- ⚠️ **Table Locking During Import**: The target table will be locked during the direct-load process, allowing only SELECT queries. INSERT/UPDATE/DELETE operations are not permitted
- ⚠️ **Not for Real-time**: If you need real-time/streaming writes with unbounded streams, please use [flink-connector-oceanbase](flink-connector-oceanbase.md) instead

For more details on OceanBase's direct-load feature, see the [direct-load document](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001375568).

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

### Prerequisites

**Data sources must be bounded streams**. The direct-load connector does not support unbounded streams.

**Flink Batch mode is recommended** for better performance:

- **Table API / Flink SQL**:

  ```sql
  SET 'execution.runtime-mode' = 'BATCH';
  ```
- **DataStream API**:

  ```java
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  env.setRuntimeMode(RuntimeExecutionMode.BATCH);
  ```

> **Note**: While you can use bounded data sources in Streaming mode, Batch mode typically provides better performance and resource utilization.

### Performance Tuning

- **Parallelism Adjustment**: Supports multi-node parallel writing. Increase Flink task parallelism to improve throughput

  ```sql
  SET 'parallelism.default' = '8';  -- Adjust based on data volume
  ```
- **Server-side Parallelism**: Use the `parallel` parameter to configure CPU resources on OceanBase server for processing the import task

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

#### Flink SQL Demo

Put the JAR files of dependencies into the 'lib' directory of Flink, then create the destination table using Flink SQL through the SQL client.

```sql
-- Recommended to set BATCH mode for better performance
SET 'execution.runtime-mode' = 'BATCH';

-- Optional: Adjust parallelism based on data volume to improve throughput
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
    'parallel' = '8'  -- OceanBase server-side parallelism
);
```

Insert records using Flink SQL:

```sql
INSERT INTO t_sink
VALUES (1, 'Tom', 99),
       (2, 'Jerry', 88),
       (3, 'Alice', 95);
```

Once executed, the records should have been written to OceanBase.

**Note**: During the execution of the `INSERT` statement (while direct-load is in progress), the target table `t_sink` will be locked. Only SELECT queries are allowed; INSERT/UPDATE/DELETE operations are not permitted.

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
        </tbody>
    </table>
</div>

## References

- [https://github.com/oceanbase/obkv-table-client-java](https://github.com/oceanbase/obkv-table-client-java)
- [https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/rest_api/)

