## Flink Connector OBKV HBase

English | [简体中文](flink-connector-obkv-hbase_cn.md)

This is the Flink connector for OBKV HBase mode, which can be used to sink data to OceanBase via [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java).

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://mvnrepository.com/artifact/com.oceanbase/flink-connector-obkv-hbase).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase</artifactId>
    <version>${project.version}</version>
</dependency>
```

If you'd rather use the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

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

You can also manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### SQL JAR

To use this connector through Flink SQL directly, you need to download the shaded jar file named `flink-sql-connector-obkv-hbase-${project.version}.jar`:
- Release versions: https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-obkv-hbase
- Snapshot versions: https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-obkv-hbase

### Demo

Create a table with the name `htable1$family1`, which means the table name is `htable1` and the column family name is `family1` in HBase.

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

#### Java Demo

Take Maven project for example, add the required dependencies to the pom.xml, and then use the following code.

```java
package com.oceanbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TABLE t_sink ( "
                        + " rowkey STRING,"
                        + " family1 ROW<column1 STRING, column2 STRING>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='obkv-hbase',"
                        + "  'url'='http://127.0.0.1:8080/services?...&database=test',"
                        + "  'table-name'='htable1',"
                        + "  'username'='root@test',"
                        + "  'password'='',"
                        + "  'sys.username'='root',"
                        + "  'sys.password'=''"
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO t_sink VALUES "
                                + "('1', ROW('r1f1c1', 'r1f1c2')),"
                                + "('2', ROW('r2f1c1', 'r2f1c2')),"
                                + "('2', ROW('r3f1c1', 'r3f1c2'));")
                .await();
    }
}
```

Once executed, the records should have been written to OceanBase.

#### Flink SQL Demo

Put the JAR files of dependencies to the 'lib' directory of Flink, and then create the destination table with Flink SQL through the sql client.

```sql
CREATE TABLE t_sink
(
    rowkey STRING,
    family1 ROW <column1 STRING,
    column2 STRING >,
    PRIMARY KEY (rowkey) NOT ENFORCED
)
with ( 'connector'='obkv-hbase',
    'url'='http://127.0.0.1:8080/services?...&database=test',
    'table-name'='htable1',
    'username'='root@test',
    'password'='',
    'sys.username'='root',
    'sys.password'='');
```

Insert records by Flink SQL.

```sql
INSERT INTO t_sink
VALUES ('1', ROW ('r1f1c1', 'r1f1c2')),
       ('2', ROW ('r2f1c1', 'r2f1c2')),
       ('2', ROW ('r3f1c1', 'r3f1c2'));
```

Once executed, the records should have been written to OceanBase.

## Configuration

|          Option          | Required | Default |   Type   |            Description            |
|--------------------------|----------|---------|----------|-----------------------------------|
| url                      | Yes      |         | String   | The config url with database name |
| table-name               | Yes      |         | String   | The table name of HBase           |
| username                 | Yes      |         | String   | The username                      |
| password                 | Yes      |         | String   | The password                      |
| sys.username             | Yes      |         | String   | The username of sys tenant        |
| sys.password             | Yes      |         | String   | The password of sys tenant        |
| buffer-flush.interval    | No       | 1s      | Duration | Buffer flush interval             |
| buffer-flush.buffer-size | No       | 1000    | Integer  | Buffer size                       |
| buffer-flush.batch-size  | No       | 100     | Integer  | Buffer flush batch size           |
| max-retries              | No       | 3       | Integer  | Max retry times on failure        |

## References

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-hbase](https://github.com/apache/flink-connector-hbase)

[https://github.com/oceanbase/obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java)

