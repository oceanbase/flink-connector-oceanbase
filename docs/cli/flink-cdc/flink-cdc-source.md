# Using Flink CDC as Source

English | [简体中文](flink-cdc-source_cn.md)

## Dependencies

This project is based on the SQL Client JAR of [Flink CDC Source Connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/overview/).

We do not provide Flink CDC Source Connector in the JAR package of this project, so you need to manually download the used Flink CDC SQL JAR. Note that this project requires Flink CDC to be 3.2.0 or later version.

If you're using Flink Oracle CDC as source, you need also download the dependencies of the source connector, see the *Dependencies* chapter of [Oracle CDC Connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/oracle-cdc/#sql-client-jar).

## Demo: Migrate from Flink MySQL CDC to OceanBase

### Preparation

Add the CLI JAR `flink-connector-oceanbase-cli-xxx.jar` and dependency JAR `flink-sql-connector-mysql-cdc-xxx.jar` to `$FLINK_HOME/lib`.

Then prepare tables and data in MySQL database.

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

### Submit Job via CLI

Replace the following command with your real database information, and execute it to submit a Flink job.

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

### Check and Verify

Check the target OceanBase database, you should find out these two tables and rows data.

You can go on insert test data to MySQL database, since it is a CDC task, after data is inserted in MySQL, you can query and verify the synchronized data in OceanBase.

## Options

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 5%">Required</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>${job-type}</td>
                <td>Yes</td>
                <td>Enumeration value</td>
                <td style="word-wrap: break-word;"></td>
                <td>Job type, can be <code>mysql-cdc</code>.</td>
            </tr>
            <tr>
                <td>--source-conf</td>
                <td>Yes</td>
                <td>Multi-value parameter</td>
                <td style="word-wrap: break-word;"></td>
                <td>Configurations of specific Flink CDC Source.</td>
            </tr>
            <tr>
                <td>--sink-conf</td>
                <td>Yes</td>
                <td>Multi-value parameter</td>
                <td style="word-wrap: break-word;"></td>
                <td>Configurations of OceanBase Sink.</td>
            </tr>
            <tr>
                <td>--job-name</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;">${job-type} Sync</td>
                <td>The Flink job name.</td>
            </tr>
            <tr>
                <td>--database</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>The target database in OceanBase, if not set, the source db name will be used.</td>
            </tr>
            <tr>
                <td>--table-prefix</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>The target table name prefix in OceanBase.</td>
            </tr>
            <tr>
                <td>--table-suffix</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>The target table name suffix in OceanBase.</td>
            </tr>
            <tr>
                <td>--including-tables</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>Table whitelist pattern.</td>
            </tr>
            <tr>
                <td>--excluding-tables</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>Table blacklist pattern.</td>
            </tr>
            <tr>
                <td>--multi-to-one-origin</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>The source table name patterns which mapping multiple source tables to one target table, multiple values are separated by <code>|</code>.</td>
            </tr>
            <tr>
                <td>--multi-to-one-target</td>
                <td>No</td>
                <td>String</td>
                <td style="word-wrap: break-word;"></td>
                <td>The target table name that corresponds to <code>--multi-to-one-origin</code>, multiple values are separated by <code>|</code>, and the length should equal to <code>--multi-to-one-origin</code>.</td>
            </tr>
            <tr>
                <td>--create-table-only</td>
                <td>No</td>
                <td>Boolean</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Whether to only synchronize the structure of the table.</td>
            </tr>
            <tr>
                <td>--ignore-default-value</td>
                <td>No</td>
                <td>Boolean</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Whether to ignore default values. </td>
            </tr>
            <tr>
                <td>--ignore-incompatible</td>
                <td>No</td>
                <td>Boolean</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Whether to ignore incompatible data types.</td>
            </tr>
        </tbody>
    </table>
</div>

