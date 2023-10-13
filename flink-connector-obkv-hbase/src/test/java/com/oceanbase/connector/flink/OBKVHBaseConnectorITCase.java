/*
 * Copyright (c) 2023 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.constants.OHConstants;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OBKVHBaseConnectorITCase extends OceanBaseTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseConnectorITCase.class);

    public static final String CLUSTER_NAME = "obcluster";
    public static final String CONFIG_URL =
            "http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=" + CLUSTER_NAME;

    private GenericContainer<?> configServer;

    @Before
    @Override
    public void before() {
        super.before();

        configServer =
                new GenericContainer<>("whhe/obconfigserver")
                        .withNetworkMode("host")
                        .waitingFor(
                                Wait.forLogMessage(".*boot success!.*", 1)
                                        .withStartupTimeout(Duration.ofMinutes(1)))
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        Startables.deepStart(configServer).join();

        try (Connection connection =
                        DriverManager.getConnection(
                                obServer.getJdbcUrl(),
                                obServer.getSysUsername(),
                                obServer.getSysPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("alter system set obconfig_url = '%s'", CONFIG_URL));
        } catch (SQLException e) {
            throw new RuntimeException("Set config url failed", e);
        }

        waitUntilConfigServerUpdated();
    }

    private void waitUntilConfigServerUpdated() {
        long start = System.currentTimeMillis();
        while (true) {
            if (isConfigServerUpdated()) {
                break;
            }
            if (System.currentTimeMillis() - start > 300_000) {
                throw new RuntimeException("Timeout to update config server");
            }

            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                LOG.error(e.toString());
            }
        }
    }

    private boolean isConfigServerUpdated() {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpget = new HttpGet(CONFIG_URL);
        try {
            CloseableHttpResponse response = httpclient.execute(httpget);
            if (response.getEntity().getContentLength() > 0) {
                String resp = EntityUtils.toString(response.getEntity(), "UTF-8");
                LOG.info("Config url response: {}", resp);
                ObjectMapper objectMapper = new ObjectMapper();
                return 200 == objectMapper.readTree(resp).get("Code").asInt();
            }
        } catch (Exception e) {
            LOG.warn("Request config url failed", e);
        }
        return false;
    }

    @After
    @Override
    public void after() {
        super.after();

        if (configServer != null) {
            configServer.close();
        }
    }

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        String hTable = "htable";
        String family1 = "family1";
        String family2 = "family2";

        String url = String.format("%s&database=%s", CONFIG_URL, obServer.getDatabaseName());
        String fullUsername = obServer.getUsername() + "#" + CLUSTER_NAME;

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " %s ROW<q1 INT>,"
                                + " %s ROW<q2 STRING, q3 INT>,"
                                + " PRIMARY KEY (rowkey) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='obkv-hbase',"
                                + "  'url'='%s',"
                                + "  'table-name'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'sys.username'='%s',"
                                + "  'sys.password'='%s'"
                                + ");",
                        family1,
                        family2,
                        url,
                        hTable,
                        fullUsername,
                        obServer.getPassword(),
                        obServer.getSysUsername(),
                        obServer.getSysPassword()));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO target VALUES %s, %s, %s, %s",
                                row("1", 1, "1", 1),
                                row("2", null, "2", null),
                                row("3", 3, null, null),
                                row("4", 4, "4", null)))
                .await();

        List<String> expected1 = Arrays.asList("1,q1,1", "3,q1,3", "4,q1,4");
        List<String> expected2 = Arrays.asList("1,q2,1", "2,q2,2", "4,q2,4");
        List<String> expected3 = Arrays.asList("1,q3,1");

        Configuration conf = new Configuration();
        conf.set(OHConstants.HBASE_OCEANBASE_PARAM_URL, url);
        conf.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME, fullUsername);
        conf.set(OHConstants.HBASE_OCEANBASE_PASSWORD, obServer.getPassword());
        conf.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME, obServer.getSysUsername());
        conf.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD, obServer.getSysPassword());

        OHTableClient client = new OHTableClient(hTable, conf);
        client.init();

        List<String> result1 = queryHTable(client, family1, "q1");
        List<String> result2 = queryHTable(client, family2, "q2");
        List<String> result3 = queryHTable(client, family2, "q3");

        assertEqualsInAnyOrder(expected1, result1);
        assertEqualsInAnyOrder(expected2, result2);
        assertEqualsInAnyOrder(expected3, result3);

        client.close();
    }

    private List<String> queryHTable(OHTableClient client, String family, String column)
            throws IOException {
        List<String> result = new ArrayList<>();
        Get get = new Get();
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        for (KeyValue kv : client.get(get).list()) {
            result.add(
                    String.format(
                            "%s,%s,%s",
                            Bytes.toString(kv.getRow()),
                            Bytes.toString(kv.getQualifier()),
                            Bytes.toString(kv.getValue())));
        }
        return result;
    }

    private String row(String key, Integer q1, String q2, Integer q3) {
        return String.format(
                "(%s, ROW(%s), ROW(%s, %s))", string(key), integer(q1), string(q2), integer(q3));
    }

    private String integer(Number n) {
        if (n == null) {
            return "CAST(NULL AS INT)";
        }
        return n.toString();
    }

    private String string(String s) {
        if (s == null) {
            return "CAST(NULL AS STRING)";
        }
        return "'" + s + "'";
    }
}
