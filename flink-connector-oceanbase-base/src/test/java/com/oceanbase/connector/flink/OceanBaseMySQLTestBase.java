/*
 * Copyright 2024 OceanBase.
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

import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class OceanBaseMySQLTestBase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLTestBase.class);

    public static final String IMAGE_TAG = "4.2.1_bp2";

    @ClassRule
    public static final OceanBaseContainer OB_SERVER =
            new OceanBaseContainer(OceanBaseContainer.DOCKER_IMAGE_NAME + ":" + IMAGE_TAG)
                    .withNetworkMode("host")
                    .withSysPassword("123456")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("sql/init.sql"),
                            "/root/boot/init.d/init.sql")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected String getUrl() {
        return OB_SERVER.getJdbcUrl();
    }

    protected abstract String getTestTable();

    protected String getUsername() {
        return OB_SERVER.getUsername();
    }

    protected String getPassword() {
        return OB_SERVER.getPassword();
    }

    protected String getDatabaseName() {
        return OB_SERVER.getDatabaseName();
    }

    protected Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", getUrl());
        options.put("username", getUsername());
        options.put("password", getPassword());
        options.put("schema-name", getDatabaseName());
        options.put("table-name", getTestTable());
        return options;
    }

    protected String getOptionsString() {
        return getOptions().entrySet().stream()
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
