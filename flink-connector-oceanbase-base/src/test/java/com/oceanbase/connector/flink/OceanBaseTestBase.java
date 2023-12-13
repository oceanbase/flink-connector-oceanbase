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

import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OceanBaseTestBase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseTestBase.class);

    protected OceanBaseContainer obServer;
    protected String imageTag = "4.2.1_bp2";

    @Before
    public void before() {
        obServer =
                new OceanBaseContainer(OceanBaseContainer.DOCKER_IMAGE_NAME + ":" + imageTag)
                        .withNetworkMode("host")
                        .withSysPassword("123456")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("sql/init.sql"),
                                "/root/boot/init.d/init.sql")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        Startables.deepStart(obServer).join();
    }

    @After
    public void after() {
        if (obServer != null) {
            obServer.close();
        }
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
