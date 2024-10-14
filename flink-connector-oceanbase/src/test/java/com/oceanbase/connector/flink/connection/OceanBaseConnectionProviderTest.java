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

package com.oceanbase.connector.flink.connection;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;

@Disabled
public class OceanBaseConnectionProviderTest {

    @Test
    public void testInitializeDefaultJdbcProperties() throws Exception {
        ImmutableMap<String, String> map =
                ImmutableMap.of(
                        "url",
                        "jdbc:mysql://localhost:3306/test",
                        "username",
                        "root@test",
                        "password",
                        "");
        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(map);
        try (OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions)) {
            Connection connection = connectionProvider.getConnection();
            // test useInformationSchema property
            ResultSet resultSet =
                    connection
                            .getMetaData()
                            .getTables(null, "test", "test", new String[] {"TABLE"});
            while (resultSet.next()) {
                String comment = resultSet.getString("REMARKS");
                Assertions.assertEquals("test table comment", comment);
            }
        }
    }
}
