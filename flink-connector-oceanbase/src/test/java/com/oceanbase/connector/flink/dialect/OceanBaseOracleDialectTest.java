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
package com.oceanbase.connector.flink.dialect;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import org.apache.flink.util.function.SerializableFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OceanBaseOracleDialectTest {

    @Test
    public void testQuoteIdentifier() {
        OceanBaseConnectorOptions options = new OceanBaseConnectorOptions(Maps.newHashMap());
        Assertions.assertTrue(options.getTableOracleTenantCaseInsensitive());
        OceanBaseOracleDialect oracleDialect = new OceanBaseOracleDialect(options);

        String identifier = "name";
        Assertions.assertEquals(identifier, oracleDialect.quoteIdentifier(identifier));
    }

    @Test
    public void testQuoteIdentifierCaseSensitive() {
        Map<String, String> config = Maps.newHashMap();
        config.put("table.oracle-tenant-case-insensitive", "false");
        OceanBaseConnectorOptions options = new OceanBaseConnectorOptions(config);
        Assertions.assertFalse(options.getTableOracleTenantCaseInsensitive());

        OceanBaseOracleDialect oracleDialect = new OceanBaseOracleDialect(options);
        Assertions.assertEquals("\"name\"", oracleDialect.quoteIdentifier("name"));
        Assertions.assertEquals(
                "\"sche1\".\"tb1\"", oracleDialect.getFullTableName("sche1", "tb1"));
    }

    @Test
    public void testGetUpsertStatementWithUpdate() {
        OceanBaseConnectorOptions options = new OceanBaseConnectorOptions(Maps.newHashMap());
        OceanBaseOracleDialect oracleDialect = new OceanBaseOracleDialect(options);

        String upsertStatement =
                oracleDialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(
                "MERGE INTO sche1.tb1 t  USING (SELECT ? AS id, ? AS name FROM DUAL) s  ON (t.id=s.id)  WHEN MATCHED THEN UPDATE SET t.name=s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
                upsertStatement);
    }

    @Test
    public void testGetSysDatabase() {
        OceanBaseOracleDialect oracleDialect =
                new OceanBaseOracleDialect(new OceanBaseConnectorOptions(Maps.newHashMap()));
        Assertions.assertEquals("SYS", oracleDialect.getSysDatabase());
    }

    @Test
    public void testGetQueryTenantNameStatement() {
        OceanBaseOracleDialect oracleDialect =
                new OceanBaseOracleDialect(new OceanBaseConnectorOptions(Maps.newHashMap()));
        Assertions.assertEquals(
                "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL",
                oracleDialect.getQueryTenantNameStatement());
    }

    @Test
    public void testGetListSchemaStatement() {
        OceanBaseOracleDialect oracleDialect =
                new OceanBaseOracleDialect(new OceanBaseConnectorOptions(Maps.newHashMap()));
        Assertions.assertEquals(
                "SELECT USERNAME FROM ALL_USERS", oracleDialect.getListSchemaStatement());
    }

    @Test
    public void testGetListTableStatement() {
        OceanBaseOracleDialect oracleDialect =
                new OceanBaseOracleDialect(new OceanBaseConnectorOptions(Maps.newHashMap()));
        Assertions.assertEquals(
                "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = 'TEST_SCHEMA'",
                oracleDialect.getListTableStatement("TEST_SCHEMA"));
    }
}
