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

import java.util.stream.Collectors;
import java.util.stream.Stream;

class OceanBaseOracleDialectTest {

    private final OceanBaseOracleDialect dialect =
            new OceanBaseOracleDialect(new OceanBaseConnectorOptions(Maps.newHashMap()));

    @Test
    void testQuoteIdentifier() {
        Assertions.assertTrue(
                new OceanBaseConnectorOptions(Maps.newHashMap())
                        .getTableOracleTenantCaseInsensitive());
        String identifier = "name";
        Assertions.assertEquals(identifier, dialect.quoteIdentifier(identifier));
    }

    @Test
    void getUpsertStatementSingleRow() {
        String upsertStatement =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        1,
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(
                "MERGE INTO sche1.tb1 t  USING (SELECT ? AS id, ? AS name FROM DUAL) s"
                        + "  ON (t.id=s.id)"
                        + "  WHEN MATCHED THEN UPDATE SET t.name=s.name"
                        + " WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
                upsertStatement);
    }

    @Test
    void getUpsertStatementMultiRow() {
        String upsertStatement =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        3,
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(
                "MERGE INTO sche1.tb1 t  USING (SELECT ? AS id, ? AS name FROM DUAL"
                        + " UNION ALL SELECT ? AS id, ? AS name FROM DUAL"
                        + " UNION ALL SELECT ? AS id, ? AS name FROM DUAL) s"
                        + "  ON (t.id=s.id)"
                        + "  WHEN MATCHED THEN UPDATE SET t.name=s.name"
                        + " WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
                upsertStatement);
    }

    @Test
    void getUpsertStatementMultiRowCompositeKey() {
        String upsertStatement =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name", "age").collect(Collectors.toList()),
                        Stream.of("id", "name").collect(Collectors.toList()),
                        2,
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(
                "MERGE INTO sche1.tb1 t  USING (SELECT ? AS id, ? AS name, ? AS age FROM DUAL"
                        + " UNION ALL SELECT ? AS id, ? AS name, ? AS age FROM DUAL) s"
                        + "  ON (t.id=s.id and t.name=s.name)"
                        + "  WHEN MATCHED THEN UPDATE SET t.age=s.age"
                        + " WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (s.id, s.name, s.age)",
                upsertStatement);
    }

    @Test
    void getUpsertStatementDefaultDelegatesToMultiRow() {
        String singleRow =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        (SerializableFunction<String, String>) s -> "?");
        String multiRow =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        1,
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(singleRow, multiRow);
    }
}
