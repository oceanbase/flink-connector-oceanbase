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

import org.apache.flink.util.function.SerializableFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

class OceanBaseMySQLDialectTest {

    @Test
    void getUpsertStatementUpdate() {
        OceanBaseMySQLDialect dialect = new OceanBaseMySQLDialect();
        String upsertStatement =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id", "name").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(
                "INSERT INTO `sche1`.`tb1`(`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)",
                upsertStatement);
    }

    @Test
    void getUpsertStatementIgnore() {
        OceanBaseMySQLDialect dialect = new OceanBaseMySQLDialect();
        String upsertStatement =
                dialect.getUpsertStatement(
                        "sche1",
                        "tb1",
                        Stream.of("id").collect(Collectors.toList()),
                        Stream.of("id").collect(Collectors.toList()),
                        (SerializableFunction<String, String>) s -> "?");
        Assertions.assertEquals(
                "INSERT IGNORE INTO `sche1`.`tb1`(`id`) VALUES (?)", upsertStatement);
    }
}
