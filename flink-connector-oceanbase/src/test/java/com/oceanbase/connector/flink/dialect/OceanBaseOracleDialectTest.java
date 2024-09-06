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

import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

public class OceanBaseOracleDialectTest {

    @Test
    public void testQuoteIdentifier() {
        OceanBaseConnectorOptions options = new OceanBaseConnectorOptions(Maps.newHashMap());
        Assert.assertTrue(options.getTableOracleTenantCaseInsensitive());
        OceanBaseOracleDialect oracleDialect = new OceanBaseOracleDialect(options);

        String identifier = "name";
        Assert.assertEquals(identifier, oracleDialect.quoteIdentifier(identifier));
    }
}
