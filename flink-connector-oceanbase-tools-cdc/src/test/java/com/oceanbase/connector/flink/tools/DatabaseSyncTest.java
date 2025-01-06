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

package com.oceanbase.connector.flink.tools;

import com.oceanbase.connector.flink.tools.cdc.DatabaseSync;
import com.oceanbase.connector.flink.tools.cdc.db2.Db2DatabaseSync;
import com.oceanbase.connector.flink.tools.cdc.mysql.MysqlDatabaseSync;
import com.oceanbase.connector.flink.tools.cdc.postgres.PostgresDatabaseSync;
import com.oceanbase.connector.flink.tools.cdc.sqlserver.SqlServerDatabaseSync;

import org.apache.flink.configuration.Configuration;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/** Unit tests for the {@link DatabaseSync}. */
public class DatabaseSyncTest {
    @Test
    public void multiToOneRulesParserTest() throws Exception {
        String[][] testCase = {
            {"a_.*|b_.*", "a|b"} //  Normal condition
            // ,{"a_.*|b_.*","a|b|c"} // Unequal length
            // ,{"",""} // Null value
            // ,{"***....","a"} // Abnormal regular expression
        };
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        Arrays.stream(testCase)
                .forEach(
                        arr -> {
                            databaseSync.multiToOneRulesParser(arr[0], arr[1]);
                        });
    }

    @Test
    public void getSyncTableListTest() throws Exception {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.setIncludingTables("tbl_1|tbl_2");
        Configuration config = new Configuration();
        config.setString("database-name", "db");
        config.setString("table-name", "tbl.*");
        databaseSync.setConfig(config);
        String syncTableList = databaseSync.getSyncTableList(Arrays.asList("tbl_1", "tbl_2"));
        assertEquals("(db)\\.(tbl_1|tbl_2)", syncTableList);
    }

    @Test
    public void singleSinkTablePatternTest() throws SQLException {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.setIncludingTables(".*");
        databaseSync.setExcludingTables("customer|dates|lineorder");
        Configuration config = new Configuration();
        config.setString("database-name", "ssb_test");
        databaseSync.setConfig(config);
        List<String> tableList =
                Arrays.asList("customer", "dates", "lineorder", "test1", "test2", "test3");
        String syncTableListPattern = databaseSync.getSyncTableList(tableList);
        assertTrue("ssb_test.test1".matches(syncTableListPattern));
        assertTrue("ssb_test.test2".matches(syncTableListPattern));
        assertTrue("ssb_test.test3".matches(syncTableListPattern));
        assertFalse("ssb_test.customer".matches(syncTableListPattern));
        assertFalse("ssb_test.dates".matches(syncTableListPattern));
        assertFalse("ssb_test.lineorder".matches(syncTableListPattern));
    }

    @Test
    public void getJdbcPropertiesTest() throws Exception {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put("jdbc.properties.use_ssl", "false");

        Configuration config = Configuration.fromMap(mysqlConfig);
        databaseSync.setConfig(config);
        Properties jdbcProperties = databaseSync.getJdbcProperties();
        Assert.assertEquals(1, jdbcProperties.size());
        Assert.assertEquals("false", jdbcProperties.getProperty("use_ssl"));
    }

    @Test
    public void getJdbcUrlTemplateTest() throws SQLException {
        String mysqlJdbcTemplate = "jdbc:mysql://%s:%d?useInformationSchema=true";
        String postgresJdbcTemplate = "jdbc:postgresql://%s:%d/%s?";
        String sqlServerJdbcTemplate = "jdbc:sqlserver://%s:%d;database=%s;";
        String db2JdbcTemplate = "jdbc:db2://%s:%d/%s";

        // mysql jdbc properties configuration
        DatabaseSync mysqlDatabaseSync = new MysqlDatabaseSync();
        Map<String, String> mysqlJdbcConfig = new LinkedHashMap<>();
        mysqlJdbcConfig.put("jdbc.properties.use_ssl", "false");

        DatabaseSync postgresDatabaseSync = new PostgresDatabaseSync();
        Map<String, String> postgresJdbcConfig = new LinkedHashMap<>();
        postgresJdbcConfig.put("jdbc.properties.ssl", "false");

        DatabaseSync sqlServerDatabaseSync = new SqlServerDatabaseSync();
        Map<String, String> sqlServerJdbcConfig = new LinkedHashMap<>();
        sqlServerJdbcConfig.put("jdbc.properties.encrypt", "false");
        sqlServerJdbcConfig.put("jdbc.properties.integratedSecurity", "false");

        DatabaseSync db2DatabaseSync = new Db2DatabaseSync();
        Map<String, String> db2JdbcConfig = new LinkedHashMap<>();
        db2JdbcConfig.put("jdbc.properties.ssl", "false");
        db2JdbcConfig.put("jdbc.properties.allowNextOnExhaustedResultSet", "1");
        db2JdbcConfig.put("jdbc.properties.resultSetHoldability", "1");

        Configuration mysqlConfig = Configuration.fromMap(mysqlJdbcConfig);
        mysqlDatabaseSync.setConfig(mysqlConfig);

        Configuration postgresConfig = Configuration.fromMap(postgresJdbcConfig);
        postgresDatabaseSync.setConfig(postgresConfig);

        Configuration sqlServerConfig = Configuration.fromMap(sqlServerJdbcConfig);
        sqlServerDatabaseSync.setConfig(sqlServerConfig);

        Configuration db2Config = Configuration.fromMap(db2JdbcConfig);
        db2DatabaseSync.setConfig(db2Config);

        Properties mysqlJdbcProperties = mysqlDatabaseSync.getJdbcProperties();
        Assert.assertEquals(1, mysqlJdbcProperties.size());
        Assert.assertEquals("false", mysqlJdbcProperties.getProperty("use_ssl"));
        String mysqlJdbcUrlTemplate =
                mysqlDatabaseSync.getJdbcUrlTemplate(mysqlJdbcTemplate, mysqlJdbcProperties);
        Assert.assertEquals(mysqlJdbcTemplate + "&use_ssl=false", mysqlJdbcUrlTemplate);

        Properties postgresJdbcProperties = postgresDatabaseSync.getJdbcProperties();
        Assert.assertEquals(1, postgresJdbcProperties.size());
        Assert.assertEquals("false", postgresJdbcProperties.getProperty("ssl"));
        String postgresJdbcUrlTemplate =
                postgresDatabaseSync.getJdbcUrlTemplate(
                        postgresJdbcTemplate, postgresJdbcProperties);
        Assert.assertEquals(postgresJdbcTemplate + "&ssl=false", postgresJdbcUrlTemplate);

        Properties sqlServerJdbcProperties = sqlServerDatabaseSync.getJdbcProperties();
        Assert.assertEquals(2, sqlServerJdbcProperties.size());
        Assert.assertEquals("false", sqlServerJdbcProperties.getProperty("encrypt"));
        Assert.assertEquals("false", sqlServerJdbcProperties.getProperty("integratedSecurity"));
        String sqlServerJdbcUrlTemplate =
                sqlServerDatabaseSync.getJdbcUrlTemplate(
                        sqlServerJdbcTemplate, sqlServerJdbcProperties);
        Assert.assertEquals(
                sqlServerJdbcTemplate + "encrypt=false;integratedSecurity=false;",
                sqlServerJdbcUrlTemplate);

        Properties db2JdbcProperties = db2DatabaseSync.getJdbcProperties();
        Assert.assertEquals(3, db2JdbcProperties.size());
        Assert.assertEquals("false", db2JdbcProperties.getProperty("ssl"));
        Assert.assertEquals("1", db2JdbcProperties.getProperty("allowNextOnExhaustedResultSet"));
        Assert.assertEquals("1", db2JdbcProperties.getProperty("resultSetHoldability"));
        String db2JdbcUrlTemplate =
                db2DatabaseSync.getJdbcUrlTemplate(db2JdbcTemplate, db2JdbcProperties);
        Assert.assertEquals(
                db2JdbcTemplate
                        + ":allowNextOnExhaustedResultSet=1;ssl=false;resultSetHoldability=1;",
                db2JdbcUrlTemplate);
    }
}
