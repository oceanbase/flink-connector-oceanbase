package com.oceanbase.connector.flink.connection;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.tools.catalog.TableSchema;
import com.oceanbase.connector.flink.utils.OceanBaseToolsJdbcUtils;

public class OceanBaseToolsConnectProvider extends OceanBaseConnectionProvider {

    public OceanBaseToolsConnectProvider(OceanBaseConnectorOptions options) {
        super(options);
    }

    public boolean databaseExists(String database) {
        return OceanBaseToolsJdbcUtils.databaseExists(database, this::getConnection);
    }

    public void createDatabase(String database) {
        OceanBaseToolsJdbcUtils.createDatabase(database, this::getConnection);
    }

    public boolean tableExists(String database, String table) {
        return OceanBaseToolsJdbcUtils.tableExists(database, table, this::getConnection);
    }

    public void createTable(TableSchema schema) {
        OceanBaseToolsJdbcUtils.createTable(schema, this::getConnection);
    }
}
