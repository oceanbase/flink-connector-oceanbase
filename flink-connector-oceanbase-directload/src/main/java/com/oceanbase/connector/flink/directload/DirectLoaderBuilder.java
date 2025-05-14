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

package com.oceanbase.connector.flink.directload;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadManager;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.execution.ObDirectLoadStatementExecutor;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/** The builder for {@link DirectLoader}. */
public class DirectLoaderBuilder implements Serializable {

    private String host;
    private int port;

    private String user;
    private String tenant;
    private String password;

    private String schema;
    private String table;

    /** Server-side parallelism. */
    private int parallel;

    private long maxErrorCount;

    private ObLoadDupActionType duplicateKeyAction = ObLoadDupActionType.REPLACE;

    /** The overall timeout of the direct load task */
    private long timeout = 1000L * 1000 * 1000;

    private long heartBeatTimeout = 60 * 1000;

    private long heartBeatInterval = 10 * 1000;

    /** Direct load mode: full, inc, inc_replace. */
    private String directLoadMethod = "full";

    private String executionId;

    public DirectLoaderBuilder host(String host) {
        this.host = host;
        return this;
    }

    public DirectLoaderBuilder port(int port) {
        this.port = port;
        return this;
    }

    public DirectLoaderBuilder user(String user) {
        this.user = user;
        return this;
    }

    public DirectLoaderBuilder tenant(String tenant) {
        this.tenant = tenant;
        return this;
    }

    public DirectLoaderBuilder password(String password) {
        this.password = password;
        return this;
    }

    public DirectLoaderBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public DirectLoaderBuilder table(String table) {
        this.table = table;
        return this;
    }

    public DirectLoaderBuilder parallel(int parallel) {
        this.parallel = parallel;
        return this;
    }

    public DirectLoaderBuilder maxErrorCount(long maxErrorCount) {
        this.maxErrorCount = maxErrorCount;
        return this;
    }

    public DirectLoaderBuilder duplicateKeyAction(ObLoadDupActionType duplicateKeyAction) {
        this.duplicateKeyAction = duplicateKeyAction;
        return this;
    }

    public DirectLoaderBuilder directLoadMethod(String directLoadMethod) {
        this.directLoadMethod = directLoadMethod;
        return this;
    }

    public DirectLoaderBuilder timeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public DirectLoaderBuilder heartBeatTimeout(long heartBeatTimeout) {
        this.heartBeatTimeout = heartBeatTimeout;
        return this;
    }

    public DirectLoaderBuilder heartBeatInterval(long heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
        return this;
    }

    public DirectLoaderBuilder executionId(String executionId) {
        this.executionId = executionId;
        return this;
    }

    public DirectLoader build() {
        try {
            ObDirectLoadConnection obDirectLoadConnection = buildConnection(parallel);
            ObDirectLoadStatement.Builder statementBuilder = buildStatement(obDirectLoadConnection);
            if (StringUtils.isNotBlank(executionId)) {
                return new DirectLoader(
                        this,
                        String.format("%s.%s", schema, table),
                        statementBuilder,
                        obDirectLoadConnection,
                        executionId);
            } else {
                return new DirectLoader(
                        this,
                        String.format("%s.%s", schema, table),
                        statementBuilder,
                        obDirectLoadConnection);
            }
        } catch (ObDirectLoadException e) {
            throw new RuntimeException("Fail to obtain direct-load connection.", e);
        }
    }

    private ObDirectLoadConnection buildConnection(int writeThreadNum)
            throws ObDirectLoadException {
        return ObDirectLoadManager.getConnectionBuilder()
                .setServerInfo(host, port)
                .setLoginInfo(tenant, user, password, schema)
                .setHeartBeatInfo(heartBeatTimeout, heartBeatInterval)
                .enableParallelWrite(writeThreadNum)
                .build();
    }

    private ObDirectLoadStatement.Builder buildStatement(ObDirectLoadConnection connection)
            throws ObDirectLoadException {
        return connection
                .getStatementBuilder()
                .setTableName(table)
                .setDupAction(duplicateKeyAction)
                .setParallel(parallel)
                .setQueryTimeout(timeout)
                .setMaxErrorRowCount(maxErrorCount)
                .setLoadMethod(directLoadMethod)
                .setNodeRole(ObDirectLoadStatementExecutor.NodeRole.P2P);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getTenant() {
        return tenant;
    }

    public String getPassword() {
        return password;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public int getParallel() {
        return parallel;
    }

    public long getMaxErrorCount() {
        return maxErrorCount;
    }

    public ObLoadDupActionType getDuplicateKeyAction() {
        return duplicateKeyAction;
    }

    public String getDirectLoadMethod() {
        return directLoadMethod;
    }

    public String getExecutionId() {
        return executionId;
    }
}
