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

package com.oceanbase.connector.flink.sink.v2;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;

import org.apache.flink.api.connector.sink2.Committer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class NoOPCommiter implements Committer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(NoOPCommiter.class);

    public NoOPCommiter(String executionId, OBDirectLoadConnectorOptions connectorOptions) {}

    @Override
    public void commit(Collection<CommitRequest<String>> commits)
            throws IOException, InterruptedException {}

    @Override
    public void close() throws Exception {}
}
