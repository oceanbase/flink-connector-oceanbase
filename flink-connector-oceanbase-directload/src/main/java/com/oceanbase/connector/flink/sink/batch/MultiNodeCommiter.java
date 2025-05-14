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

package com.oceanbase.connector.flink.sink.batch;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;

import org.apache.flink.api.connector.sink2.SinkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * The multi-node direct load committer. Note: The committer node always operates with single
 * parallelism.
 */
public class MultiNodeCommiter implements SinkWriter<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiNodeCommiter.class);

    private final DirectLoader directLoader;

    public MultiNodeCommiter(String executionId, OBDirectLoadConnectorOptions connectorOptions) {
        this.directLoader =
                DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions, executionId);
        this.directLoader.begin();
    }

    @Override
    public void write(Void element, Context context) throws IOException, InterruptedException {
        // Do nothing.
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            // Do nothing, For streaming mode
        }
    }

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(this.directLoader)) {
            this.directLoader.commit();
            LOG.info("The direct-load commiter commited successfully.");
            this.directLoader.close();
        }
    }
}
