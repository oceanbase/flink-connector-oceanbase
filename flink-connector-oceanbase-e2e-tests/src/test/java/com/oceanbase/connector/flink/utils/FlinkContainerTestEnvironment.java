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

package com.oceanbase.connector.flink.utils;

import com.oceanbase.connector.flink.OceanBaseMySQLTestBase;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Volume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class FlinkContainerTestEnvironment extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainerTestEnvironment.class);

    private static final String FLINK_VERSION = System.getProperty("flink_version");
    private static final String MODULE_DIRECTORY = System.getProperty("moduleDir", "");

    private static final int JOB_MANAGER_REST_PORT = 8081;
    private static final String FLINK_BIN = "bin";
    private static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    private static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";

    protected String getFlinkDockerImageTag() {
        return String.format("flink:%s-scala_2.12", FLINK_VERSION);
    }

    protected String getFlinkProperties() {
        return String.join(
                "\n",
                Arrays.asList(
                        "jobmanager.rpc.address: jobmanager",
                        "taskmanager.numberOfTaskSlots: 10",
                        "parallelism.default: 4",
                        "execution.checkpointing.interval: 300"));
    }

    protected final TemporaryFolder temporaryFolder = new TemporaryFolder();
    protected final Volume sharedVolume = new Volume("/tmp/shared");

    public GenericContainer<?> jobManager;
    public GenericContainer<?> taskManager;

    @SuppressWarnings("resource")
    @BeforeEach
    public void before() throws Exception {
        LOG.info("Starting Flink containers...");
        temporaryFolder.create();

        jobManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", getFlinkProperties())
                        .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(sharedVolume))
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        Startables.deepStart(jobManager).join();
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", sharedVolume.toString());

        taskManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", getFlinkProperties())
                        .dependsOn(jobManager)
                        .withVolumesFrom(jobManager, BindMode.READ_WRITE)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        Startables.deepStart(taskManager).join();
        runInContainerAsRoot(taskManager, "chmod", "0777", "-R", sharedVolume.toString());

        LOG.info("Flink containers started");
    }

    @AfterEach
    public void after() throws Exception {
        if (jobManager != null) {
            jobManager.stop();
        }
        if (taskManager != null) {
            taskManager.stop();
        }
        temporaryFolder.delete();
    }

    @Override
    public String getHost() {
        return getOBServerIP();
    }

    @Override
    public int getPort() {
        return 2881;
    }

    @Override
    public int getRpcPort() {
        return 2882;
    }

    /**
     * Searches for a resource file matching the given regex in the given directory. This method is
     * primarily intended to be used for the initialization of static {@link Path} fields for
     * resource file(i.e. jar, config file). if resolvePaths is empty, this method will search file
     * under the modules {@code target} directory. if resolvePaths is not empty, this method will
     * search file under resolvePaths of current project.
     *
     * @param resourceNameRegex regex pattern to match against
     * @param resolvePaths an array of resolve paths of current project
     * @return Path pointing to the matching file
     * @throws RuntimeException if none or multiple resource files could be found
     */
    public static Path getResource(final String resourceNameRegex, String... resolvePaths) {
        Path path = Paths.get(MODULE_DIRECTORY).toAbsolutePath();

        if (resolvePaths != null && resolvePaths.length > 0) {
            path = path.getParent().getParent();
            for (String resolvePath : resolvePaths) {
                path = path.resolve(resolvePath);
            }
        }

        try (Stream<Path> dependencyResources = Files.walk(path)) {
            final List<Path> matchingResources =
                    dependencyResources
                            .filter(
                                    jar ->
                                            Pattern.compile(resourceNameRegex)
                                                    .matcher(jar.toAbsolutePath().toString())
                                                    .find())
                            .collect(Collectors.toList());
            switch (matchingResources.size()) {
                case 0:
                    throw new RuntimeException(
                            new FileNotFoundException(
                                    String.format(
                                            "No resource file could be found that matches the pattern %s. "
                                                    + "This could mean that the test module must be rebuilt via maven.",
                                            resourceNameRegex)));
                case 1:
                    return matchingResources.get(0);
                default:
                    throw new RuntimeException(
                            new IOException(
                                    String.format(
                                            "Multiple resource files were found matching the pattern %s. Matches=%s",
                                            resourceNameRegex, matchingResources)));
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Could not search for resource resource files.", ioe);
        }
    }

    /**
     * Submits a SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(List<String> sqlLines, Path... jars)
            throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        Path script = temporaryFolder.newFile().toPath();
        Files.write(script, sqlLines);
        jobManager.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");
        // commands.add("cat /tmp/script.sql | ");
        commands.add("timeout 3m");
        commands.add(FLINK_BIN + "/sql-client.sh -f /tmp/script.sql");
        for (Path jar : jars) {
            commands.add("--jar");
            String containerPath = copyAndGetContainerPath(jar.toAbsolutePath().toString());
            commands.add(containerPath);
        }

        Container.ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0 && execResult.getExitCode() != 124) {
            throw new AssertionError(
                    "Failed when submitting the SQL job. Exit code: "
                            + execResult.getExitCode()
                            + ", stderr: "
                            + execResult.getStderr());
        }
    }

    private String copyAndGetContainerPath(String filePath) {
        Path path = Paths.get(filePath);
        String containerPath = sharedVolume + "/" + path.getFileName();
        jobManager.copyFileToContainer(MountableFile.forHostPath(path), containerPath);
        return containerPath;
    }

    private void runInContainerAsRoot(GenericContainer<?> container, String... command)
            throws InterruptedException {
        ToStringConsumer stdoutConsumer = new ToStringConsumer();
        ToStringConsumer stderrConsumer = new ToStringConsumer();
        DockerClient dockerClient = DockerClientFactory.instance().client();
        ExecCreateCmdResponse execCreateCmdResponse =
                dockerClient
                        .execCreateCmd(container.getContainerId())
                        .withUser("root")
                        .withCmd(command)
                        .exec();
        FrameConsumerResultCallback callback = new FrameConsumerResultCallback();
        callback.addConsumer(OutputFrame.OutputType.STDOUT, stdoutConsumer);
        callback.addConsumer(OutputFrame.OutputType.STDERR, stderrConsumer);
        dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }

    public String multipleParameterArg(String key, String value) {
        return String.format("--%s '%s'", key, value);
    }

    public void submitJob(List<Path> dependencies, Path jar, String[] args)
            throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        commands.add(FLINK_BIN + "/flink run --detached");
        if (dependencies != null && !dependencies.isEmpty()) {
            for (Path dependency : dependencies) {
                commands.add(
                        "--classpath "
                                + "file://"
                                + copyAndGetContainerPath(dependency.toAbsolutePath().toString()));
            }
        }
        commands.add(copyAndGetContainerPath(jar.toAbsolutePath().toString()));
        commands.addAll(Arrays.asList(args));

        Container.ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the job.");
        }
    }
}
