# OceanBase Connectors for Apache Flink

[English](README.md) | 简体中文

[![Build Status](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/push_pr.yml/badge.svg?branch=main)](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/push_pr.yml?query=branch%3Amain)
[![Release](https://img.shields.io/github/release/oceanbase/flink-connector-oceanbase.svg)](https://github.com/oceanbase/flink-connector-oceanbase/releases)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

本仓库包含 OceanBase 的 Flink Connector。

## 功能

运行环境需要准备

- Flink 1.15–1.20：JDK 8
- Flink 2.0–2.3：JDK 17

OceanBase Connector 和 OceanBase Direct Load Connector 为 Flink 2.x 提供独立制品，避免影响 Flink 1.x 制品的二进制兼容性。可使用以下命令构建 Flink 2.x SQL Connector：

```shell
mvn -pl flink-sql-connector-oceanbase-flink2,flink-sql-connector-oceanbase-directload-flink2 \
    -am clean package -Pflink-2 -DskipTests
```

生成的制品分别为 `flink-sql-connector-oceanbase-flink2` 和
`flink-sql-connector-oceanbase-directload-flink2`；不带 `-flink2` 后缀的制品继续兼容 Flink 1.x。

Flink 2.x 制品会持续在当前各版本线的最新补丁版本上进行测试：2.0.2、2.1.3、2.2.1 和
2.3.0。默认使用 Flink 2.0.2 构建；如需针对其他受支持的 Flink 2.x 版本构建和测试，可覆盖
`flink2.version`，例如：

```shell
mvn -pl flink-sql-connector-oceanbase-flink2,flink-sql-connector-oceanbase-directload-flink2 \
    -am clean package -Pflink-2 -Dflink2.version=2.3.0
```

Flink 1.x 以及全量 Connector 仍使用默认的 JDK 8 构建方式：

```shell
mvn clean package -DskipTests
```

本仓库提供了如下 Connector：

|               Connector                |                                                                            描述                                                                             |                             使用文档                             |
|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| Flink Connector: OceanBase             | 该Connector通过OceanBase支持的JDBC驱动从OceanBase读取数据或将数据写入OceanBase，支持MySQL 和 Oracle 兼容模式。Source 支持可配置分片策略的并行快照读取。                                                | [Source & Sink](docs/sink/flink-connector-oceanbase_cn.md)   |
| Flink Connector: OceanBase Direct Load | 该Connector通过[旁路导入](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)API将数据写入OceanBase。                                        | [Sink](docs/sink/flink-connector-oceanbase-directload_cn.md) |
| Flink Connector: OBKV HBase            | 该Connector通过[OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java)将数据写入OceanBase。                                                          | [Sink](docs/sink/flink-connector-obkv-hbase_cn.md)           |
| Flink Connector: OBKV HBase2           | 该Connector通过[OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java)将数据写入OceanBase，相比`flink-connector-obkv-hbase`以**扁平化**为基础，支持**动态列更新**等特性。 | [Sink](docs/sink/flink-connector-obkv-hbase2_cn.md)          |

我们还提供了一个用于提交 Flink 端到端任务的命令行工具，详细信息请参阅 [CLI 文档](docs/cli/README_CN.md)。

### 其他外部项目

在其他的社区和组织中，也有一些项目可以用于通过 Flink 处理 OceanBase 中的数据。

|                                Project                                 | OceanBase 兼容模式 |                                                                      支持的功能                                                                       |
|------------------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| [Flink Connector JDBC](https://github.com/apache/flink-connector-jdbc) | MySQL, Oracle  | [Source + Sink](https://nightlies.apache.org/flink/flink-docs-release-1.19/zh/docs/connectors/table/jdbc/)                                       |
| [Flink CDC](https://github.com/ververica/flink-cdc-connectors)         | MySQL, Oracle  | [Source + CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/oceanbase-cdc/)                         |
| [Apache SeaTunnel](https://github.com/apache/seatunnel)                | MySQL, Oracle  | [Source](https://seatunnel.apache.org/docs/connector-v2/source/OceanBase), [Sink](https://seatunnel.apache.org/docs/connector-v2/sink/OceanBase) |

## 社区

当你需要帮助时，你可以在 [https://ask.oceanbase.com](https://ask.oceanbase.com) 上找到开发者和其他的社区伙伴。

当你发现项目缺陷时，请在 [issues](https://github.com/oceanbase/flink-connector-oceanbase/issues) 页面创建一个新的 issue。

## 许可证

更多信息见 [LICENSE](LICENSE)。
