# OceanBase Connectors for Apache Flink

English | [简体中文](README_CN.md)

[![Build Status](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/push_pr.yml/badge.svg?branch=main)](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/push_pr.yml?query=branch%3Amain)
[![Release](https://img.shields.io/github/release/oceanbase/flink-connector-oceanbase.svg)](https://github.com/oceanbase/flink-connector-oceanbase/releases)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

This repository contains the OceanBase connectors for Apache Flink.

## Features

Prerequisites

- Flink 1.15–1.20: JDK 8
- Flink 2.0–2.3: JDK 17

The OceanBase and OceanBase Direct Load connectors provide separate Flink 2.x artifacts so that
their Flink 1.x artifacts remain binary compatible. Build the Flink 2.x SQL connector artifacts
with:

```shell
mvn -pl flink-sql-connector-oceanbase-flink2,flink-sql-connector-oceanbase-directload-flink2 \
    -am clean package -Pflink-2 -DskipTests
```

The generated artifacts are `flink-sql-connector-oceanbase-flink2` and
`flink-sql-connector-oceanbase-directload-flink2`. The artifacts without the `-flink2` suffix
remain compatible with Flink 1.x.

The Flink 2.x artifacts are tested against the latest patch releases of every currently supported
minor line: 2.0.2, 2.1.3, 2.2.1, and 2.3.0. The default build targets Flink 2.0.2. To build and
test against another supported Flink 2.x version, override `flink2.version`, for example:

```shell
mvn -pl flink-sql-connector-oceanbase-flink2,flink-sql-connector-oceanbase-directload-flink2 \
    -am clean package -Pflink-2 -Dflink2.version=2.3.0
```

For Flink 1.x and all connectors, use the default JDK 8 build:

```shell
mvn clean package -DskipTests
```

This repository contains connectors as following:

|               Connector                |                                                                                                                         Description                                                                                                                         |                         Document                          |
|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| Flink Connector: OceanBase             | This Connector uses the JDBC driver supported by OceanBase to read data from and write data to OceanBase, and supports MySQL and Oracle compatibility modes. Source supports parallel snapshot read with configurable split strategies.                     | [Source & Sink](docs/sink/flink-connector-oceanbase.md)   |
| Flink Connector: OceanBase Direct Load | This Connector uses the [direct load](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001375568) API to write data to OceanBase.                                                                                                            | [Sink](docs/sink/flink-connector-oceanbase-directload.md) |
| Flink Connector: OBKV HBase            | This Connector uses the [OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java) to write data to OceanBase.                                                                                                                                   | [Sink](docs/sink/flink-connector-obkv-hbase.md)           |
| Flink Connector: OBKV HBase2           | This Connector uses the [OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java) to write data to OceanBase. Compared to `flink-connector-obkv-hbase`, it is based on **flattening** and supports features such as **dynamic column updates**. | [Sink](docs/sink/flink-connector-obkv-hbase2.md)          |

We also provide a command line tool for submitting Flink end-to-end tasks, see the [CLI docs](docs/cli/README.md) for details.

### Other External Projects

There are some community projects which can be used to work with Apache Flink and OceanBase.

|                                Project                                 | OceanBase Compatible Mode |                                                                  Supported Features                                                                  |
|------------------------------------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Flink Connector JDBC](https://github.com/apache/flink-connector-jdbc) | MySQL, Oracle             | [Source + Sink](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/jdbc/)                                              |
| [Flink CDC](https://github.com/ververica/flink-cdc-connectors)         | MySQL, Oracle             | [Source + CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/oceanbase-cdc/)                                |
| [Apache SeaTunnel](https://github.com/apache/seatunnel)                | MySQL, Oracle             | [Source](https://seatunnel.apache.org/docs/connector-v2/source/OceanBase)<br/> [Sink](https://seatunnel.apache.org/docs/connector-v2/sink/OceanBase) |

## Community

Don’t hesitate to ask!

Contact the developers and community at [https://ask.oceanbase.com](https://ask.oceanbase.com) if you need any help.

[Open an issue](https://github.com/oceanbase/flink-connector-oceanbase/issues) if you found a bug.

## Licensing

See [LICENSE](LICENSE) for more information.
