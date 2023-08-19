# OceanBase Connectors for Apache Flink

English | [简体中文](README_CN.md)

[![Build Status](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/maven_build_main.yml/badge.svg?branch=main)](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/maven_build_main.yml)
[![Release](https://img.shields.io/github/release/oceanbase/flink-connector-oceanbase.svg)](https://github.com/oceanbase/flink-connector-oceanbase/releases)
[![License](https://img.shields.io/badge/license-Mulan%20PSL%20v2-green.svg)](LICENSE)

This repository contains the OceanBase connectors for Apache Flink.

## Features

Prerequisites

- JDK 8
- Flink 1.15 or later version

This repository contains connectors as following:

|          Connector          | OceanBase Compatible Mode |               Supported Features                |
|-----------------------------|---------------------------|-------------------------------------------------|
| Flink Connector: OceanBase  | MySQL, Oracle             | [Sink](docs/sink/flink-connector-oceanbase.md)  |
| Flink Connector: OBKV HBase | OBKV HBase                | [Sink](docs/sink/flink-connector-obkv-hbase.md) |

### Other External Projects

There are some community projects which can be used to work with Apache Flink and OceanBase.

|                            Project                             | OceanBase Compatible Mode |                                                                  Supported Features                                                                  |
|----------------------------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Flink CDC](https://github.com/ververica/flink-cdc-connectors) | MySQL, Oracle             | [Source + CDC](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oceanbase-cdc.html)                                        |
| [Apache SeaTunnel](https://github.com/apache/seatunnel)        | MySQL, Oracle             | [Source](https://seatunnel.apache.org/docs/connector-v2/source/OceanBase)<br/> [Sink](https://seatunnel.apache.org/docs/connector-v2/sink/OceanBase) |

## Community

Don’t hesitate to ask!

Contact the developers and community at [https://ask.oceanbase.com](https://ask.oceanbase.com) if you need any help.

[Open an issue](https://github.com/oceanbase/flink-connector-oceanbase/issues) if you found a bug.

## Licensing

See [LICENSE](LICENSE) for more information.
