# OceanBase Connectors for Apache Flink

[English](README.md) | 简体中文

[![Build Status](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/maven_build_main.yml/badge.svg?branch=main)](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/maven_build_main.yml)
[![Release](https://img.shields.io/github/release/oceanbase/flink-connector-oceanbase.svg)](https://github.com/oceanbase/flink-connector-oceanbase/releases)
[![License](https://img.shields.io/badge/license-Mulan%20PSL%20v2-green.svg)](LICENSE)

本仓库包含 OceanBase 的 Flink Connector。

## 功能

运行环境需要准备

- JDK 8
- Flink 1.15 或后续版本

本仓库提供了如下 Connector：

|          Connector          | OceanBase 兼容模式 |                       支持的功能                        |
|-----------------------------|----------------|----------------------------------------------------|
| Flink Connector: OceanBase  | MySQL, Oracle  | [Sink](docs/sink/flink-connector-oceanbase_cn.md)  |
| Flink Connector: OBKV HBase | OBKV HBase     | [Sink](docs/sink/flink-connector-obkv-hbase_cn.md) |

### 其他外部项目

在其他的社区和组织中，也有一些项目可以用于通过 Flink 处理 OceanBase 中的数据。

|                            Project                             | OceanBase 兼容模式 |                                                                      支持的功能                                                                       |
|----------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| [Flink CDC](https://github.com/ververica/flink-cdc-connectors) | MySQL, Oracle  | [Source + CDC](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oceanbase-cdc%28ZH%29.html)                            |
| [Apache SeaTunnel](https://github.com/apache/seatunnel)        | MySQL, Oracle  | [Source](https://seatunnel.apache.org/docs/connector-v2/source/OceanBase), [Sink](https://seatunnel.apache.org/docs/connector-v2/sink/OceanBase) |

## 社区

当你需要帮助时，你可以在 [https://ask.oceanbase.com](https://ask.oceanbase.com) 上找到开发者和其他的社区伙伴。

当你发现项目缺陷时，请在 [issues](https://github.com/oceanbase/flink-connector-oceanbase/issues) 页面创建一个新的 issue。

## 许可证

更多信息见 [LICENSE](LICENSE)。
