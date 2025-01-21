# Flink Connector OceanBase CLI

English | [简体中文](README_CN.md)

The project is a set of CLI (command line interface) tools that supports submitting Flink jobs to migrate data from other data sources to OceanBase.

## Getting Started

You can get the release packages at [Releases Page](https://github.com/oceanbase/flink-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase-cli)，or get the latest snapshot packages at [Sonatype Snapshot](https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-connector-oceanbase-cli).

You can also manually build it from the source code.

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

## Features

This command line tool supports the following connectors as sources:

| Source Connectors | Supported Data Sources |                   Documentation                   |
|-------------------|------------------------|---------------------------------------------------|
| Flink CDC         | MySQL                  | [Flink CDC Source](flink-cdc/flink-cdc-source.md) |

