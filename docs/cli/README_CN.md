# Flink Connector OceanBase CLI

[English](README.md) | 简体中文

本项目是一套 CLI（命令行界面）工具，支持提交 Flink 作业将数据从其他数据源迁移到 OceanBase。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-oceanbase-cli) 找到正式的发布版本，或者从 [Sonatype Snapshot](https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-connector-oceanbase-cli) 获取最新的快照版本。

您也可以通过源码构建的方式获得程序包。

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

## 功能

此命令行工具支持以下连接器作为源端：

|   源端连接器   | 支持的数据源 |                        文档                        |
|-----------|--------|--------------------------------------------------|
| Flink CDC | MySQL  | [Flink CDC 源端](flink-cdc/flink-cdc-source_cn.md) |

