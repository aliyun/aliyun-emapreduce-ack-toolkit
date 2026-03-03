# Aliyun EMR on ACK Toolkit

本项目为阿里云 EMR on ACK 平台提供客户端工具集，简化 Spark 作业提交和 SQL 执行流程。

## 项目目的

EMR on ACK Toolkit 旨在提供与 Apache Spark 原生命令行体验兼容的工具，让用户能够：

- 通过 Kyuubi Server 提交 Spark 作业（JAR/PySpark）
- 通过 Kyuubi Server 执行 Spark SQL

## 工具列表

| 目录 | 工具 | 说明 |
|------|------|------|
| `spark-submit/` | spark-submit | Spark 作业提交工具 |
| `spark-submit/` | spark-sql | Spark SQL 执行工具 |

## 快速开始

### 构建工具

进入 spark-submit 目录并运行构建脚本：

```bash
cd spark-submit
./create-spark-submit.sh
```

构建完成后，将生成的可执行文件添加到系统 PATH：

```bash
sudo mv spark-submit spark-sql /usr/local/bin/
```

### 使用示例

```bash
# 提交 Spark 作业
spark-submit --class com.example.Main oss://bucket/app.jar

# 执行 SQL
spark-sql -e "SHOW DATABASES"
```

## 详细文档

- [spark-submit 使用手册](spark-submit/USER_GUIDE.md)

## 环境要求

- Java 8 或更高版本
- Maven 3.6+（仅构建时需要）

## License

Apache License 2.0
