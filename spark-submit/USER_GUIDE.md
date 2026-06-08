# Spark Submit 客户端使用指南

本指南面向使用 Spark Submit/SQL 客户端工具向 Kyuubi Server 提交 Spark 作业或执行 SQL 的用户。

## 工具概述

本工具集包含两个命令行工具：

| 工具 | 用途 | 
|------|------|
| `spark-submit` | 提交 Spark 作业（JAR/PySpark）或执行 SQL |
| `spark-sql` | 执行 Spark SQL（`spark-submit -e/-f` 的快捷方式）|

两个工具使用相同的参数格式，功能完全兼容原生 Apache Spark 的命令行体验。

## 快速开始

### 第一步：生成可执行文件

进入项目目录，使用构建脚本生成 `spark-submit` 和 `spark-sql` 可执行文件：

```bash
cd aliyun-emapreduce-ack-toolkit/spark-submit

# 运行构建脚本（需要 Maven 环境）
./create-spark-submit.sh
```

构建完成后，将在当前目录生成以下可执行文件：
- `spark-submit` - Spark 作业提交工具
- `spark-sql` - Spark SQL 执行工具

### 第二步：添加到系统 PATH（可选但推荐）

将工具添加到系统 PATH 后，您可以在任何目录直接使用命令，而不需要输入完整路径。

#### 方法一：移动到系统目录（推荐）

```bash
# 将生成的可执行文件移动到 /usr/local/bin（需要管理员权限）
sudo mv spark-submit spark-sql /usr/local/bin/

# 验证
which spark-submit spark-sql
spark-submit --help
spark-sql --help
```

#### 方法二：创建符号链接

```bash
# 创建符号链接到系统目录
sudo ln -s $(pwd)/spark-submit /usr/local/bin/spark-submit
sudo ln -s $(pwd)/spark-sql /usr/local/bin/spark-sql

# 验证
which spark-submit spark-sql
```

#### 方法三：添加到用户 PATH

如果您没有管理员权限，可以将工具所在目录添加到用户的 PATH：

```bash
# 1. 创建本地 bin 目录（如果不存在）
mkdir -p ~/bin

# 2. 将生成的可执行文件移动到 ~/bin
mv spark-submit spark-sql ~/bin/

# 3. 添加到 PATH（根据您的 shell 选择）
# 对于 bash
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# 对于 zsh
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# 4. 验证
which spark-submit spark-sql
```

**注意**：添加到 PATH 后，您可以直接使用 `spark-submit` 和 `spark-sql` 命令。

### 第三步：配置 Kyuubi Server 连接信息

有两种方式配置 Kyuubi 连接信息：

#### 方式一：命令行参数（推荐用于临时使用）

直接在命令行传入配置，无需创建配置文件：

```bash
spark-sql --kyuubi-url http://your-kyuubi-server:10099 \
          --kyuubi-user your-username \
          --kyuubi-password your-password \
          --history-url http://your-history-server:18080 \
          -e "SHOW DATABASES"
```

#### 方式二：配置文件（推荐用于长期使用）

创建配置文件 `~/.spark-submit.conf`：

```bash
cat > ~/.spark-submit.conf << EOF
kyuubi.server.url=http://your-kyuubi-server:10099
kyuubi.server.username=your-username
kyuubi.server.password=your-password
spark.history.server.url=http://your-history-server:18080
EOF
```

配置文件创建后，后续命令无需再指定连接参数。

#### 配置优先级

配置的优先级（从高到低）：

1. **命令行参数**：`--kyuubi-url`、`--kyuubi-user`、`--kyuubi-password`、`--history-url`
2. **系统属性**：`-Dkyuubi.server.url=...`、`-Dspark.history.server.url=...`
3. **环境变量**：`KYUUBI_SERVER_URL`、`KYUUBI_SERVER_USERNAME`、`KYUUBI_SERVER_PASSWORD`、`SPARK_HISTORY_SERVER_URL`
4. **配置文件**：`~/.spark-submit.conf`

**验证配置**

```bash
spark-submit --help
```

### 第四步：执行第一条 SQL

```bash
# 使用命令行参数
spark-sql --kyuubi-url http://your-kyuubi:10099 \
          --kyuubi-user user --kyuubi-password pwd \
          -e "SHOW DATABASES"

# 或使用配置文件后
spark-sql -e "SHOW DATABASES"
```

### 第五步：提交第一个作业（使用 OSS JAR）

```bash
spark-submit \
  --name my-first-job \
  --class org.apache.spark.examples.SparkPi \
  oss://your-bucket/path/spark-examples_2.12-3.5.7.jar
```

## 提交作业

### 基本命令格式

```bash
./spark-submit [选项] <JAR文件路径> [应用程序参数]
```

### 必需参数

- `--class`：应用程序的主类名（必需）

### 常用选项

| 选项 | 说明 | 示例 |
|------|------|------|
| `--name` | 作业名称 | `--name my-spark-job` |
| `--class` | 主类名（JAR 作业必需，PySpark 可省略） | `--class com.example.MyApp` |
| `--conf` | Spark 配置 | `--conf spark.executor.memory=2g` |
| `--queue` | 队列名称 | `--queue root_queue` |
| `--proxy-user` | 代理用户 | `--proxy-user test` |
| `--driver-memory` | Driver 内存 | `--driver-memory 1g` |
| `--executor-memory` | Executor 内存 | `--executor-memory 2g` |
| `--executor-cores` | Executor CPU 核心数 | `--executor-cores 2` |
| `--num-executors` | Executor 数量 | `--num-executors 5` |
| `--driver-cores` | Driver CPU 核心数 | `--driver-cores 1` |
| `--files` | 分发资源文件 | `--files oss://bucket/file1,oss://bucket/file2` |
| `--py-files` | PySpark 依赖脚本/包 | `--py-files oss://bucket/a.py,oss://bucket/b.py` |
| `--jars` | 额外 JAR 依赖 | `--jars oss://bucket/a.jar,oss://bucket/b.jar` |
| `--archives` | 归档资源（支持 #name） | `--archives oss://bucket/env.tar.gz#env` |
| `--status` | 查询 Batch 状态 | `--status jr-xxxx` |
| `--kill` | 终止 Batch | `--kill jr-xxxx` |
| `--timeout` | 作业超时时间（秒），超时后自动终止并返回退出码 124 | `--timeout 3600` |
| `-e` | 执行内联 SQL 语句 | `-e "SHOW DATABASES"` |
| `-f` | 执行 SQL 文件（支持本地路径和 OSS 远程路径） | `-f /path/to/query.sql` |
| `--session` | SQL Session 模式，返回格式化表格结果（需配合 `-e` 或 `-f` 使用） | `--session -e "SELECT * FROM table"` |
| `--kyuubi-url` | Kyuubi Server 地址 | `--kyuubi-url http://kyuubi:10099` |
| `--kyuubi-user` | Kyuubi 用户名 | `--kyuubi-user admin` |
| `--kyuubi-password` | Kyuubi 密码 | `--kyuubi-password secret` |
| `--history-url` | Spark History Server 地址 | `--history-url http://history:18080` |

### 资源路径

推荐使用 **OSS 路径**：`oss://bucket-name/path/to/file.jar`
- 文件存储在阿里云 OSS 上
- 确保 Kyuubi Server 已配置 OSS 访问凭证
- 示例：`oss://my-bucket/spark/apps/my-app.jar`

### 示例：提交 PySpark 作业

```bash
spark-submit \
  --name pyspark-job \
  --py-files oss://your-bucket/lib1.py,oss://your-bucket/lib2.zip \
  --files oss://your-bucket/conf.yaml \
  oss://your-bucket/jobs/main.py arg1 arg2
```

### 提交成功示例

当作业提交成功时，您会看到类似以下的输出：

```
==========================================
Submitting Spark job to Kyuubi Server
==========================================
Kyuubi Server URL: http://47.96.173.147:10099
Username: kyuubi-server
------------------------------------------
Application Class: org.apache.spark.examples.SparkPi
Resource: oss://your-bucket/path/spark-examples_2.12-3.5.7.jar
Job Name: spark-pi
==========================================

✅ Batch submitted successfully!
------------------------------------------
Batch ID: 176b69b6-f8f4-4e6f-a85a-87d290ee63cd
State: PENDING
Application ID: spark-d99461f259674299bfd3faf71acb902c
Application URL: http://spark-history-server:18080/history/spark-d99461f259674299bfd3faf71acb902c/1/
------------------------------------------
```

**重要信息**：
- **Batch ID**：Kyuubi 分配的批次 ID，用于查询作业状态
- **Application ID**：Spark 应用 ID
- **Application URL**：Spark 应用的 Web UI 地址，可以在浏览器中打开查看作业详情

### 示例 1：提交 Spark Pi 示例（OSS）

```bash
spark-submit \
  --name spark-pi \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  --class org.apache.spark.examples.SparkPi \
  oss://your-bucket/path/spark-examples_2.12-3.5.7.jar
```

## 查看帮助信息

如果您需要查看完整的帮助信息，包括所有支持的选项和配置说明：

```bash
spark-submit --help
spark-sql --help
```

## Spark SQL 模式

SQL 模式支持两种执行方式：

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **Batch 模式**（默认） | 通过 SparkSQLCLIDriver 提交 SQL 作为 Batch 作业 | 生产环境、长时间运行的 SQL 作业 |
| **Session 模式**（`--session`） | 通过 Kyuubi Session API 执行 SQL，返回格式化表格结果 | 交互式查询、需要查看结果集 |

### 执行内联 SQL

使用 `-e` 参数直接执行 SQL：

```bash
# 单条 SQL（Batch 模式，默认）
spark-sql -e "SHOW DATABASES"

# Session 模式（返回格式化表格结果）
spark-sql --session -e "SHOW DATABASES"

# 多条 SQL（分号分隔）
spark-sql -e "USE default; SHOW TABLES; SELECT * FROM my_table LIMIT 10"

# 使用 spark-submit 也可以
spark-submit -e "SELECT 1 + 1 as result"
```

### 执行 SQL 文件

使用 `-f` 参数执行 SQL 文件，支持本地文件路径和远程 URI（如 OSS）：

```bash
# 执行本地 SQL 文件
spark-sql -f /path/to/queries.sql

# 执行远程 SQL 文件（OSS）
spark-sql -f oss://your-bucket/sql/etl_job.sql

# Session 模式执行 SQL 文件
spark-sql --session -f /path/to/queries.sql

# 使用 spark-submit 也可以
spark-submit -f /path/to/etl_job.sql
```

### SQL 模式特性

**Batch 模式（默认）：**
- 通过 SparkSQLCLIDriver 在 cluster 模式下执行 SQL
- SQL 文件在客户端本地读取后传递给 Spark Driver
- **不返回查询结果数据**，仅输出作业日志和执行状态
- 适合生产环境的长时间运行作业（如 ETL、数据导入导出）
- 支持远程 SQL 文件（OSS 等）

**Session 模式（`--session`）：**
- 基于 Kyuubi Session API，一个 Session 中顺序执行多条语句
- 实时流式输出 Operation 日志（来自 Kyuubi Server 和 Spark Engine）
- **返回格式化表格结果**，适合交互式查询
- 30 分钟心跳超时：如果任务无日志/状态更新超过 30 分钟，自动终止
- 适合交互式查询和需要查看结果集的场景

### 超时设置

使用 `--timeout` 参数设置作业超时时间：

```bash
# 设置 1 小时超时
spark-sql -f /path/to/long_query.sql --timeout 3600
```

### 退出码

| 退出码 | 说明 |
|--------|------|
| 0 | 成功 |
| 1 | 失败 |
| 124 | 超时 |
| 130 | 被中断 |

### SQL 执行成功示例

**Session 模式示例（`--session`）：**

```
==========================================
Executing Spark SQL via Kyuubi Server
==========================================
Kyuubi Server URL: http://47.110.75.67:10099
Username: emr-user
SQL statements to execute: 1
------------------------------------------

[2026-03-03 11:04:17] Creating Kyuubi session...
[2026-03-03 11:04:17] Session created: 84f4d87b-9859-4a03-8f0d-5e06b3985293

------------------------------------------
[2026-03-03 11:04:17] [1/1] Executing: SHOW DATABASES
------------------------------------------
...
[2026-03-03 11:04:19] [Status] FINISHED_STATE
+-----------+
| namespace |
+-----------+
| default   |
+-----------+
1 row(s) in set

[2026-03-03 11:04:19] All SQL statements completed successfully.
```

**Batch 模式示例（默认）：**

```
==========================================
Submitting Spark SQL Batch Job to Kyuubi
==========================================
Kyuubi Server URL: http://47.110.75.67:10099
Username: emr-user
------------------------------------------
Mode: Batch (SparkSQLCLIDriver cluster mode)
Class: org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
SQL: SHOW DATABASES
==========================================

✅ Batch submitted successfully!
Batch ID: 176b69b6-f8f4-4e6f-a85a-87d290ee63cd
Application ID: spark-d99461f259674299bfd3faf71acb902c
Application URL: http://spark-history-server:18080/history/spark-d99461f259674299bfd3faf71acb902c/1/

Waiting for job to complete...
------------------------------------------

=== Job Logs ===
...
[Status] RUNNING -> FINISHED

------------------------------------------
Job finished!
Final State: FINISHED
Application ID: spark-d99461f259674299bfd3faf71acb902c

✅ Job completed successfully!
```

## 连接重试（自动）

为应对到 Kyuubi 的瞬时网络抖动（如内网 LB/CLB 偶发的连接超时），**作业提交**与**大 SQL 文件上传**内置了带指数退避（含 full jitter）的自动重试。重试是**客户端行为**，相关配置不会下发给 Kyuubi/Spark。

针对不同操作采用不同策略，兼顾**成功率**与**安全性**：

| 操作 | 重试哪些失败 | 默认次数 | 说明 |
|------|--------------|----------|------|
| 作业提交（batch submit） | 仅"连接建立阶段"失败（连接被拒 / 连接超时 / DNS 解析失败） | 3 | 非幂等：请求一旦可能已送达就**不重试**，避免重复提交作业 |
| 文件上传（大 SQL，>10KB） | 瞬时网络错误 + HTTP 5xx/429 | 4 | 幂等：可安全重试 |

> 作业提交对**读超时、`NoHttpResponseException`、5xx/429 故意不重试**——此时请求可能已被 Kyuubi 接收，重试会造成重复作业。

### 配置项

所有配置均为客户端专属（前缀 `spark.submit.retry.`），可通过 `--conf` 或 `~/.spark-submit.conf` 设置：

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `spark.submit.retry.enabled` | `true` | 总开关，设为 `false` 关闭全部重试 |
| `spark.submit.retry.maxAttempts` | `3` | 作业提交最大尝试次数 |
| `spark.submit.retry.upload.maxAttempts` | `4` | 文件上传最大尝试次数 |
| `spark.submit.retry.initialBackoffMs` | `1000` | 初始退避（毫秒） |
| `spark.submit.retry.maxBackoffMs` | `8000` | 退避上限（毫秒） |
| `spark.submit.retry.multiplier` | `2.0` | 退避倍率 |

### 示例

```bash
# 调大提交重试次数、缩短初始退避
spark-submit -f big_query.sql \
  --conf spark.submit.retry.maxAttempts=5 \
  --conf spark.submit.retry.initialBackoffMs=500

# 关闭重试
spark-submit -e "SELECT 1" --conf spark.submit.retry.enabled=false
```

重试触发时会在 stderr 打印进度（不影响结果输出）：

```
[2026-06-08 10:56:30] submitBatch failed (attempt 1/3): Connect to 10.24.192.117:10099 failed: connect timed out, retrying in 742ms
```

退避等待期间按 `Ctrl-C` 中断会以退出码 `130` 退出。

## 技术说明

### 缓存机制

工具使用 JAR 缓存机制提升启动性能：

- 首次运行时，JAR 会被解压到 `~/.cache/emr-spark-tools/`
- 后续运行直接使用缓存的 JAR，无需重复解压
- 当工具版本更新时，缓存会自动刷新（基于 checksum 校验）

### 清理缓存

如需清理缓存，可执行：

```bash
rm -rf ~/.cache/emr-spark-tools/
```
