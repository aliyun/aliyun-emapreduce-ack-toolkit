# Spark Submit 客户端使用指南

本指南面向使用 Spark Submit 客户端工具向 Kyuubi Server 提交 Spark 作业的用户。

## 快速开始

### 第一步：下载可执行文件

从项目发布页面下载 `spark-submit` 可执行文件，并赋予执行权限：

```bash
# 下载 spark-submit 文件
# 赋予执行权限
chmod +x spark-submit
```

### 第二步：添加到系统 PATH（可选但推荐）

将 `spark-submit` 添加到系统 PATH 后，您可以在任何目录直接使用 `spark-submit` 命令，而不需要输入完整路径。

#### 方法一：移动到系统目录（推荐）

```bash
# 将 spark-submit 移动到 /usr/local/bin（需要管理员权限）
sudo mv spark-submit /usr/local/bin/

# 验证
which spark-submit
spark-submit --help
```

#### 方法二：创建符号链接

```bash
# 创建符号链接到系统目录
sudo ln -s $(pwd)/spark-submit /usr/local/bin/spark-submit

# 验证
which spark-submit
spark-submit --help
```

#### 方法三：添加到用户 PATH

如果您没有管理员权限，可以将 `spark-submit` 所在目录添加到用户的 PATH：

```bash
# 1. 创建本地 bin 目录（如果不存在）
mkdir -p ~/bin

# 2. 将 spark-submit 移动到 ~/bin
mv spark-submit ~/bin/

# 3. 添加到 PATH（根据您的 shell 选择）
# 对于 bash
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# 对于 zsh
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# 4. 验证
which spark-submit
spark-submit --help
```

**注意**：添加到 PATH 后，您可以直接使用 `spark-submit` 命令，而不需要 `./spark-submit`。

### 第三步：配置 Kyuubi Server 连接信息

创建配置文件 `~/.spark-submit.conf`：

```bash
mkdir -p ~/.kyuubi
cat > ~/.kyuubi/config.properties << EOF
kyuubi.server.url=http://your-kyuubi-server:port
kyuubi.server.username=your-username
kyuubi.server.password=your-password
EOF
```

**请将上述配置中的值替换为您的实际 Kyuubi Server 信息。**

**验证配置**
运行以下命令查看帮助信息，如果配置正确，提交作业时会显示您的 Kyuubi Server 地址：

```bash
./spark-submit --help
```
如果使用默认配置，提交作业时会显示警告信息，提示您进行配置。

### 第四步：提交第一个作业（使用 OSS JAR）

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
./spark-submit --help
# 或
./spark-submit -h
```

