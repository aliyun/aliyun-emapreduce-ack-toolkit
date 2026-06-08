# CLAUDE.md

Guidance for working in this repo. Keep it short and current.

## What this is

Client tools for **Aliyun EMR on ACK**: submit Spark jobs and run Spark SQL
through a **Kyuubi Server REST API** with a native-`spark-submit`-like CLI.
Pure Java client; no Spark runtime on the client side.

- `spark-submit/` — the only module. Builds two self-extracting executables:
  - `spark-submit` — submit JAR/PySpark jobs, or run SQL (`-e`/`-f`)
  - `spark-sql` — shortcut for SQL

## Build & test

```bash
cd spark-submit
mvn test                 # unit tests (JUnit 4); reports in target/surefire-reports/
./create-spark-submit.sh # mvn clean package -DskipTests, then wrap the shaded jar
                         # into self-extracting ./spark-submit and ./spark-sql
```

- Java **8** bytecode (`maven.compiler.{source,target}=8`) — must run on JRE 8.
- maven-shade builds an all-in-one jar (mainClass `com.aliyun.emr.ack.SparkSubmit`);
  the wrappers embed it as base64 and extract to `~/.cache/emr-spark-tools/`
  (cache keyed by jar md5).

## Architecture (`spark-submit/src/main/java/com/aliyun/emr/ack/`)

- `SparkSubmit.java` — entry point + all top-level flows: job submit, SQL **Batch**
  mode (default; `SparkSQLCLIDriver` cluster job, returns a Batch ID), SQL
  **Session** mode (`--session`; interactive, returns a result table), status
  polling, log streaming, timeouts/heartbeat. Retry is wired in here.
- `KyuubiClient.java` — Kyuubi REST client (`/batches`, `/sessions`, `/operations`,
  `/files/upload`) + response models. HttpClient built-in retries are **disabled**
  on purpose (see Retry).
- `SparkSubmitParser.java` / `SparkSubmitArgs.java` — CLI parsing. Note `-e`/`-f`
  are parsed globally as SQL mode (not passable as app args).
- `Config.java` — connection config. Priority: **CLI args > system props > env >
  `~/.spark-submit.conf`**.
- `Retry.java` / `HttpStatusException.java` — see below.
- `OssUploader.java` — client-side OSS PUT (signed) used as the large-SQL upload
  fallback.

## Connection retry (added in v0.2.0)

`Retry.java` is a small backoff engine (exponential + full jitter) with two
classification policies — pick by idempotency:

- **`isConnectPhaseOnly`** — for the **non-idempotent batch submit**. Retries
  ONLY connection-establishment failures (ConnectException / ConnectTimeout /
  UnknownHost). Read timeouts, `NoHttpResponseException`, 5xx, 429 are NOT
  retried, so a lost response can never duplicate a Spark job.
- **`isTransientNetwork`** — for **idempotent uploads** (Kyuubi file upload /
  OSS PUT). Retries transient network errors + 5xx/429.

Tuning is via **client-only** conf keys (prefix `spark.submit.retry.`):
`enabled`, `maxAttempts`, `upload.maxAttempts`, `initialBackoffMs`,
`maxBackoffMs`, `multiplier`. User docs: `spark-submit/USER_GUIDE.md`.

**Convention:** any client-only conf MUST be filtered out before the request is
sent to Kyuubi/Spark — see `KyuubiClient.isClientOnlyConf(...)` (matches
`spark.submit.retry.*`). Keep retries application-level (HttpClient retries stay
disabled) so retry behavior is a single, predictable source.

`HttpStatusException` carries the HTTP status code so failures can be classified
without parsing messages.

## Large SQL (>10 KB) upload

The SQL is too big for the K8s pod spec, so it is uploaded and passed as
`-f <remote-uri>`:
1. Strategy 1: `POST /api/v1/files/upload` to Kyuubi (needs the EMR upload
   plugin; 404 → fall back).
2. Strategy 2: client-side OSS PUT via `OssUploader` — needs
   `spark.hadoop.fs.oss.{accessKeyId,accessKeySecret,endpoint}` +
   `spark.kubernetes.file.upload.path`.

> Known gap: `OssUploader` only supports **static AK/SK** (no STS / RAM-role).
> On a zero-trust cluster without the Kyuubi upload plugin, large-SQL upload
> can't authenticate.

## Release

CI is `.github/workflows/build.yml`:
- push **tag `v*`** → builds and publishes a versioned **GitHub Release** with
  the `spark-submit`/`spark-sql` binaries (auto-generated notes).
- push to **`main`** → refreshes the rolling `latest` pre-release.

Versioning is `vX.Y.Z`, driven by the git tag (the pom `<version>` is not used
for release naming). To cut a release: commit to `main`, then
`git tag -a vX.Y.Z -m "..." && git push origin vX.Y.Z`.

## Conventions

- 4-space indent, no trailing whitespace, files end with a newline. There is no
  configured formatter — match the surrounding style; don't bulk-reformat.
- Keep changes scoped; don't mix whitespace cleanup of pre-existing lines into a
  feature diff.
