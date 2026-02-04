---
title: "serverless-spark-get-batch-logs"
type: docs
description: >
  A "serverless-spark-get-batch-logs" tool gets Cloud Logging logs for a Serverless Spark batch.
aliases:
  - /resources/tools/serverless-spark-get-batch-logs
---

# serverless-spark-get-batch-logs

The `serverless-spark-get-batch-logs` tool allows you to query log entries for a
specific Serverless Spark (Dataproc Serverless) batch from Google Cloud Logging.
It's compatible with the following sources:

- [serverless-spark](../../sources/serverless-spark.md)

`serverless-spark-get-batch-logs` accepts the following parameters:

- **`batch_id`**: The short batch ID (e.g. `my-batch`).
- **`filter`**: Cloud Logging filter query to append to the resource-specific filter. Common fields:
  `resource.type`, `resource.labels.*`, `logName`, `severity`, `textPayload`, `jsonPayload.*`,
  `protoPayload.*`, `labels.*`, `httpRequest.*`. Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `:`,
  `=~`, `AND`, `OR`, `NOT`.
- **`newestFirst`**: Set to true for newest logs first. Defaults to oldest first.
- **`startTime`**: Start time in RFC3339 format (e.g., `2025-12-09T00:00:00Z`). Defaults to the
  batch creation time.
- **`endTime`**: End time in RFC3339 format (e.g., `2025-12-09T23:59:59Z`). Defaults to now (or batch
  end time if terminal).
- **`verbose`**: Include additional fields (`insertId`, `trace`, `spanId`, `httpRequest`, `labels`,
  `operation`, `sourceLocation`). Defaults to false.
- **`limit`**: Maximum number of log entries to return. Default: `20`.

The tool gets the `project` and `location` from the source configuration.

## Example

```yaml
kind: tools
name: get_my_batch_logs
type: serverless-spark-get-batch-logs
source: my-serverless-spark-source
description: Use this tool to get logs for a serverless spark batch.
```

## Reference

| **field**   | **type** | **required** | **description** |
| ----------- | :------: | :----------: | --------------- |
| type        |  string  |     true     | Must be "serverless-spark-get-batch-logs". |
| source      |  string  |     true     | Name of the source the tool should use. |
| description |  string  |     true     | Description of the tool that is passed to the LLM. |
| authRequired| string[] |    false     | List of auth services required to invoke this tool |
