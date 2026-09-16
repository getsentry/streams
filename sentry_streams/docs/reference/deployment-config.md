# Deployment configuration

The deployment config is a YAML file, validated against a JSON schema, holding everything
that varies between deployments of the same application. Its purpose is to keep
infrastructure concerns out of product code: the same application file, deployed with a
different config, reads from a different topic, runs with different parallelism, and reports
to a different metrics backend.

This is the single description of the file. Other documents link here rather than repeating
it.

- Schema: `sentry_streams/config.json`
- Types: `sentry_streams/deployment_config/config_types.py`
- Examples: `sentry_streams/deployment_config/*.yaml`

## Structure

```yaml
env: {}                      # general environment settings
metrics:                     # metrics backend, both languages
  type: log
  period_sec: 5
  tags:
    pipeline: errors
sentry_sdk_config:           # error reporting
  dsn: "${envvar:SENTRY_DSN}"
pipeline:
  adapter_config:            # settings meaningful to one runtime only
    arroyo:
      write_healthcheck: true
  segments:                  # deployment units; --segment-id selects one
    - steps_config:          # keyed by step name
        myinput:
          starts_segment: True
          bootstrap_servers: ["127.0.0.1:9092"]
        parser:
          starts_segment: True
          parallelism:
            multi_process:
              processes: 4
              batch_size: 1000
              batch_time: 0.2
        mysink:
          starts_segment: True
          bootstrap_servers: ["127.0.0.1:9092"]
```

## Environment variables

Any value may reference an environment variable with a `${envvar:NAME}` placeholder, resolved
when the config is loaded, so secrets and per-environment endpoints need not live in the file.

```yaml
override_params:
  max.poll.interval.ms: "${envvar:MAX_POLL_INTERVAL_MS}"
```

## Segments

The word has two meanings. Both are in the [glossary](../architecture/README.md#glossary);
this is how each appears in the file.

**`pipeline.segments` is a list.** Each entry is a deployment unit with its own `steps_config`,
selected at run time with `--segment-id`. The adapter only ever sees the configuration of the
segment it is running. The Kubernetes integration deploys one workload per entry.

**`starts_segment: True` inside a `steps_config`** marks a boundary *within* what a single
process runs: it is where the adapter stops fusing consecutive maps and where a new
parallelism setting takes effect.

Because the Rust adapter builds a single consumer per process, the list usually has one entry
and the interesting segmentation is the one done by `starts_segment`.

### Parallelism

Declared on the step that starts a segment; configuring it mid-chain is rejected.

```yaml
parser:
  starts_segment: True
  parallelism:
    multi_process:
      processes: 4
      batch_size: 1000
      batch_time: 0.2
```

The fused function of the segment becomes the function executed in the pool, which is why it
must be picklable — a module-level function pickles, a closure or local function does not, and
the check runs at build time even when multiprocessing is off.

## Per-step configuration

`steps_config` is keyed by step name. Keys fall into two groups, which is worth knowing when a
value appears to be ignored:

**Read by the step**, in its `override_config()`:

| Step | Keys |
| --- | --- |
| `StreamSource` | `topic`, `consumer_group` |
| `StreamSink` | `topic` |
| `GCSSink` | `bucket`, `parallelism.threads` |
| `Batch` | `batch_size`, `batch_timedelta` (a mapping of `timedelta` kwargs) |
| `DevNullSink` | `batch_size`, `batch_time_ms`, `average_sleep_time_ms`, `max_sleep_time_ms` |

**Read by the adapter**, not by the step: `starts_segment`, `parallelism`,
`bootstrap_servers`, `override_params` (passed through to the Kafka client), and `dlq`.

A source keeps the **logical** stream name written in the application for schema and codec
lookup even when `topic` is overridden: the logical name identifies the data, the topic
identifies where it lives in this environment.

Adding a new key for a new step needs **no schema change** — `steps_config` entries allow
additional properties. Document the keys in the step's docstring and add a row above.

## Metrics

```yaml
metrics:
  type: datadog
  host: 127.0.0.1
  port: 8125
  tags:
    environment: production
  flush_interval_ms: 1000
```

| Type | Behaviour |
| --- | --- |
| `datadog` | Sends to a DogStatsD agent over UDP |
| `log` | Writes each metric to the log, with a configurable `period_sec`. Useful locally |
| `dummy` | Discards. The default when no `metrics` block is present, and what tests use |

The runner adds the pipeline name as a default tag and configures both languages from this one
block: the Python backend plus an adapter installed into the Python Arroyo library, and — via
`PyMetricConfig` — a DogStatsD exporter and a recorder for Rust Arroyo. Note the asymmetry:
the Rust side only has a real backend for `datadog`, so with `log` or `dummy` the Rust metrics
are not produced.

What the metrics mean is in [Metrics](../architecture/rust-arroyo/metrics.md).

## Adapter configuration

`pipeline.adapter_config` holds settings meaningful to one runtime only. For the Arroyo
adapters:

| Key | Effect |
| --- | --- |
| `arroyo.write_healthcheck` | Adds the `HealthCheck` strategy, which touches a file on every poll for the Kubernetes liveness probe |
| `arroyo.sentry_sdk_config` | Sentry SDK settings for the adapter |

## Dead letter queue

A `dlq` block on a source configures the DLQ for that consumer: `topic`, `bootstrap_servers`,
`override_params`. **With no DLQ configured, an invalid message stops processing** — the
consumer logs this at startup. Current DLQ limits are Arroyo's defaults (no rate limit, no cap
on buffered messages);

## The seam with `sentry_streams_k8s`

The `sentry_streams_k8s` package renders the Kubernetes objects that run a pipeline. The
contract between the two packages is this file:

- One **workload per `pipeline.segments` entry**, each started with the matching
  `--segment-id`.
- The config file itself is delivered as a **ConfigMap**, rendered by the sentry-kube consumer
  macro (or by the experimental operator) alongside the Deployment.
- `arroyo.write_healthcheck` is what makes the liveness probe meaningful, so it and the probe
  have to be enabled together.

See `sentry_streams_k8s/README.md` for the rendering side.

## Example files

| File | Shows |
| --- | --- |
| `simple_map_filter.yaml` | The minimum: source and sink bootstrap servers |
| `parallel_processing.yaml` | Segments and `multi_process` parallelism |
| `envvars.yaml` | `${envvar:...}` placeholders and `override_params` |
| `simple_batching.yaml` | `Batch` configuration |
| `gcs_sink.yaml` | GCS sink configuration |
| `devnull_benchmark.yaml` | Benchmarking with `DevNullSink` |
| `blq.yaml` | The branching `blq.py` example: broadcast, router, several Kafka sinks |
