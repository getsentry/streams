---
name: create-pipeline
description: Creates a new example streaming pipeline including the python definition and the yaml config. Use when the user asks to create a new pipeline.
argument-hint: '[pipeline name] [stream name]'
---

# Create Pipeline

Creates a pipeline definition that contains a streaming source given the name
of the pipeline and the name of the stream.

**Usage**: `/create-pipeline <pipeline name> <stream name>`

## Prerequisites

The pipeline must not already exist in the example directory:

- Check that there is no python file in the `sentry_streams/sentry_streams/examples` directory with the same name as the pipeline name.
- Check that there is no yaml file in the `sentry_streams/sentry_streams/deployment_config` directory with the same name as the pipeline name.
- Neither the pipeline name nor the stream name can contain spaces.

## Instructions

### Step 1 [Create the pipeline python file]

- Create a new python file with the name equal to the pipeline name in `sentry_streams/sentry_streams/examples`
- Import `streaming_source` from `sentry_streams.pipeline.pipeline`
- Create a variable named `pipeline` initialize it with this `streaming_source(name="INPUT NAME", stream_name="STREAM NAME"`,
    where `INPUT NAME` is the the pipeline name concatenated with the word `input` and `STREAM NAME`
    is the name of the stream
- Ensure this structure is correct by loading the module with the python interpreter inside the venv from the `sentry_streams` directory.

### Step 2 [Create the deployment config file]

- Create a new yaml file with the name equal to the pipeline name in `sentry_streams/sentry_streams/deployment_config`.

Use this basic structure

```
env: {}

pipeline:
  adapter_config:
    arroyo:
      write_healthcheck: true
  segments:
    - steps_config:
        INPUT NAME:
          starts_segment: True
          bootstrap_servers: ["127.0.0.1:9092"]
```

Replace `INPUT NAME` with the `INPUT NAME` generated at Step 1
