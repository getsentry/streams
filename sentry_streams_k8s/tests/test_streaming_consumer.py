from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
import yaml

from sentry_streams_k8s.operator.streaming_consumer import (
    OperatorDefaults,
    build_pipeline_config,
    build_pipeline_step_context,
    derive_module,
    from_crd_spec,
    render,
    validate,
)
from sentry_streams_k8s.pipeline_step import compute_config_version

CRD_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "sentry_streams_k8s"
    / "operator"
    / "manifests"
    / "crd.yaml"
)

EXAMPLE_APPLICATION_DSN = "https://examplePublicKey@o0.ingest.sentry.io/0"
EXAMPLE_ARROYO_DSN = "https://exampleRuntimeKey@o0.ingest.sentry.io/1"
ERRORS_BROKERS = ["broker-0:9092", "broker-1:9092", "broker-2:9092"]


def errors_spec(**overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "service_name": "super-big-consumers",
        "pipeline_name": "errors",
        "image": "example.dev/super-big-consumers/image:latest",
        "sentry_region": "us",
        "application_dsn": EXAMPLE_APPLICATION_DSN,
        "arroyo_dsn": EXAMPLE_ARROYO_DSN,
        "replicas": 16,
        "with_canary": True,
        "resources": {"cpu_per_process": 1000, "memory_per_process": 2000},
        "app_feature": "analytics_errors",
        "component": "sbc-streams-errors-gcs",
        "service_account": "service-super-big-consumer-test-us",
        "node_selector": {"cloud.google.com/compute-class": "general-purpose"},
        "enable_kafka_auth": False,
        "kafka": {
            "topic": "events",
            "consumer_group": "pipeline-sbc-errors",
            "auto_offset_reset": "latest",
            "bootstrap_servers": ERRORS_BROKERS,
        },
        "steps": {
            "batched_messages": {"batch_size": 3000, "batch_timedelta": {"seconds": 1}},
            "batch_parser": {
                "starts_segment": True,
                "parallelism": {
                    "multi_process": {"processes": 12, "batch_size": 1, "batch_time": 2}
                },
            },
            "gcs_sink": {"bucket": "sentry-kafka-messages-test-streaming-us", "threads": 8},
        },
    }
    spec.update(overrides)
    return spec


def test_errors_pipeline_config_matches_materialized() -> None:
    """The assembled pipeline_config is byte-identical (same configVersion)."""
    config = build_pipeline_config(errors_spec())

    assert config == {
        "env": {},
        "sentry_sdk_config": {"dsn": EXAMPLE_APPLICATION_DSN},
        "metrics": {
            "type": "datadog",
            "host": "${envvar:HOST_IP}",
            "port": 8128,
            "flush_interval_ms": 5000,
            "tags": {"pipeline": "errors"},
        },
        "pipeline": {
            "adapter_config": {
                "arroyo": {
                    "write_healthcheck": True,
                    "sentry_sdk_config": {"dsn": EXAMPLE_ARROYO_DSN},
                }
            },
            "segments": [
                {
                    "steps_config": {
                        "kafka": {
                            "starts_segment": True,
                            "bootstrap_servers": ERRORS_BROKERS,
                            "auto_offset_reset": "latest",
                            "consumer_group": "pipeline-sbc-errors",
                            "override_params": {},
                        },
                        "batched_messages": {
                            "batch_size": 3000,
                            "batch_timedelta": {"seconds": 1},
                        },
                        "batch_parser": {
                            "starts_segment": True,
                            "parallelism": {
                                "multi_process": {
                                    "processes": 12,
                                    "batch_size": 1,
                                    "batch_time": 2,
                                }
                            },
                        },
                        "gcs_sink": {
                            "bucket": "sentry-kafka-messages-test-streaming-us",
                            "threads": 8,
                        },
                    }
                }
            ],
        },
    }


def test_errors_configmap_matches_materialized() -> None:
    result = render(errors_spec())
    configmap = result["configmap"]

    assert configmap["metadata"]["name"] == "super-big-consumers-pipeline-errors"
    parsed = json.loads(configmap["data"]["pipeline_config.yaml"])
    expected_config = build_pipeline_config(errors_spec())
    assert parsed == expected_config
    assert result["deployment"]["spec"]["template"]["metadata"]["annotations"][
        "configVersion"
    ] == compute_config_version(expected_config)


def test_errors_deployment_matches_materialized() -> None:
    result = render(errors_spec())
    deployment = result["deployment"]
    canary = result["canary_deployment"]

    assert deployment["metadata"]["name"] == "super-big-consumers-pipeline-errors-0"
    assert deployment["spec"]["replicas"] == 15
    assert canary["metadata"]["name"] == "super-big-consumers-pipeline-errors-0-canary"
    assert canary["spec"]["replicas"] == 1

    labels = deployment["metadata"]["labels"]
    assert labels == {
        "app_feature": "analytics_errors",
        "app_function": "ingest",
        "component": "sbc-streams-errors-gcs",
        "env": "primary",
        "pipeline": "errors",
        "pipeline-app": "consumerstreaming-platformpipelineserrors-py",
        "service": "super-big-consumers",
        "system": "streaming_platform",
    }
    assert deployment["spec"]["selector"]["matchLabels"] == {
        "component": "sbc-streams-errors-gcs",
        "env": "primary",
        "pipeline": "errors",
        "pipeline-app": "consumerstreaming-platformpipelineserrors-py",
        "service": "super-big-consumers",
    }
    assert deployment["spec"]["template"]["metadata"]["labels"] == labels

    pod_spec = deployment["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "service-super-big-consumer-test-us"
    assert pod_spec["nodeSelector"] == {"cloud.google.com/compute-class": "general-purpose"}
    assert deployment["spec"]["minReadySeconds"] == 60

    container = pod_spec["containers"][0]
    assert container["name"] == "pipeline-consumer"
    assert container["command"] == ["python", "-m", "sentry_streams.runner"]
    assert container["args"] == [
        "-n",
        "errors",
        "--log-level",
        "INFO",
        "--adapter",
        "rust_arroyo",
        "--segment-id",
        "0",
        "--config",
        "/etc/pipeline-config/pipeline_config.yaml",
        "consumer/streaming_platform/pipelines/errors.py",
    ]
    assert container["resources"]["requests"]["cpu"] == "12000m"
    assert container["resources"]["requests"]["memory"] == "24000Mi"
    assert container["resources"]["limits"]["memory"] == "24000Mi"

    env_names = [e["name"] for e in container["env"]]
    assert env_names == ["SENTRY_ENVIRONMENT", "HOST_IP"]

    volume_names = {v["name"] for v in pod_spec["volumes"]}
    assert volume_names == {"pipeline-config", "dshm", "liveness-health"}


def test_selector_is_subset_of_template_labels() -> None:
    """Kubernetes rejects a Deployment whose selector.matchLabels is not a subset
    of spec.template.metadata.labels. This invariant can only be enforced by the
    API server, so guard it here to catch regressions without a cluster."""
    result = render(errors_spec(with_canary=True))
    for key in ("deployment", "canary_deployment"):
        spec = result[key]["spec"]
        selector = spec["selector"]["matchLabels"]
        template_labels = spec["template"]["metadata"]["labels"]
        missing = set(selector) - set(template_labels)
        assert not missing, f"{key}: selector labels {missing} absent from template labels"
        assert all(template_labels[k] == v for k, v in selector.items())


def test_kafka_auth_on_by_default() -> None:
    """Matches the SBC macro (values.get('enable_kafka_auth', True)): auth is on
    unless a region/CR opts out. Guards the default from silently flipping."""
    spec = errors_spec()
    del spec["enable_kafka_auth"]
    env_names = [e["name"] for e in build_pipeline_step_context(spec)["container_template"]["env"]]
    assert "KAFKA_SASL_PASSWORD" in env_names
    override_params = build_pipeline_config(spec)["pipeline"]["segments"][0]["steps_config"][
        "kafka"
    ]["override_params"]
    assert override_params["sasl.password"] == "${envvar:KAFKA_SASL_PASSWORD}"


def test_kafka_auth_adds_override_params_and_secret_env() -> None:
    spec = errors_spec(enable_kafka_auth=True)
    config = build_pipeline_config(spec)
    kafka_step = config["pipeline"]["segments"][0]["steps_config"]["kafka"]
    assert kafka_step["override_params"] == {
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": "super-big-consumers",
        "sasl.password": "${envvar:KAFKA_SASL_PASSWORD}",
    }
    assert list(kafka_step["override_params"]) == sorted(kafka_step["override_params"])

    container = render(spec)["deployment"]["spec"]["template"]["spec"]["containers"][0]
    sasl_env = next(e for e in container["env"] if e["name"] == "KAFKA_SASL_PASSWORD")
    assert sasl_env["valueFrom"]["secretKeyRef"]["name"] == "service-super-big-consumers"


def test_explicit_override_params_win_over_auth() -> None:
    spec = errors_spec(enable_kafka_auth=True)
    spec["kafka"]["override_params"] = {"client.id": "custom"}
    config = build_pipeline_config(spec)
    assert config["pipeline"]["segments"][0]["steps_config"]["kafka"]["override_params"] == {
        "client.id": "custom"
    }


def test_dsns_are_not_hardcoded_and_optional() -> None:
    """DSNs come from the spec; omitting them drops the sentry_sdk_config blocks."""
    config = build_pipeline_config(errors_spec())
    assert config["sentry_sdk_config"] == {"dsn": EXAMPLE_APPLICATION_DSN}
    assert config["pipeline"]["adapter_config"]["arroyo"]["sentry_sdk_config"] == {
        "dsn": EXAMPLE_ARROYO_DSN
    }

    spec = errors_spec()
    del spec["application_dsn"]
    del spec["arroyo_dsn"]
    config = build_pipeline_config(spec)
    assert "sentry_sdk_config" not in config
    arroyo = config["pipeline"]["adapter_config"]["arroyo"]
    assert arroyo == {"write_healthcheck": True}


def test_defaults_applied() -> None:
    """Optional fields fall back to shared defaults (Q2)."""
    minimal: dict[str, Any] = {
        "service_name": "super-big-consumers",
        "pipeline_name": "replays",
        "image": "image:latest",
        "sentry_region": "us",
        "application_dsn": EXAMPLE_APPLICATION_DSN,
        "resources": {"cpu_per_process": 500, "memory_per_process": 1000},
        "kafka": {
            "consumer_group": "g",
            "bootstrap_servers": ["broker-0:9092"],
        },
        "steps": {"gcs_sink": {"bucket": "b", "threads": 2}},
    }
    context = build_pipeline_step_context(minimal)
    assert context["cpu_per_process"] == 500
    assert context["memory_per_process"] == 1000
    assert context["replicas"] == 1
    assert context["segment_id"] == 0
    assert context["log_level"] == "INFO"
    assert context["enable_liveness_probe"] is True
    assert context["with_canary"] is False
    assert context["container_name"] == "pipeline-consumer"

    deployment_template = context["deployment_template"]
    assert (
        deployment_template["spec"]["template"]["spec"]["serviceAccountName"]
        == "service-super-big-consumer"
    )
    assert deployment_template["metadata"]["labels"]["component"] == "sbc-streams-replays-gcs"
    assert deployment_template["metadata"]["labels"]["app_feature"] == "analytics_replays"
    assert [e["name"] for e in context["container_template"]["env"]] == [
        "SENTRY_ENVIRONMENT",
        "HOST_IP",
        "KAFKA_SASL_PASSWORD",
    ]

    config = build_pipeline_config(minimal)
    assert config["metrics"]["tags"]["pipeline"] == "replays"
    assert (
        config["pipeline"]["segments"][0]["steps_config"]["kafka"]["auto_offset_reset"] == "latest"
    )
    assert config["pipeline"]["segments"][0]["steps_config"]["kafka"]["override_params"] == {
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": "super-big-consumers",
        "sasl.password": "${envvar:KAFKA_SASL_PASSWORD}",
    }

    validate(minimal)  # type: ignore[arg-type]


OPERATOR_DEFAULTS: dict[str, Any] = {
    "service_name": "super-big-consumers",
    "image": "example.dev/super-big-consumers/image:latest",
    "sentry_region": "us",
    "application_dsn": EXAMPLE_APPLICATION_DSN,
    "arroyo_dsn": EXAMPLE_ARROYO_DSN,
    "service_account": "service-super-big-consumer",
    "node_selector": {"cloud.google.com/compute-class": "general-purpose"},
}


def test_derive_module() -> None:
    assert derive_module("errors") == "consumer/streaming_platform/pipelines/errors.py"
    assert derive_module("items-log") == "consumer/streaming_platform/pipelines/items_log.py"


def test_minimal_cr_merges_operator_defaults() -> None:
    """A minimal CR + operator defaults renders the full consumer."""
    cr_spec: dict[str, Any] = {
        "replicas": 16,
        "with_canary": True,
        "resources": {"cpu_per_process": 1000, "memory_per_process": 2000},
        "app_feature": "analytics_errors",
        "service_account": "service-super-big-consumer-test-us",
        "kafka": {
            "topic": "events",
            "consumer_group": "pipeline-sbc-errors",
            "bootstrap_servers": ERRORS_BROKERS,
        },
        "steps": {"gcs_sink": {"bucket": "b", "threads": 8}},
    }
    spec = from_crd_spec(cr_spec, name="errors", defaults=OPERATOR_DEFAULTS)

    assert spec["service_name"] == "super-big-consumers"
    assert spec["image"] == "example.dev/super-big-consumers/image:latest"
    assert spec["sentry_region"] == "us"
    assert spec["pipeline_name"] == "errors"
    assert spec["service_account"] == "service-super-big-consumer-test-us"

    deployment = render(spec)["deployment"]
    assert deployment["metadata"]["name"] == "super-big-consumers-pipeline-errors-0"
    assert "consumer/streaming_platform/pipelines/errors.py" in (
        deployment["spec"]["template"]["spec"]["containers"][0]["args"]
    )
    pod_spec = deployment["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "service-super-big-consumer-test-us"
    assert deployment["metadata"]["labels"]["component"] == "sbc-streams-errors-gcs"


def test_cr_field_overrides_operator_default() -> None:
    spec = from_crd_spec(
        {
            "service_name": "other-service",
            "resources": {"cpu_per_process": 1, "memory_per_process": 1},
            "kafka": {"consumer_group": "g", "bootstrap_servers": ["b:9092"]},
            "steps": {},
        },
        name="p",
        defaults=OPERATOR_DEFAULTS,
    )
    assert spec["service_name"] == "other-service"


def test_validate_requires_merged_fields() -> None:
    spec = from_crd_spec(
        {
            "resources": {"cpu_per_process": 1, "memory_per_process": 1},
            "kafka": {"consumer_group": "g", "bootstrap_servers": ["b:9092"]},
            "steps": {},
        },
        name="p",
    )
    with pytest.raises(ValueError, match="missing required field"):
        validate(spec)


def test_validate_rejects_invalid_log_level() -> None:
    spec = errors_spec(log_level="TRACE")
    with pytest.raises(ValueError, match="Invalid log_level"):
        validate(spec)  # type: ignore[arg-type]


def test_operator_default_fields_have_no_crd_default() -> None:
    """No OperatorDefaults field may declare a CRD ``default:``.

    The apiserver injects CRD defaults into the CR's .spec before from_crd_spec
    merges operator defaults underneath it, so an injected default would always
    win and the operator-level value could never take effect. Defaults for these
    fields live in streaming_consumer.py instead, applied after the merge.
    """
    crd = yaml.safe_load(CRD_PATH.read_text())
    spec_props = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"][
        "properties"
    ]
    offenders = [
        field
        for field in OperatorDefaults.__annotations__
        if field in spec_props and "default" in spec_props[field]
    ]
    assert offenders == [], (
        "OperatorDefaults fields must not carry a CRD default (it would shadow the "
        f"operator-level value): {offenders}"
    )


def test_operator_defaults_survive_when_cr_omits_field() -> None:
    """service_account / enable_kafka_auth from operator defaults apply when the
    CR omits them — the behavior the CRD-default removal protects."""
    defaults: dict[str, Any] = {
        **OPERATOR_DEFAULTS,
        "service_account": "service-region-specific",
        "enable_kafka_auth": True,
    }
    cr_spec: dict[str, Any] = {
        "resources": {"cpu_per_process": 1, "memory_per_process": 1},
        "kafka": {"consumer_group": "g", "bootstrap_servers": ["b:9092"]},
        "steps": {},
    }
    spec = from_crd_spec(cr_spec, name="p", defaults=defaults)
    assert spec["service_account"] == "service-region-specific"
    assert spec["enable_kafka_auth"] is True
