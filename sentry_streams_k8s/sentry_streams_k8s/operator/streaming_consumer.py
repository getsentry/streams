"""Assemble a StreamingConsumer spec into the PipelineStep inputs, so the operator
renders manifests identical to the sentry-kube macro. Only per-consumer knobs live
in the CRD spec; the scaffolding shared across all SBC pipelines lives here."""

from __future__ import annotations

from typing import Any, NotRequired, TypedDict

from sentry_streams_k8s.pipeline_step import PipelineStep

DEFAULT_CONTAINER_NAME = "pipeline-consumer"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_SEGMENT_ID = 0
DEFAULT_REPLICAS = 1

DEFAULT_AUTO_OFFSET_RESET = "latest"

MODULE_TEMPLATE = "consumer/streaming_platform/pipelines/{name}.py"

METRICS_TYPE = "datadog"
METRICS_HOST = "${envvar:HOST_IP}"
METRICS_PORT = 8128
METRICS_FLUSH_INTERVAL_MS = 5000

APP_FUNCTION = "ingest"
SYSTEM = "streaming_platform"
MIN_READY_SECONDS = 60
ROLLING_UPDATE = {"maxSurge": "25%", "maxUnavailable": "25%"}
SAFE_TO_EVICT_ANNOTATION = {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
SECURITY_CONTEXT = {
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "runAsGroup": 1000,
    "runAsNonRoot": True,
    "runAsUser": 1000,
}

DEFAULT_ENABLE_KAFKA_AUTH = True
DEFAULT_KAFKA_SASL_USERNAME = "super-big-consumers"
DEFAULT_KAFKA_SASL_SECRET_NAME = "service-super-big-consumers"
DEFAULT_KAFKA_SASL_MECHANISM = "SCRAM-SHA-256"
DEFAULT_KAFKA_SECURITY_PROTOCOL = "sasl_plaintext"


class KafkaSourceSpec(TypedDict):
    consumer_group: str
    bootstrap_servers: list[str]
    topic: NotRequired[str]
    auto_offset_reset: NotRequired[str]
    override_params: NotRequired[dict[str, Any]]


class ResourcesSpec(TypedDict):
    cpu_per_process: int
    memory_per_process: int


class OperatorDefaults(TypedDict, total=False):
    service_name: str
    image: str
    sentry_region: str
    application_dsn: str | None
    arroyo_dsn: str | None
    service_account: str
    node_selector: dict[str, str]
    enable_kafka_auth: bool
    kafka_sasl_username: str
    kafka_sasl_secret_name: str


class StreamingConsumerSpec(TypedDict):
    image: str
    service_name: str
    pipeline_name: str
    sentry_region: str
    resources: ResourcesSpec
    kafka: KafkaSourceSpec
    steps: dict[str, Any]

    application_dsn: NotRequired[str | None]
    arroyo_dsn: NotRequired[str | None]

    app_feature: NotRequired[str]
    component: NotRequired[str]
    replicas: NotRequired[int]
    segment_id: NotRequired[int]
    log_level: NotRequired[str]
    enable_liveness_probe: NotRequired[bool]
    with_canary: NotRequired[bool]
    service_account: NotRequired[str]
    node_selector: NotRequired[dict[str, str]]

    enable_kafka_auth: NotRequired[bool]
    kafka_sasl_username: NotRequired[str]
    kafka_sasl_secret_name: NotRequired[str]

    emergency_patch: NotRequired[dict[str, Any]]


DEFAULT_SERVICE_ACCOUNT = "service-super-big-consumer"


def _kafka_override_params(spec: StreamingConsumerSpec) -> dict[str, Any]:
    """Explicit ``kafka.override_params`` wins; else the SASL block when
    ``enable_kafka_auth`` is set (the caller sorts the keys); else empty."""
    kafka = spec["kafka"]
    if "override_params" in kafka:
        return kafka["override_params"]
    if not spec.get("enable_kafka_auth", DEFAULT_ENABLE_KAFKA_AUTH):
        return {}
    username = spec.get("kafka_sasl_username", DEFAULT_KAFKA_SASL_USERNAME)
    return {
        "security.protocol": DEFAULT_KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanism": DEFAULT_KAFKA_SASL_MECHANISM,
        "sasl.username": username,
        "sasl.password": "${envvar:KAFKA_SASL_PASSWORD}",
    }


def build_pipeline_config(spec: StreamingConsumerSpec) -> dict[str, Any]:
    """Assemble the pipeline_config. Key order matches the macro's ``_*_config.yaml``
    so the serialized config (and its configVersion hash) is byte-identical."""
    kafka = spec["kafka"]
    metrics_tag = spec["pipeline_name"]

    kafka_step: dict[str, Any] = {
        "starts_segment": True,
        "bootstrap_servers": kafka["bootstrap_servers"],
        "auto_offset_reset": kafka.get("auto_offset_reset", DEFAULT_AUTO_OFFSET_RESET),
        "consumer_group": kafka["consumer_group"],
        "override_params": dict(sorted(_kafka_override_params(spec).items())),
    }

    steps_config: dict[str, Any] = {"kafka": kafka_step}
    steps_config.update(spec["steps"])

    arroyo: dict[str, Any] = {"write_healthcheck": True}
    arroyo_dsn = spec.get("arroyo_dsn")
    if arroyo_dsn is not None:
        arroyo["sentry_sdk_config"] = {"dsn": arroyo_dsn}

    config: dict[str, Any] = {"env": {}}
    application_dsn = spec.get("application_dsn")
    if application_dsn is not None:
        config["sentry_sdk_config"] = {"dsn": application_dsn}
    config["metrics"] = {
        "type": METRICS_TYPE,
        "host": METRICS_HOST,
        "port": METRICS_PORT,
        "flush_interval_ms": METRICS_FLUSH_INTERVAL_MS,
        "tags": {"pipeline": metrics_tag},
    }
    config["pipeline"] = {
        "adapter_config": {"arroyo": arroyo},
        "segments": [{"steps_config": steps_config}],
    }
    return config


def build_deployment_template(spec: StreamingConsumerSpec) -> dict[str, Any]:
    """Mirror of ``_deployment_template.yaml``; PipelineStep layers its labels,
    replicas, container and volumes on top."""
    labels = {
        "app_feature": spec.get("app_feature", _default_app_feature(spec)),
        "app_function": APP_FUNCTION,
        "component": spec.get("component", _default_component(spec)),
        "system": SYSTEM,
        "service": spec["service_name"],
    }
    pod_spec: dict[str, Any] = {
        "serviceAccountName": spec.get("service_account", DEFAULT_SERVICE_ACCOUNT),
    }
    if "node_selector" in spec:
        pod_spec["nodeSelector"] = spec["node_selector"]

    return {
        "metadata": {"labels": labels},
        "spec": {
            "minReadySeconds": MIN_READY_SECONDS,
            "selector": {"matchLabels": {"component": labels["component"]}},
            "strategy": {"rollingUpdate": dict(ROLLING_UPDATE), "type": "RollingUpdate"},
            "template": {
                "metadata": {
                    "labels": dict(labels),
                    "annotations": dict(SAFE_TO_EVICT_ANNOTATION),
                },
                "spec": pod_spec,
            },
        },
    }


def build_container_template(spec: StreamingConsumerSpec) -> dict[str, Any]:
    env: list[dict[str, Any]] = [
        {"name": "SENTRY_ENVIRONMENT", "value": spec["sentry_region"]},
        {"name": "HOST_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}},
    ]
    if spec.get("enable_kafka_auth", DEFAULT_ENABLE_KAFKA_AUTH):
        secret_name = spec.get("kafka_sasl_secret_name", DEFAULT_KAFKA_SASL_SECRET_NAME)
        env.append(
            {
                "name": "KAFKA_SASL_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "KAFKA_SASL_PASSWORD"}},
            }
        )
    return {"env": env, "securityContext": dict(SECURITY_CONTEXT)}


def _default_component(spec: StreamingConsumerSpec) -> str:
    return f"sbc-streams-{spec['pipeline_name']}-gcs"


def _default_app_feature(spec: StreamingConsumerSpec) -> str:
    return f"analytics_{spec['pipeline_name'].replace('-', '_')}"


def derive_module(pipeline_name: str) -> str:
    """Pipeline module path derived from the pipeline name (see MODULE_TEMPLATE)."""
    return MODULE_TEMPLATE.format(name=pipeline_name.replace("-", "_"))


def build_pipeline_step_context(spec: StreamingConsumerSpec) -> dict[str, Any]:
    """Translate a StreamingConsumer spec into a ``PipelineStep`` context."""
    resources = spec["resources"]
    context: dict[str, Any] = {
        "service_name": spec["service_name"],
        "pipeline_name": spec["pipeline_name"],
        "deployment_template": build_deployment_template(spec),
        "container_template": build_container_template(spec),
        "pipeline_config": build_pipeline_config(spec),
        "pipeline_module": derive_module(spec["pipeline_name"]),
        "image_name": spec["image"],
        "cpu_per_process": resources["cpu_per_process"],
        "memory_per_process": resources["memory_per_process"],
        "segment_id": spec.get("segment_id", DEFAULT_SEGMENT_ID),
        "replicas": spec.get("replicas", DEFAULT_REPLICAS),
        "log_level": spec.get("log_level", DEFAULT_LOG_LEVEL),
        "enable_liveness_probe": spec.get("enable_liveness_probe", True),
        "with_canary": spec.get("with_canary", False),
        "container_name": DEFAULT_CONTAINER_NAME,
    }
    if "emergency_patch" in spec:
        context["emergency_patch"] = spec["emergency_patch"]
    return context


REQUIRED_FIELDS = ("service_name", "pipeline_name", "image", "sentry_region", "resources", "kafka")
REQUIRED_KAFKA_FIELDS = ("consumer_group", "bootstrap_servers")


def validate(spec: StreamingConsumerSpec) -> None:
    """Check required merged fields, then defer to ``PipelineStep.validate_context``
    for the same schema/liveness checks the macro applies."""
    missing = [f for f in REQUIRED_FIELDS if f not in spec]
    if "steps" not in spec:
        missing.append("steps")
    if missing:
        raise ValueError(
            f"StreamingConsumer is missing required field(s): {', '.join(sorted(missing))}. "
            f"Set them on the CR .spec or in the operator defaults."
        )
    missing_kafka = [f for f in REQUIRED_KAFKA_FIELDS if f not in spec["kafka"]]
    if missing_kafka:
        raise ValueError(f"StreamingConsumer .spec.kafka is missing: {', '.join(missing_kafka)}")

    PipelineStep.validate_context(build_pipeline_step_context(spec))


def render(spec: StreamingConsumerSpec) -> dict[str, Any]:
    """Render to ``configmap`` + ``deployment`` (+ ``canary_deployment`` when
    canary splitting is active), via ``PipelineStep.run``."""
    context = build_pipeline_step_context(spec)
    return PipelineStep().run(context)


def from_crd_spec(
    crd_spec: dict[str, Any],
    *,
    name: str | None = None,
    defaults: OperatorDefaults | dict[str, Any] | None = None,
) -> StreamingConsumerSpec:
    """Merge OperatorDefaults under the CR .spec (CR wins); default
    ``pipeline_name`` to ``name`` (metadata.name) when unset."""
    spec: dict[str, Any] = {**(defaults or {}), **crd_spec}
    if "pipeline_name" not in spec and name is not None:
        spec["pipeline_name"] = name
    return spec  # type: ignore[return-value]
