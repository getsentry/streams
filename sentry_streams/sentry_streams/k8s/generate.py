import argparse
import copy
import importlib.resources
import sys
from typing import Any, List, Mapping

import yaml

from sentry_streams.adapters.stream_adapter import PipelineConfig

K8sDeploymentManifest = Mapping[str, Any]
K8sConfigMapManifest = Mapping[str, Any]


def _merge_labels(
    tpl_labels: Mapping[str, str], seg_labels: Mapping[str, str]
) -> Mapping[str, str]:
    """
    Merge labels. Segment labels overrides the template labels.
    """
    return {**tpl_labels, **seg_labels}


def _apply_namespace(
    k8s_resources: List[K8sConfigMapManifest | K8sDeploymentManifest],
    namespace: str | None = None,
) -> None:
    for k8s_resource in k8s_resources:
        k8s_resource["metadata"]["namespace"] = namespace


def generate_configmap(
    *, config: PipelineConfig, configmap_template: K8sConfigMapManifest
) -> K8sConfigMapManifest:
    configmap = copy.deepcopy(configmap_template)
    pipeline_name = config["pipeline"]["name"]
    configmap["metadata"]["name"] = pipeline_name
    common_labels = {"pipeline": pipeline_name}
    configmap["metadata"]["labels"] = _merge_labels(
        configmap["metadata"].get("labels", {}),
        common_labels,
    )
    configmap["data"]["config"] = yaml.safe_dump(config)
    return configmap


def generate_deployments(
    *,
    config: PipelineConfig,
    deployment_template: K8sDeploymentManifest,
    container_name: str,
    image: str,
) -> List[K8sDeploymentManifest]:
    deployments = []
    pipeline_name = config["pipeline"]["name"]
    common_labels = {"pipeline": pipeline_name}

    # configmap volume and mount
    cm_volume = {
        "name": f"{pipeline_name}-config",
        "configMap": {"name": pipeline_name},
    }
    cm_volume_mount = {
        "name": f"{pipeline_name}-config",
        # TODO: Make mount/path configurable
        "mountPath": f"/etc/{pipeline_name}-config",
        "subPath": "config",
        "readOnly": True,
    }

    for segid, segment in enumerate(config["pipeline"]["segments"]):
        # TODO: sync with PR#77
        # TODO: config as env variable as well?
        labels = copy.deepcopy(common_labels)
        labels["segment"] = str(segid)

        deployment = copy.deepcopy(deployment_template)

        deployment["metadata"]["labels"] = _merge_labels(
            deployment["metadata"].get("labels", {}),
            labels,
        )
        deployment["spec"]["selector"]["matchLabels"] = _merge_labels(
            deployment["spec"]["selector"].get("matchLabels", {}),
            labels,
        )
        deployment["spec"]["template"]["metadata"]["labels"] = _merge_labels(
            deployment["spec"]["template"]["metadata"]["labels"],
            labels,
        )

        deployment["spec"]["replicas"] = segment["parallelism"]
        deployment["metadata"]["name"] = f"{pipeline_name}-{segid}"

        volumes = deployment["spec"]["template"]["spec"].setdefault("volumes", [])
        volumes.append(cm_volume)

        for container in deployment["spec"]["template"]["spec"]["containers"]:
            if container["name"] == container_name:
                container["image"] = image

                env = container.get("env", [])
                env.append({"name": "SEGMENT_ID", "value": str(segid)})
                container["env"] = env

                volume_mounts = container.setdefault("volumeMounts", [])
                volume_mounts.append(cm_volume_mount)

                container["args"] = [
                    "--config",
                    cm_volume_mount["mountPath"],
                ]

        deployments.append(deployment)

    return deployments


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate k8s resources from a sentry_streams deployment config."
    )
    parser.add_argument(
        "--config",
        type=argparse.FileType("r"),
        help="Path to a deployment config file.",
        required=True,
    )
    parser.add_argument(
        "--deployment-template",
        type=argparse.FileType("r"),
        help="Path to a deployment template file.",
        default=importlib.resources.files("sentry_streams")
        .joinpath("k8s/templates/deployment.yaml")
        .open("r"),
    )
    parser.add_argument(
        "--configmap-template",
        type=argparse.FileType("r"),
        help="Path to a configmap template file.",
        default=importlib.resources.files("sentry_streams")
        .joinpath("k8s/templates/configmap.yaml")
        .open("r"),
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        help="Output target file. Defaults to stdout.",
        default=sys.stdout,
    )
    parser.add_argument(
        "--container-name",
        type=str,
        help="Streams segment application container name.",
        default="segment",
    )
    parser.add_argument(
        "--image",
        type=str,
        help="Segment container image.",
        required=True,
    )
    parser.add_argument(
        "--namespace",
        type=str,
        help="Namespace for deployment and configmap.",
    )
    args = parser.parse_args()

    config = yaml.safe_load(args.config)
    deployment_template = yaml.safe_load(args.deployment_template)
    configmap_template = yaml.safe_load(args.configmap_template)

    k8s_resources = generate_deployments(
        config=config,
        deployment_template=deployment_template,
        image=args.image,
        container_name=args.container_name,
    ) + [
        generate_configmap(
            config=config,
            configmap_template=configmap_template,
        )
    ]

    _apply_namespace(k8s_resources, args.namespace)

    # default_flow_style=False - always dump in the block style, never inline
    yaml.dump_all(
        k8s_resources,
        args.output,
        default_flow_style=False,
    )


if __name__ == "__main__":
    main()
