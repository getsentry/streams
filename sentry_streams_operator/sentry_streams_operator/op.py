import importlib.resources
import logging
import os
from typing import Self

import kopf
import kubernetes
import yaml
from kubernetes.client.exceptions import ApiException as K8sApiException

from sentry_streams_operator.generate import (
    apply_namespace,
    generate_configmap,
    generate_deployments,
)
from sentry_streams_operator.settings import load_settings_from_envvar


class StreamsOperator:
    PIPELINES_CONFIGMAP: str

    def __init__(self, settings):
        self.settings = settings
        self.PIPELINES_CONFIGMAP = os.getenv(
            "STREAMS_PIPELINES_CONFIGMAP",
            "streams-operator-pipelines",
        )
        self.logger = logging.getLogger("streams_operator")

        self.deployment_template = yaml.safe_load(
            importlib.resources.files("sentry_streams_operator")
            .joinpath("templates/deployment.yaml")
            .open("r"),
        )
        self.configmap_template = yaml.safe_load(
            importlib.resources.files("sentry_streams_operator")
            .joinpath("templates/configmap.yaml")
            .open("r"),
        )

        # Default to running in k8s cluster
        if settings.incluster:
            kubernetes.config.load_incluster_config()
        else:
            kubernetes.config.load_kube_config()

        self.core_v1_client = kubernetes.client.CoreV1Api()
        self.apps_v1_client = kubernetes.client.AppsV1Api()

    def configure(self, logger, **_):
        logger.info("Running configure method ...")
        logger.info(f"{self.PIPELINES_CONFIGMAP=}")

        pipelines = self.read_pipelines_configmap()
        self.apply_pipelines(pipelines)

    def read_pipelines_configmap(self):
        api_res = self.core_v1_client.read_namespaced_config_map(
            self.PIPELINES_CONFIGMAP,
            self.settings.namespace,
        )
        self.logger.info(f"{api_res=}")
        self.logger.info(f"{api_res.data=}")

        pipelines = {}
        for filename, config_txt in api_res.data.items():
            pipelines[filename] = yaml.safe_load(config_txt)

        return pipelines

    def apply_pipelines(self, pipelines):
        for pipeline_name, pipeline_config in pipelines.items():
            self.logger.debug(f"Handling {pipeline_name=}")
            cm = generate_configmap(
                config=pipeline_config, configmap_template=self.configmap_template
            )
            apply_namespace(k8s_resources=[cm], namespace=self.settings.namespace)

            self.create_or_patch_configmap(body=cm)

            deployments = generate_deployments(
                config=pipeline_config,
                deployment_template=self.deployment_template,
                image=self.settings.container_image,
                container_name=self.settings.container_name,
            )
            apply_namespace(k8s_resources=deployments, namespace=self.settings.namespace)

            for deployment in deployments:
                self.create_or_patch_deployment(body=deployment)

    def create_or_patch_configmap(self, body):
        name, namespace = body["metadata"]["name"], body["metadata"]["namespace"]
        try:
            self.core_v1_client.read_namespaced_config_map(name=name, namespace=namespace)
        except K8sApiException as exc:
            if exc.status == 404:
                # Create a new configmap.
                self.logger.info(f"Create config map {name=}")
                self.core_v1_client.create_namespaced_config_map(namespace=namespace, body=body)
            else:
                raise
        # config-map already exists patch it
        self.core_v1_client.patch_namespaced_config_map(name=name, namespace=namespace, body=body)

    def create_or_patch_deployment(self, body):
        name, namespace = body["metadata"]["name"], body["metadata"]["namespace"]
        try:
            self.apps_v1_client.read_namespaced_deployment(name=name, namespace=namespace)
        except K8sApiException as exc:
            if exc.status == 404:
                self.logger.info(f"Create deployment {name=}")
                self.apps_v1_client.create_namespaced_deployment(namespace=namespace, body=body)
            else:
                raise
        # do not patch the image
        for container in body["spec"]["template"]["spec"]["containers"]:
            if container["name"] == self.settings.container_name:
                del container["image"]
        self.apps_v1_client.patch_namespaced_deployment(name=name, namespace=namespace, body=body)

    def register_handlers(self):
        kopf.on.startup()(self.configure)
        kopf.on.create("configmap", field="metadata.name", value=self.PIPELINES_CONFIGMAP)(
            self.configure
        )
        kopf.on.update("configmap", field="metadata.name", value=self.PIPELINES_CONFIGMAP)(
            self.configure
        )

    @classmethod
    def from_env(cls) -> Self:
        settings = load_settings_from_envvar()
        return cls(settings)
