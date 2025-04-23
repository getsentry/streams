import argparse
import importlib.resources
import sys
from typing import List

import yaml


def _apply_name_and_namespace(rbac_resources: List[dict], name_prefix: str, namespace: str) -> None:
    service_account_name = f"{name_prefix}-serviceaccount"
    role_name = f"{name_prefix}-role"
    cluster_role_name = f"{name_prefix}-clusterrole"
    for resource in rbac_resources:
        name = f"{name_prefix}-{resource['kind'].lower()}"
        resource["metadata"]["name"] = name
        resource["metadata"]["namespace"] = namespace

        if resource["kind"] == "RoleBinding":
            resource["roleRef"] = {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": role_name,
            }
            resource["subjects"] = [
                {
                    "kind": "ServiceAccount",
                    "name": service_account_name,
                }
            ]

        if resource["kind"] == "ClusterRoleBinding":
            resource["roleRef"] = {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": cluster_role_name,
            }
            resource["subjects"] = [
                {
                    "kind": "ServiceAccount",
                    "name": service_account_name,
                    "namespace": namespace,
                }
            ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate k8s RBAC resources for operator to be able to watch and manipulate resources as needed."
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        help="Output target file. Defaults to stdout.",
        default=sys.stdout,
    )
    parser.add_argument(
        "--namespace",
        type=str,
        help="Namespace for RBAC resources (where applicable).",
        required=True,
    )
    parser.add_argument(
        "--name-prefix", type=str, help="Prefix to resource names.", default="streams-operator"
    )
    with (
        importlib.resources.files("sentry_streams")
        .joinpath("k8s/templates/operator_rbac.yaml")
        .open("r") as template_file
    ):
        rbac_resources = [doc for doc in yaml.safe_load_all(template_file)]

    args = parser.parse_args()

    _apply_name_and_namespace(rbac_resources, args.name_prefix, args.namespace)

    # default_flow_style=False - always dump in the block style, never inline
    yaml.dump_all(
        rbac_resources,
        args.output,
        default_flow_style=False,
    )


if __name__ == "__main__":
    main()
