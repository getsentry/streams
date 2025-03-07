import argparse
import subprocess

from sentry_flink.flink_runtime.flink_api import list_jobs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploys a Sentry Streams Flink application on the local cluster."
    )
    parser.add_argument(
        "--jobmanager",
        "-j",
        type=str,
        default="jobmanager",
        help="The container name of the Flink Job Manager",
    )
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        default="name",
        help="The name of the Flink Job",
    )
    parser.add_argument(
        "--adapter",
        type=str,
        default="sentry_flink.flink.flink_adapter.FlinkAdapter",
        help="The name of the adapter to use",
    )
    parser.add_argument(
        "--jobmanager-address",
        "-a",
        type=str,
        default="localhost:8081",
        help="Host and port of the Flink Job Manager to send API calls",
    )
    parser.add_argument(
        "application",
        type=str,
        help=(
            "The Sentry Streams application file. This has to be relative "
            "to the path mounted in the job manager as the /apps directory."
        ),
    )

    args = parser.parse_args()

    print(f"Job Manager Container: {args.jobmanager}")
    print(f"Job Manager Host:port: {args.jobmanager_address}")
    print(f"Deploying application: {args.application}")

    running_jobs = list_jobs(f"http://{args.jobmanager_address}")
    names = [job["name"] for job in running_jobs]
    if args.name in names:
        print(f"Job {args.name} is already running.")
        return -1

    result = subprocess.run(
        [
            "docker",
            "exec",
            args.jobmanager,
            "/opt/flink/bin/flink",
            "run",
            "--detached",
            "-pyclientexec",
            "/usr/bin/python3.11",
            "--python",
            "/apps/sentry_streams/runner.py",
            "--name",
            args.name,
            f"/apps/{args.application}",
            "--adapter",
            args.adapter,
        ]
    )

    if result.stdout:
        print(result.stdout.decode())
    if result.stderr:
        print(result.stderr.decode())

    if result.returncode == 0:
        print(f"Application {args.application} deployed successfully.")
    else:
        print(f"Failed to deploy application {args.application}.")

    return 0


if __name__ == "__main__":
    main()
