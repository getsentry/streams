import argparse
import sys

from sentry_streams.flink_runtime.flink_api import cancel_job


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cancels all the Flink jobs with a given application name"
    )
    parser.add_argument(
        "name",
        type=str,
        help="The name of the Flink Job",
    )

    args = parser.parse_args()

    print(f"Canceling jobs application: {args.name}")

    deleted_ids = cancel_job("http://localhost:8081", args.name)
    print(f"Job {args.name} canceled.")
    for id in deleted_ids:
        print(f"- ID: {id}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
