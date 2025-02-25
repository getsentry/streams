from typing import Any, Mapping, Sequence

import requests


def list_jobs(flink_api_url: str) -> Sequence[Mapping[str, Any]]:
    """
    Returns the list of running Flink jobs in the local Flink cluster.

    Args:
        flink_api_url (str): The base URL of the Flink API.
    """
    response = requests.get(f"{flink_api_url}/jobs/overview")
    response.raise_for_status()
    jobs_data = response.json()

    return [job for job in jobs_data["jobs"] if job["state"] == "RUNNING"]


def cancel_job(flink_api_url: str, job_name: str) -> Sequence[str]:
    """
    Cancels a Flink job

    Args:
        flink_api_url (str): The base URL of the Flink API.
        job_name (str): The name of the job to cancel.
    """
    # Get the list of jobs to find the job ID
    jobs = list_jobs(flink_api_url)
    job_ids = [job["jid"] for job in jobs if job["name"] == job_name]
    if not job_ids:
        raise ValueError(f"Job with name {job_name} not found")

    ret = []
    for job_id in job_ids:
        # Cancel the job using the job ID
        response = requests.get(f"{flink_api_url}/jobs/{job_id}/yarn-cancel")
        response.raise_for_status()
        ret.append(job_id)

    return ret
