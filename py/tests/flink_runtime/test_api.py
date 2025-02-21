import pytest
import responses

from sentry_streams.flink_runtime.flink_api import cancel_job, list_jobs

JOBS = {
    "jobs": [
        {"jid": "job_1", "name": "Job 1", "state": "RUNNING"},
        {"jid": "job_2", "name": "Job 2", "state": "FINISHED"},
        {"jid": "job_3", "name": "Job 3", "state": "RUNNING"},
    ]
}


@responses.activate
def test_list_jobs() -> None:
    flink_api_url = "http://localhost:8081"
    responses.add(
        responses.GET,
        f"{flink_api_url}/jobs/overview",
        json=JOBS,
        status=200,
    )

    jobs = list_jobs(flink_api_url)
    assert len(jobs) == 2
    assert jobs[0]["jid"] == "job_1"
    assert jobs[1]["jid"] == "job_3"


@responses.activate
def test_cancel_job() -> None:
    flink_api_url = "http://localhost:8081"
    job_name = "Job 1"

    # Mock the list_jobs response
    responses.add(
        responses.GET,
        f"{flink_api_url}/jobs/overview",
        json=JOBS,
        status=200,
    )

    # Mock the cancel job response
    responses.add(responses.GET, f"{flink_api_url}/jobs/job_1/yarn-cancel", status=200)

    job_ids = cancel_job(flink_api_url, job_name)
    assert len(job_ids) == 1
    assert job_ids[0] == "job_1"


@responses.activate
def test_cancel_job_not_found() -> None:
    flink_api_url = "http://localhost:8081"
    job_name = "Job 4"

    # Mock the list_jobs response
    responses.add(
        responses.GET,
        f"{flink_api_url}/jobs/overview",
        json=JOBS,
        status=200,
    )

    with pytest.raises(ValueError):
        cancel_job(flink_api_url, job_name)
