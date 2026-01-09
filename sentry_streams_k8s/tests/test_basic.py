"""Basic tests for sentry_streams_k8s."""


def test_import() -> None:
    """Test that the package can be imported."""
    import sentry_streams_k8s

    assert sentry_streams_k8s.__version__ == "0.0.1"
