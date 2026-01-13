from sentry_streams_k8s.consumer import Consumer


def test_consumer_basic_functionality() -> None:
    # Create a valid context
    context = {
        "deployment_template": {"kind": "Deployment"},
        "pipeline_module": "my_module",
        "pipeline_config_file": "config.yaml",
    }

    # Test that validate_context doesn't raise with valid context
    Consumer.validate_context(context)

    # Test that run returns a dictionary
    consumer = Consumer()
    result = consumer.run(context)
    assert isinstance(result, dict)
