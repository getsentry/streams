
from sentry_streams_operator.settings import Settings, load_settings_from_envvar


def test_load_from_envvar_defaults() -> None:
    assert Settings() == load_settings_from_envvar()
