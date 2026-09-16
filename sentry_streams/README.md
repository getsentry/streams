# The Sentry Streaming Platform

Sentry Streams is a distributed platform that, like most streaming platforms,
is designed to handle real-time unbounded data streams.

This is built primarily to allow the creation of Sentry ingestion pipelines
though the api provided is fully independent from the Sentry product and can
be used to build any streaming application.

## Documentation

- **[Architecture](./docs/architecture/README.md)** — the subsystems, the decisions behind
  them, the rules that must hold, and what is unfinished. Start there whether you are building
  on the platform or taking ownership of it.
- [Adding a step](./docs/architecture/guides/adding-a-step.md) — the procedure for extending
  the DSL and the runtime.
- [Deployment configuration](./docs/reference/deployment-config.md) — the config file format.
- [User documentation](https://getsentry.github.io/streams/) — the published Sphinx docs.
- [AGENTS.md](./AGENTS.md) — development environment, tests, type checking.
