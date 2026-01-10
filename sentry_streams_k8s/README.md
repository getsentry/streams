# Sentry Streams K8s

Kubernetes integration for Sentry Streams.

Sentry Streams contains the streaming platform. That is everything that runs
the streaming applications. Sentry Streams is agnostic to the way the application
is deployed in production or in any environment.

This package instead provides the automation and the infrastructure to deploy
Sentry Streams in a Kubernetes environment.

At the moment it targets Sentry production environment that is based on
[sentry-kube](https://github.com/getsentry/sentry-infra-tools), though in the
future we expect to have something more Sentry agnostic like Helm charts and
a Kubernetes operator.

The main component is the Consumer macro. This is a sentry-kube external macro
that can be used inside the sentry-kube jinja template in the ops repo.
This takes the basic deployment infrastructure (application specific) and
fills it with containers and other resources to run the streaming platform.
