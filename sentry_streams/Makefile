
.PHONY: buld-operator-image
build-operator-image:
	cp pyproject.toml docker/src/
	cp -r sentry_streams docker/src/
	find docker/src/ -type f -name "*.py[co]" -delete
	find docker/src/ -type d -name "__pycache__" -delete
	cd docker && docker buildx build -f Dockerfile.operator -t streams-k8s-operator:latest .
	kind load docker-image streams-k8s-operator:latest

.PHONY: deploy
deploy:
	kubectl apply -f k8s/operator.deploy.yaml
	kubectl -n streams-operator rollout restart deploy streams-operator
	kubectl -n streams-operator get pod


.PHONY: logs
logs:
	kubectl -n streams-operator logs deployment/streams-operator

.PHONY: k8s-delete
k8s-delete:
	kubectl -n streams-operator delete cm transactions-pipeline
	kubectl -n streams-operator delete cm events-pipeline
	kubectl -n streams-operator delete deploy events-pipeline-1
	kubectl -n streams-operator delete deploy events-pipeline-0
	kubectl -n streams-operator delete deploy transactions-pipeline-0

.PHONY: k8s-list
k8s-list:
	kubectl -n streams-operator get cm
	kubectl -n streams-operator get deploy
	kubectl -n streams-operator get pod

.PHONY: single-run
single-run:
	STREAMS_OPERATOR_INCLUSTER=false python -m sentry_streams.k8s.operator.handlers
