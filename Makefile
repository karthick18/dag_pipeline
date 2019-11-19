help:
	@echo "The follow make targets are supported"
	@echo "    build         - build the dag_pipeline experiment"
	@echo "    run           - run, but don't build executable, of dag_pipeline experiment"
	@echo "    lint          - perform static code analysis on experiment"
	@echo "    fmt           - format the source code for the experiment"
	@echo "    mod           - verify and tidy the go module dependencies"
	@echo "    docker-build  - build experiment as a docker image"
	@echo "    clean         - remove build artifacts from local directory"
	@echo "    help          - display this information"
	@echo ""

DOCKER_REPOSITORY ?=
DOCKER_TAG ?= latest
DOCKER_ARGS ?=

.PHONY: build
build:
	go build -o dag_pipeline ./cmd/dag_pipeline/dag_pipeline.go

docker-build:
	docker build --rm -f Dockerfile -t $(DOCKER_REPOSITORY)dag_pipeline:$(DOCKER_TAG) $(DOCKER_ARGS) .

have-compose:
ifeq (, $(shell which docker-compose))
$(error docker-compose is not installed, please see install instructions at https://docs.docker.com/compose/install/)
endif

compose-up: have-compose
	docker-compose -p dag_pipeline -f docker-compose.yaml up -d

compose-down: have-compose
	docker-compose -p dag_pipeline -f docker-compose.yaml down

compose-logs: have-compose
	docker-compose -p dag_pipeline -f docker-compose.yaml logs -f

compose-ps: have-compose
	docker-compose -p dag_pipeline -f docker-compose.yaml ps

run:
	@go run $(GO_ARGS)  ./cmd/dag_pipeline/dag_pipeline.go

lint:
	go vet ./...

mod:
	go mod tidy
	go mod verify

fmt:
	go fmt ./...

clean:
	rm -f dag_pipeline

