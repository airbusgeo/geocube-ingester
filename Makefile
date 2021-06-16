PROJECT := d-gcb-geocuberd
CLUSTER := gke_d-gcb-geocuberd_europe-west1-d_geocube
TAG := 20210617


WF_IMAGE := eu.gcr.io/$(PROJECT)/ingester/workflow:$(TAG)
DOWNLOADER_IMAGE := eu.gcr.io/$(PROJECT)/ingester/downloader:$(TAG)
PROCESSOR_IMAGE := eu.gcr.io/$(PROJECT)/ingester/processor:$(TAG)
NAMESPACE=ingester

build-workflow:
	docker build -t $(WF_IMAGE) -f cmd/workflow/Dockerfile .

build-processor:
	docker build -t $(PROCESSOR_IMAGE) -f cmd/processor/Dockerfile .

build-downloader:
	docker build -t $(DOWNLOADER_IMAGE) -f cmd/downloader/Dockerfile .

build: build-workflow build-processor build-downloader
push: push-workflow push-processor push-downloader

push-workflow:
	docker push $(WF_IMAGE)
push-processor:
	docker push $(PROCESSOR_IMAGE)
push-downloader:
	docker push $(DOWNLOADER_IMAGE)

deploy:
	kubectl --context=$(CLUSTER) apply -f deploy/k8s/namespace.yaml
	cat deploy/k8s/interne/workflow.yaml | sed  -e 's#@@DOWNLOADER_IMAGE@@#$(DOWNLOADER_IMAGE)#' -e 's#@@PROCESSOR_IMAGE@@#$(PROCESSOR_IMAGE)#' -e 's#@@WF_IMAGE@@#$(WF_IMAGE)#' | kubectl --context=$(CLUSTER) apply -n $(NAMESPACE) -f -
	kubectl --context=$(CLUSTER) apply -n $(NAMESPACE) -f secrets


connect:
	kubectl --context=$(CLUSTER) run -n $(NAMESPACE) psql --rm -it --image postgres -- psql $$(kubectl --context=$(CLUSTER) -n $(NAMESPACE) get secrets ingester-db -o 'go-template={{index .data "connection_string"}}'|base64 -d)

connect-postgres:
	kubectl --context=$(CLUSTER) run -n $(NAMESPACE) psql-pg --rm -it --image postgres -- psql $$(kubectl --context=$(CLUSTER) get secrets db -o 'go-template={{index .data "db_root_connection_string"}}'|base64 -d)

port-forward:
	kubectl --context=$(CLUSTER) port-forward -n $(NAMESPACE) svc/workflow-service 8080:8080
