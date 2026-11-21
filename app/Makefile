VERSION   ?= $(shell cat VERSION)
IMAGE     ?= registry.digitalocean.com/datenschleuder/traefikgui
PLATFORMS ?= linux/amd64,linux/arm64
BUILDER   ?= traefikgui

.PHONY: test test-frontend builder image push dev

test:
	go vet ./...
	go test -race ./...

test-frontend:
	node --test internal/web/i18n.test.js

# Multi-Arch-Builds brauchen einen docker-container-Builder (der Standard-Treiber kann das nicht).
builder:
	@docker buildx inspect $(BUILDER) >/dev/null 2>&1 || docker buildx create --name $(BUILDER) --driver docker-container

# Baut beide Architekturen in ein OCI-Archiv (ohne Push).
image: builder
	docker buildx build --builder $(BUILDER) --platform $(PLATFORMS) --build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) --output type=oci,dest=traefikgui-$(VERSION).oci.tar .

# Baut und veröffentlicht das Multi-Arch-Image (vorher: doctl registry login).
push: builder
	docker buildx build --builder $(BUILDER) --platform $(PLATFORMS) --build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) --push .

# Lokale Entwicklung ohne Cluster: Redis + simulierte Traefik-Pods.
dev:
	docker run -d --rm --name traefikgui-dev-redis -p 127.0.0.1:6379:6379 redis:8.10-alpine || true
	go run ./cmd/fakekube -addr 127.0.0.1:8001 & \
	KUBE_API_URL=http://127.0.0.1:8001 LISTEN_ADDR=127.0.0.1:8787 LOG_FORMAT=text go run ./cmd/traefikgui
