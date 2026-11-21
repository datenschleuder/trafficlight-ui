# syntax=docker/dockerfile:1
# Multi-Arch-Image (linux/amd64, linux/arm64). Go kompiliert auf der Build-Plattform
# für die Zielarchitektur (kein QEMU nötig):
#   docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/traefikgui:<version> --push .
ARG GO_VERSION=1.27.4

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/traefikgui ./cmd/traefikgui

# Länder-Datenbank (DB-IP Lite, CC BY 4.0, https://db-ip.com) – wird ins Image gelegt,
# damit der Pod zur Laufzeit keinen Internetzugriff braucht. Für aktuelle Daten das
# Image monatlich neu bauen. GEOIP=off baut ohne Länderzuordnung.
FROM --platform=$BUILDPLATFORM alpine:3.24 AS geoip
ARG GEOIP=on
RUN set -eu; mkdir -p /geoip; \
    if [ "$GEOIP" = "off" ]; then exit 0; fi; \
    for back in 0 20 40; do \
      month=$(date -u -d "@$(( $(date -u +%s) - back * 86400 ))" +%Y-%m); \
      url="https://download.db-ip.com/free/dbip-country-lite-${month}.csv.gz"; \
      if wget -q -U "traefikgui-build" -O /tmp/geo.csv.gz "$url" && gzip -t /tmp/geo.csv.gz; then \
        mv /tmp/geo.csv.gz /geoip/dbip-country-lite.csv.gz; echo "GeoIP: ${month}"; exit 0; \
      fi; \
    done; \
    echo "GeoIP-Download fehlgeschlagen (mit --build-arg GEOIP=off ohne Länderzuordnung bauen)" >&2; exit 1

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/traefikgui /traefikgui
COPY --from=geoip /geoip /geoip
ENV GEOIP_DB=/geoip/dbip-country-lite.csv.gz \
    LISTEN_ADDR=:8080
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/traefikgui"]
