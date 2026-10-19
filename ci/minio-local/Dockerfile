# MinIO server + mc client built from the (archived) source repositories.
#
# Why: MinIO deleted minio/minio and minio/mc from Docker Hub in September 2026 and closed
# quay.io to anonymous pulls, so `image: minio/minio` fails on a fresh machine such as a CI
# runner. The base images come from Google's mirror so this build does not depend on Docker Hub.
# Nobody publishes new releases any more: pin the tags below and review them occasionally.
FROM mirror.gcr.io/library/golang:1.24-alpine AS build
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_TIME=2025-10-15T17:29:55Z
ARG MC_TAG=RELEASE.2025-08-13T08-35-41Z
ARG MC_TIME=2025-08-13T08:35:41Z
ENV CGO_ENABLED=0 MINIO_RELEASE=RELEASE MC_RELEASE=RELEASE
RUN apk add --no-cache git
RUN git clone -q --depth 1 -b "$MINIO_TAG" https://github.com/minio/minio /minio
RUN git clone -q --depth 1 -b "$MC_TAG" https://github.com/minio/mc /mc
WORKDIR /minio
RUN go build -trimpath -tags kqueue -o /out/minio \
      -ldflags "$(go run buildscripts/gen-ldflags.go "$MINIO_TIME")"
WORKDIR /mc
RUN go build -trimpath -tags kqueue -o /out/mc \
      -ldflags "$(go run buildscripts/gen-ldflags.go "$MC_TIME")"

FROM mirror.gcr.io/library/alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /out/minio /out/mc /usr/bin/
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]