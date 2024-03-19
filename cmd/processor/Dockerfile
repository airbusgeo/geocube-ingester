# Light docker to process images (with python scripts only)
# Change BASE_IMAGE=mundialis/esa-snap:ubuntu to add SNAP
ARG BASE_IMAGE=ubuntu:focal
# If SNAP is used as a base image, UPDATE_SNAP=1 will update the modules
ARG UPDATE_SNAP

FROM golang:1.21-bullseye AS builder
ENV GOFLAGS=-mod=vendor
WORKDIR /build
COPY go.mod .
COPY vendor vendor
COPY graph graph
COPY service service
COPY processor processor
COPY common common
COPY cmd/processor cmd
RUN cd cmd && go install .

FROM ${BASE_IMAGE}
ARG UPDATE_SNAP

RUN mkdir -p /usr/local/snap/etc/ && touch /usr/local/snap/etc/snap.properties && touch /usr/local/snap/etc/snap.conf &&\
    echo -e "\nsnap.versionCheck.interval=NEVER" >> /usr/local/snap/etc/snap.properties &&\
    sed -i 's/default_options="\(.*\)"/default_options="\1 -J-Dplugin.manager.check.interval=NEVER"/' /usr/local/snap/etc/snap.conf
RUN if test -n "${UPDATE_SNAP}"; then \
        bin/bash -c '/usr/local/snap/bin/snap --nosplash --nogui --modules --list --refresh --update-all 2>&1 | while read -r line; do &&\
        echo "$line"; [ "$line" == "updates=0" ] && pkill -f "snap/jre/bin/java"; done'; exit 0; \
    fi

RUN apt update && \
    apt install -y  --no-install-recommends \
        python3-pip \
        python3-gdal \
        libgfortran5 &&\
    pip3 install --upgrade pip &&\
    pip3 install rasterio scipy shapely &&\
    apt auto-remove -y && apt clean -y && rm -rf /var/lib/apt/lists/*

COPY ./graph/snap/*.xml /graph/snap/
COPY ./graph/python/*.py /graph/python/
COPY ./graph/library/*.json /graph/library/
COPY --from=builder /go/bin/cmd /processor

ENTRYPOINT ["/processor"]

