# Light docker to download images
# Change BASE_IMAGE=s1tbx:8.0 (for example) to add SNAP S1toolbox
ARG BASE_IMAGE=ubuntu:focal
# If SNAP is used as a base image, UPDATE_SNAP=1 will update the modules
ARG UPDATE_SNAP


FROM golang:1.19 AS builder

ENV GOFLAGS=-mod=vendor
WORKDIR /build
COPY go.mod .
COPY vendor vendor
COPY common common
COPY graph graph
COPY service service
COPY interface/provider interface/provider
COPY interface/shared interface/shared
COPY downloader downloader
COPY cmd/downloader cmd
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

COPY ./graph/snap/*.xml /graph/snap/
COPY ./graph/library/*.json /graph/library/
COPY --from=builder /go/bin/cmd /downloader

ENTRYPOINT ["/downloader"]
