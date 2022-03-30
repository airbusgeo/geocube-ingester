FROM golang:latest AS builder

ENV GOFLAGS=-mod=vendor
WORKDIR /build
COPY go.mod .
COPY vendor vendor
COPY common common
COPY graph graph
COPY service service
COPY interface/provider interface/provider
COPY downloader downloader
COPY cmd/downloader cmd
RUN cd cmd && go install .

FROM mundialis/esa-snap:ubuntu
RUN echo -e "\nsnap.versionCheck.interval=NEVER" >> /usr/local/snap/etc/snap.properties \
    && sed -i 's/default_options="\(.*\)"/default_options="\1 -J-Dplugin.manager.check.interval=NEVER"/' /usr/local/snap/etc/snap.conf \
    && bin/bash -c '/usr/local/snap/bin/snap --nosplash --nogui --modules --list --refresh --update-all 2>&1 | while read -r line; do \
    echo "$line"; [ "$line" == "updates=0" ] && pkill -f "snap/jre/bin/java"; done'; exit 0

COPY ./graph/snap/*.xml /data/graph/snap/
COPY ./graph/library/*.json /data/graph/library/
COPY --from=builder /go/bin/cmd /downloader

ENTRYPOINT ["/downloader"]
