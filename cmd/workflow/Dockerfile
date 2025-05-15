FROM golang:1.24-bookworm AS builder

RUN apt update && apt install -y --no-install-recommends libgeos++-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt autoremove -y \
    && apt clean -y

ENV GOFLAGS=-mod=vendor
WORKDIR /build
COPY go.mod .
COPY vendor vendor
COPY common common
COPY service service
COPY catalog catalog
COPY interface interface
COPY workflow workflow
COPY cmd/workflow cmd
RUN cd cmd && go install .

FROM gcr.io/distroless/base-debian12

COPY --from=builder /usr/lib/x86_64-linux-gnu/libgeos* \
                    /usr/lib/x86_64-linux-gnu/libstdc++.so.6 \
                    /lib/x86_64-linux-gnu/libgcc_s.so.1 \
                    /usr/lib/x86_64-linux-gnu/
COPY --from=builder /lib/x86_64-linux-gnu/libm.so.6 \
                    /lib/x86_64-linux-gnu/

COPY --from=builder /go/bin/cmd /workflow
ENTRYPOINT ["/workflow"]
