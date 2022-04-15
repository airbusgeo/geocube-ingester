ARG BASE_IMAGE

FROM ${BASE_IMAGE}

RUN apt update && \
    apt install -y  --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        openssl \
        apt-transport-https \
        &&\
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg &&\
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt update && \
    apt install -y  --no-install-recommends \
        docker-ce \
        docker-ce-cli \
        containerd.io \
        docker-compose-plugin && \
    apt auto-remove -y && apt clean -y && rm -rf /var/lib/apt/lists/*