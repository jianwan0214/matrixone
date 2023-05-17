FROM golang:1.19-buster as builder

# goproxy
ARG GOPROXY="https://proxy.golang.org,direct"
RUN go env -w GOPROXY=${GOPROXY}

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

RUN make build

FROM ubuntu:20.04

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/etc /etc
# Install some utilities used for debugging or by startup script
RUN echo "deb http://mirrors.ustc.edu.cn/ubuntu-ports/ jammy main restricted universe multiverse\ndeb http://mirrors.ustc.edu.cn/ubuntu-ports/ jammy-updates main restricted universe multiverse\ndeb http://mirrors.ustc.edu.cn/ubuntu-ports/ jammy-backports main restricted universe multiverse\ndeb http://mirrors.ustc.edu.cn/ubuntu-ports/ jammy-security main restricted universe multiverse" > /etc/apt/sources.list
RUN apt-get update && apt-get install -y \
    dnsutils \
    curl \
&& apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
