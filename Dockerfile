ARG gover
FROM registry.inventec/proxy/library/golang:${gover} as builder

ENV GO111MODULE=on
ENV GOPROXY=http://nexus.itc.inventec.net/repository/go-proxy/,https://goproxy.cn,https://goproxy.io,direct

WORKDIR /go/src/lineage

# download all dependencies in image layer cache
COPY go.mod go.sum ./
RUN go mod download

# copy all source files/dirs
COPY . .

# build binary
RUN make build

# Package
FROM registry.inventec/proxy/library/alpine:latest

WORKDIR /opt/lineage

COPY --from=builder /go/src/lineage/dist/* .
