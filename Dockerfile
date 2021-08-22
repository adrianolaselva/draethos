FROM golang:1.14.4-stretch as builder

ARG VERSION
ENV VERSION=$VERSION

WORKDIR /app

COPY . .

RUN GOOS=linux GOARCH=amd64 go build -a -ldflags="-s -w" -o draethos ./init

FROM debian:sid-slim

WORKDIR /app

COPY --from=builder ./app/draethos .
