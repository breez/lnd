#!/bin/bash
#

### go 1.19.8

COMMIT=$(git describe --abbrev=40 --dirty)
COMMIT_HASH=$(git rev-parse HEAD)
PKG=github.com/lightningnetwork/lnd
export CGO_ENABLED=0
export GOOS=linux
export GOARCH=amd64
go build -trimpath -ldflags "-X ${PKG}/build.Commit=${COMMIT} -X ${PKG}/build.CommitHash=${COMMIT_HASH} -s -w" -tags=submarineswaprpc,chanreservedynamic,routerrpc,walletrpc,chainrpc,signrpc,invoicesrpc -o lnd ./cmd/lnd
go build -trimpath -ldflags "-X ${PKG}/build.Commit=${COMMIT} -X ${PKG}/build.CommitHash=${COMMIT_HASH} -s -w" -tags=submarineswaprpc,chanreservedynamic,routerrpc,walletrpc,chainrpc,signrpc,invoicesrpc -o lncli ./cmd/lncli
