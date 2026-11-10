# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG SEMVER
ARG BUILD_TIMESTAMP
ARG BUILD_COMMIT

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.sum ./
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the Go source code
COPY apis/ apis/
COPY cmd/ cmd/
COPY internal/ internal/
COPY third_party/licenses/ third_party/licenses/

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -a -ldflags "-X main.semver=${SEMVER} -X main.buildTimestamp=${BUILD_TIMESTAMP} -X main.buildCommit=${BUILD_COMMIT}" -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/third_party/licenses/ third_party_licenses/
USER 65532:65532

ENTRYPOINT ["/manager"]
