# woc: wake-on-call proxy. A static binary on distroless (no shell, no curl), about 10 MB.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /woc . && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /woc /woc
# /data for WOC_STATE_FILE, owned by the image's user so a fresh volume mounted there is writable.
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8080 8081
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --start-interval=1s CMD ["/woc", "healthcheck"]
ENTRYPOINT ["/woc"]
