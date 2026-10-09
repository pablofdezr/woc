# woc: wake-on-call proxy. A static binary on distroless (no shell, no curl), about 10 MB.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /woc .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /woc /woc
EXPOSE 8080 8081
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --start-interval=1s CMD ["/woc", "healthcheck"]
ENTRYPOINT ["/woc"]
