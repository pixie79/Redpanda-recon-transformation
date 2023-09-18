FROM golang:1.21 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /replay

FROM gcr.io/distroless/base-debian12:debug AS release

WORKDIR /

COPY --from=build-stage /replay /bin/replay
USER nonroot:nonroot

ENTRYPOINT ["tail", "-f", "/dev/null"]