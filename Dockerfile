FROM golang:1.13-alpine as build
ENV GO111MODULE=on

WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/dag_pipeline/dag_pipeline.go

FROM scratch
WORKDIR /app
COPY --from=build /app/dag_pipeline /app/dag_pipeline
CMD ["/app/dag_pipeline"]
