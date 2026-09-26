FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/memory-zhorka .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/memory-zhorka /memory-zhorka
EXPOSE 8080
ENTRYPOINT ["/memory-zhorka"]

