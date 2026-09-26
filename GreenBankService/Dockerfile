FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/green-bank-service .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/green-bank-service /green-bank-service
EXPOSE 8080
ENTRYPOINT ["/green-bank-service"]

