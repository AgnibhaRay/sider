# Stage 1: Build the Go Binary
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy the source code
COPY main.go .

# Initialize a temporary module and build
# CGO_ENABLED=0 ensures a static binary that runs anywhere
RUN go mod init sider && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o sider main.go

# Stage 2: Create the Final Lightweight Image
FROM alpine:latest

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/sider .

# Create the data directory for persistence
RUN mkdir data

# Expose the custom TCP port defined in your Go code
EXPOSE 4000

# Run the database
CMD ["./sider"]