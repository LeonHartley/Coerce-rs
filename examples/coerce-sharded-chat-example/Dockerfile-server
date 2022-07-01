FROM rust:1.61-bullseye as build

WORKDIR /app

COPY . ./

RUN cargo build --bin sharded-chat-server --release


FROM debian:bullseye-slim

#Coerce Remoting
EXPOSE 31101

#WebSocket Server
EXPOSE 31102

#Coerce HTTP API
EXPOSE 31103

#Metrics Exporter
EXPOSE 31104

ENV NODE_ID=1
ENV REMOTE_LISTEN_ADDR="0.0.0.0:31101"
ENV WEBSOCKET_LISTEN_ADDR="0.0.0.0:31102"
ENV CLUSTER_API_LISTEN_ADDR="0.0.0.0:31103"
ENV METRICS_EXPORTER_LISTEN_ADDR="0.0.0.0:31104"
ENV LOG_LEVEL="INFO"

WORKDIR /app
COPY --from=build /app/target/release/sharded-chat-server /app/sharded-chat-server

ENTRYPOINT ["./sharded-chat-server"]