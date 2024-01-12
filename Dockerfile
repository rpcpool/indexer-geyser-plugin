FROM rust:1.69-bullseye as builder
RUN apt update
RUN apt install -y build-essential
WORKDIR /geyser
COPY . .
RUN cargo build --release

FROM 878626956398.dkr.ecr.eu-north-1.amazonaws.com/step-solana:step-release
COPY --from=builder /geyser/target/release/libholaplex_indexer_rabbitmq_geyser.so /geyser/libholaplex_indexer_rabbitmq_geyser.so
WORKDIR /geyser

ENTRYPOINT [ "solana-test-validator" ]
CMD ["--geyser-plugin-config", "/geyser/compose_config.json"]
