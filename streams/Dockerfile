FROM alpine:3.20
RUN apk add --no-cache ca-certificates netcat-openbsd
COPY --from=ghcr.io/warpstreamlabs/bento:latest /bento /usr/local/bin/bento
COPY wait-and-run.sh /usr/local/bin/wait-and-run.sh
RUN chmod +x /usr/local/bin/wait-and-run.sh
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/wait-and-run.sh"]
