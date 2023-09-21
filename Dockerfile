####################################################################################################
# base
####################################################################################################
FROM alpine:3.12.3 as base
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/redisstreams-source /bin/redisstreams-source
RUN chmod +x /bin/redisstreams-source

####################################################################################################
# redisstreams-source
####################################################################################################
FROM scratch as redisstreams-source
ARG ARCH
COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/redisstreams-source /bin/redisstreams-source
ENTRYPOINT [ "/bin/redisstreams-source" ]
