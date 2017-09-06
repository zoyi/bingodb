FROM golang:1.9

RUN go get -u -v github.com/zoyi/bingodb/cmd/bingodb

CMD bingodb -addr :80 -config /var/lib/bingodb/config.yml

EXPOSE 80

VOLUME /var/lib/bingodb
