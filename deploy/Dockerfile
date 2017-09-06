FROM golang:1.9

RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -u -d -v github.com/zoyi/bingodb/cmd/bingodb

CMD bingodb
