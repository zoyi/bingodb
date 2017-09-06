FROM golang:1.9

RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -d -v github.com/zoyi/bingodb
RUN cd /go/src/github.com/zoyi/bingodb && dep ensure
RUN go install -v github.com/zoyi/bingodb/cmd/bingodb

CMD bingodb
