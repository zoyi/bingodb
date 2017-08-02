# BingoDB
[![CircleCI](https://circleci.com/gh/zoyi/bingodb/tree/master.svg?style=shield)](https://circleci.com/gh/zoyi/bingodb/tree/master) [![codecov](https://codecov.io/gh/zoyi/bingodb/branch/master/graph/badge.svg?token=n78FlqQZC7)](https://codecov.io/gh/zoyi/bingodb)

**BingoDB** is light-weight and linearly scalable pre-defined (for now) in-memory database. BingoDB provides callback function when TTL expires.

## Motivation
DynamoDB is fast but it does not provide callback feature, and other SQL-based services are not fast enough (since accessing disk is slow relative to accessing memory). We specifically wanted to have two features: a database notifies after a event expires in certain period of time, and be able to search with certain index name with high performance. However, we could not find a solid service that satisfied our needs. So we decided to create our own database which provides two key features.

## Installation
Install go 1.9 from official website. We only supports 1.9 and higher version of go. Get package manager [dep](https://github.com/golang/dep) by using command

```sh
 go get github.com/golang/dep
```
and run the following from the project root directory to install dependencies:
```sh
dep ensure
```

## Usage
To use BingoDB, you first need to create a `{filename}.yml` and define of your tables (see example.yml below). Once you've done this, you can simply run a server with following command:
```sh
go run main/main.go -config /path/to/config.yml -addr address:port`
```

```
#example.yml
tables:
  #your table name
  onlines:
    fields:
	  #define your fields
	  #we support string/integer only for now
      id: 'string'
      guestKey: 'string'
      channelId: 'string'
      expiresAt: 'integer'
      lastSeen: 'integer'
    expireKey: 'expiresAt'
    hashKey: 'channelId'
    sortKey: 'id'
    subIndices:
      #name of index you want to search for a particular case
      guest:
        #these fields' value suppose to be in fields
        hashKey: 'channelId'
        sortKey: 'lastSeen'
```

## Performance
* put: O(lg(n))
* lookup: O(lg(n))
* delete: O(lg(n))
* fetch with startkey, stopkey, limit(m): O(lg(n)*m)
* count with startkey, stopkey: O(lg(n))
* sorted fetch by index with startkey, stopkey, limit(m), index: O(lg(n)*m)

## Milestones
* Support distributed computing
* Support Fast concurrent lock-free binary search tree
* In-memory database and do not support persistency (**completed**)
* One bingoDB contains multiple tables (**completed**)
* A table is structure to store data which contains key/value pair  (**completed**)
* Support JSON format for value type (**completed**)
* Support dynamic table configuration like NoSQL
* Support pre-define table configuration with config file (**completed**)
* Provide time to live feature on a value, and the value suppose to be deleted after expired
* Support duplicated TTL value for keys (**completed**)
* Expired data transits to Message Queue (AMQP, RabbitMQ, etc)
* One table can have multiple secondary indexes (**completed**)

## Benchmarks
* TBD