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
go run cmd/bingodb/bingodb.go -config /path/to/config.yml -addr address:port`
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

## API Overview

### <code>GET</code> /tables
* 존재하는 모든 테이블의 정보를 주는 API

### <code>GET</code> /tables/:table?hash=[hash]&sort=[sort]
* 해당 table 에서 hashKey가 hash, sortKey가 sort 인 item을 찾는 API

### <code>DELETE</code> /tables/:table?hash=[hash]&sort=[sort]
* 해당 table 에서 hashKey가 hash, sortKey가 sort 인 item을 지우는 API

### <code>PUT</code> /tables/:table/
* 새로운 document를 추가하는 API
* $setOnInsert는 해당 document가 디비에 없어 새로 추가되는 경우에만 값을 set하게 됨
* Response에는 디비에 이전 값이 있다면 이전 값과 새로 추가된 값, 교체된 여부를 알 수 있음
* Request example 
```json
{
  "$set": {
    "id": "soc1",
    "personKey": "user1",
    "updatedAt": 1505200000000,
    "expiresAt": 1513008702000
  },
  "$setOnInsert": {
    "createdAt": 1505200000000
  }
}
```

### <code>GET</code> /tables/:table/info
* 해당 table 에 대한 정보를 주는 API

### <code>GET</code> /tables/:table/scan?hash=[hash]&since=[since]&limit=[limit]&backward=[backward]
* 해쉬 값에 해당하는 아이템들을 list로 얻는 API
* since 값을 포함해 그 이후 데이터를 조회함(backward 값이 y일 경우 그 이전)
* 최대 limit 개수 만큼 조회 

### <code>GET</code> /tables/:table/indices/:index?hash=[hash]&sort=[sort]
* index 이름을 가진 서브 인덱스에 대해 hashKey가 hash, sortKey가 sort 인 아이템을 찾는 API

### <code>GET</code> /tables/:table/indices/:index/scan?hash=[hash]&limit=[limit]&since=[since1]&since=[since2]&since=[since3]&backward=[backward]
* index 이름을 가진 서브 인덱스에 대해 해당하는 아이템들을 list로 얻는 API
* since 값을 포함해 그 이후 데이터를 조회함(backward 값이 y일 경우 그 이전)
* since1: subIndex sort key, since2: primary hash key, since3: primary sort key
* 최대 limit 개수 만큼 조회


## Performance
* put: O(lg(n))
* lookup: O(lg(n))
* delete: O(lg(n))
* fetch with since, limit(m): O(lg(n)*m)

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
