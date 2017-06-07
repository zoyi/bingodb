# bingodb

### 설계 방향
* In-memory 디비로 persistence 지원되지 않음
* 하나의 빙고디비는 여러개의 테이블로 구성된다
* 하나의 테이블은 논리적인 데이터를 저장하는 단위로 key, value 페어 데이터를 저장한다
* value는 편의성을 위해 JSON 형태를 지원한다
* 테이블의 설정 정보는 우선 config 파일로 처리하고 추후 동적으로 업데이트 할 수 있게 개선할 것이다
* value에 만료 시간을 정할 수 있고, 만료 시간이 지난 데이터는 db에 의해 자동 만료 처리(삭제) 된다
* 여러 key에 대해 만료 시간은 같을 수 있다 
* 만료된 데이터는 config 속성을 통해 AMQP 등에게 정보가 전달 된다
* 하나의 테이블에 대해 복수개의 세컨더리 인덱스를 지원한다
* 추후 분산 컴퓨팅이 지원될 것이다
* 추후 Fast concurrent lock-free binary search tree 개념이 도입될 것이다


### 테이블 API (사이즈가 n 일 때)
* key, value에 대한 put: O(lg(n))
* key에 대한 lookup: O(lg(n))
* key에 대한 delete: O(lg(n))
* startkey, stopkey, limit(m) 를 주고 fetch: O(lg(n)*m)
* startkey, stopkey 를 주고 count: O(lg(n))
* startkey, stopkey, limit(m), index 정보를 주고 인덱스로 정렬된 fetch: O(lg(n)*m) 
