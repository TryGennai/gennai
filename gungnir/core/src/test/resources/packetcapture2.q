CREATE TUPLE RequestPacket(
  request_properties STRUCT<
    Content_Length STRING,
    Content_Type STRING,
    Method STRING,
    Request_URI STRING,
    Authorization STRING,
    Host STRING,
    Accept STRING,
    Connection STRING,
    Referer STRING,
    User_Agent STRING
  >,
  request_pheader STRUCT<
    Packet_Length INT,
    Destination_Port STRING,
    Source_Port STRING,
    Destination_Ip STRING,
    Source_Ip STRING,
    ID STRING
  >,
  _time
);

CREATE TUPLE ResponsePacket (
  response_properties STRUCT<
    Content_Type STRING,
    Connection STRING,
    Status STRING,
    Date STRING,
    Server STRING,
    Last_Modified STRING,
    ETag STRING,
    Accept_Ranges STRING,
    Content_Length STRING,
    Keep_Alive STRING
  >,
  response_pheader STRUCT<
    Packet_Length INT,
    Destination_Port STRING,
    Source_Port STRING,
    Destination_Ip STRING,
    Source_Ip STRING,
    ID STRING
  >,
  _time
);

SET default.parallelism = 32;

FROM (
  RequestPacket JOIN ResponsePacket
  ON RequestPacket.request_pheader.Destination_Port = ResponsePacket.response_pheader.Source_Port
  AND RequestPacket.request_pheader.Source_Port = ResponsePacket.response_pheader.Destination_Port
  AND RequestPacket.request_pheader.Destination_Ip = ResponsePacket.response_pheader.Source_Ip
  AND RequestPacket.request_pheader.Source_Ip = ResponsePacket.response_pheader.Destination_Ip
  TO
    RequestPacket.request_properties AS request_properties,
    RequestPacket._time AS request_time,
    ResponsePacket.response_properties AS response_properties,
    ResponsePacket._time AS response_time
  EXPIRE 10sec
) AS packet USING kafka_spout() parallelism 8
EACH
  request_properties.Host AS host,
  request_properties.Request_URI AS uri,
  response_properties.Status AS status,
  request_time,
  response_time
INTO s1;

FROM s1
SNAPSHOT EVERY 1min *, count() AS cnt
EACH *, sum(cnt) AS sum, ifnull(record, 'cnt_all') AS record parallelism 1
EMIT record, sum, request_time, response_time USING mongo_persist('front', 'count', ['record']) parallelism 1;

FROM s1
SNAPSHOT EVERY 1min *, count() AS cnt
SLIDE LENGTH 5min BY response_time sum(cnt) AS sum, request_time, response_time parallelism 1
EACH *, ifnull(record, 'cnt_all_time') AS record parallelism 1
EMIT record, sum, request_time, response_time USING mongo_persist('front', 'count', ['record']) parallelism 1;

FROM s1
JOIN sid USING mongo_fetch('front', 'service', '{host:@host}', ['sid'], 10min)
INTO s2;

FROM s2
FILTER sid IS NULL
JOIN flag USING mongo_fetch('front', 'unkowns', '{host:@host}', ['host'], 10min)
FILTER flag IS NULL
EMIT host USING mongo_persist('front', 'unknowns');

FROM s2
EACH ifnull(sid, 'unknowns') AS sid, host, uri, status, request_time, response_time
BEGIN GROUP BY sid
INTO s3;

FROM s3
SNAPSHOT EVERY 1min *, count() AS cnt
EACH *, sum(cnt) AS sum, ifnull(record, 'cnt_sid') AS record parallelism 1
EMIT record, sum, sid, request_time, response_time USING mongo_persist('front', 'count', ['record', 'sid']) parallelism 1;

FROM s3
SNAPSHOT EVERY 1min *, count() AS cnt
SLIDE LENGTH 5min BY response_time sum(cnt) AS sum, sid, request_time, response_time parallelism 1
EACH *, ifnull(record, 'cnt_sid_time') AS record parallelism 1
EMIT record, sum, sid, request_time, response_time USING mongo_persist('front', 'count', ['record', 'sid']) parallelism 1;

FROM s3
FILTER sid <> 'unknowns'
BEGIN GROUP BY host
INTO s4;

FROM s4
SNAPSHOT EVERY 1min *, count() AS cnt
EACH *, sum(cnt) AS sum, ifnull(record, 'cnt_host') AS record  parallelism 1
EMIT record, sum, sid, host, request_time, response_time USING mongo_persist('front', 'count', ['record', 'sid', 'host']) parallelism 1;

FROM s4
SNAPSHOT EVERY 1min *, count() AS cnt
SLIDE LENGTH 5min BY response_time sum(cnt) AS sum, sid, host, request_time, response_time parallelism 1
EACH *, ifnull(record, 'cnt_host_time') AS record parallelism 1
EMIT record, sum, sid, host, request_time, response_time USING mongo_persist('front', 'count', ['record', 'sid', 'host']) parallelism 1;

FROM s4
EACH *, regexp_extract(status, '(.*) (\d{3}) (.*)', 2) AS code
FILTER code REGEXP '^[^23]\d{2}$'
BEGIN GROUP BY code
INTO s5;

FROM s5
SNAPSHOT EVERY 1min *, count() AS cnt
EACH *, sum(cnt) AS sum, ifnull(record, 'cnt_status') AS record  parallelism 1
EMIT record, sum, sid, host, code, request_time, response_time USING mongo_persist('front', 'count', ['record', 'sid', 'host', 'code']) parallelism 1;

FROM s5
SNAPSHOT EVERY 1min *, count() AS cnt
SLIDE LENGTH 5min BY response_time sum(cnt) AS sum, sid, host, uri, code, request_time, response_time parallelism 1
EACH *, ifnull(record, 'cnt_status_time') AS record parallelism 1
EMIT record, sum, sid, host, code, request_time, response_time USING mongo_persist('front', 'count', ['record', 'sid', 'host', 'code']) parallelism 1;

FROM s5
EACH *, ifnull(record, 'uri_status') AS record
EMIT record, sid, host, uri, status, code, request_time, response_time USING mongo_persist('front', 'output');

EXPLAIN EXTENDED;

SUBMIT TOPOLOGY pk2;

@POST('RequestPacket', '{"request_pheader":{"ID":20,"Source_Ip":"172.20.4.64","Destination_Ip":"160.37.39.43","Source_Port":80,"Destination_Port":1920},"request_properties":{"Host":"gennai.org","Request_URI":"/path/002 HTTP/1.1"}}');
@POST('ResponsePacket', '{"response_pheader":{"ID":21,"Source_Ip":"160.37.39.43","Destination_Ip":"172.20.4.64","Source_Port":1920,"Destination_Port":80},"response_properties":{"Status":"HTTP/1.1 304 Not Modified"}}');
@FETCH('JOIN_9', '["host"]', '{"gennai.org":[["GENNAI"]]}');
@FETCH('JOIN_19', '["host"]', '{"gennai.org":[["gennai.org"]]}');
@PLAY(5);

STOP TOPOLOGY pk2;
