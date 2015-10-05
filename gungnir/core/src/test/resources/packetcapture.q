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

SET default.parallelism = 3;
SET topology.metrics.enabled = true;
SET topology.metrics.interval.secs = 3;

FROM RequestPacket, ResponsePacket USING kafka_spout() parallelism 8
INTO s1;

FROM s1(RequestPacket)
EACH
  request_pheader.Destination_Port AS dest_port,
  request_pheader.Source_Port AS src_port,
  request_pheader.Destination_Ip AS dest_ip,
  request_pheader.Source_Ip AS src_ip,
  request_properties.Host AS host,
  _time AS time
EMIT * USING mongo_persist('front', 'request');

FROM s1(ResponsePacket)
EACH
  response_pheader.Source_Port AS src_port,
  response_pheader.Destination_Port AS dest_port,
  response_pheader.Source_Ip AS src_ip,
  response_pheader.Destination_Ip AS dest_ip,
  response_properties.Status AS status,
  _time AS time
EMIT * USING mongo_persist('front', 'response');

EXPLAIN EXTENDED;

SUBMIT TOPOLOGY pk;

@POST('RequestPacket', '{request_pheader:{ID:20,Source_Ip:"192.168.11.1",Destination_Ip:"192.168.11.2",Source_Port:80,Destination_Port:1000,Packet_Length:123},request_properties:{Host:"gennai.org",Request_URI:"/",Content_Length:100,Content_Type:"text/html; charset=UTF-8",Method:"GET",Authorization:"OAuth realm=gennai.org",Accept:"text/html,application/xhtml+xml,application/xml",Connection:"close",Referer:"http://gennai.org/doc",User_Agent:"Mozilla/5.0 AppleWebKit (KHTML, like Gecko) Chrome/30"}}');
@EMIT('EMIT_4', '{dest_port:"1000", src_port:"80", dest_ip:"192.168.11.2", src_ip:"192.168.11.1", host:"gennai.org", time:"_time"}');
@PLAY(60);

STOP TOPOLOGY pk;
