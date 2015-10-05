CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

FROM tuple1 USING kafka_spout() INTO s1;

FROM s1
LIMIT FIRST EVERY 30sec
EMIT aaa, bbb USING web_emit('http://localhost:3000/update');

FROM s1
LIMIT LAST EVERY 30sec
EMIT ccc, ddd USING web_emit('http://localhost:3000/update');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> LIMIT_2
  -S-> LIMIT_3
 LIMIT_2(FIRST, interval(30SECONDS)) parallelism=1
  -S-> EMIT_4
 LIMIT_3(LAST, interval(30SECONDS)) parallelism=1
  -S-> EMIT_5
 EMIT_4(web_emit(http://localhost:3000/update), [aaa, bbb]) parallelism=1
 EMIT_5(web_emit(http://localhost:3000/update), [ccc, ddd]) parallelism=1
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: LIMIT_2, LIMIT_3
 LIMIT_2
  incoming: PARTITION_1
  outgoing: EMIT_4
 LIMIT_3
  incoming: PARTITION_1
  outgoing: EMIT_5
 EMIT_4
  incoming: LIMIT_2
  outgoing: -
 EMIT_5
  incoming: LIMIT_3
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd]}
 LIMIT_2 {tuple1=[aaa, bbb, ccc, ddd]}
 LIMIT_3 {tuple1=[aaa, bbb, ccc, ddd]}
 EMIT_4 {tuple1=[aaa, bbb, ccc, ddd]}
 EMIT_5 {tuple1=[aaa, bbb, ccc, ddd]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -MultiDispatcher[-SingleDispatcher-> LIMIT_2, -SingleDispatcher-> LIMIT_3]
  LIMIT_2 -SingleDispatcher-> EMIT_4
  LIMIT_3 -SingleDispatcher-> EMIT_5
 } parallelism=1
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY limit;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@EMIT('EMIT_4', '{aaa:"aaa1", bbb:10}');
@PLAY(60);

@TIMER(+40);

@POST('tuple1', '{aaa:"aaa2", bbb:20, ccc:200, ddd:"ddd2"}');
@EMIT('EMIT_4', '{aaa:"aaa2", bbb:20}');
@EMIT('EMIT_5', '{ccc:100, ddd:"ddd1"}');
@PLAY(60);

STOP TOPOLOGY limit;
