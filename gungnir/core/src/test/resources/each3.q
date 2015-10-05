CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

FROM tuple1 USING kafka_spout()
EACH date_format(cast(bbb AS TIMESTAMP), 'yyyy-MM-dd-HH-mm') AS t1, cast(date_format(cast(ddd AS TIMESTAMP('yyyyMMdd')), 'dd') AS INT) AS t2
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> EACH_2
 EACH_2([date_format(cast(bbb, TIMESTAMP) AS bbb, yyyy-MM-dd-HH-mm) AS t1, cast(date_format(cast(ddd, TIMESTAMP(yyyyMMdd)) AS ddd, dd) AS ddd, INT) AS t2]) parallelism=1
  -S-> EMIT_3
 EMIT_3(mongo_persist(db1, collection1), [*]) parallelism=1
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2
 EACH_2
  incoming: PARTITION_1
  outgoing: EMIT_3
 EMIT_3
  incoming: EACH_2
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd]}
 EACH_2 {tuple1=[t1, t2]}
 EMIT_3 {tuple1=[t1, t2]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -SingleDispatcher-> EACH_2
  EACH_2 -SingleDispatcher-> EMIT_3
 } parallelism=1
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY each3;

@POST('tuple1', '{aaa:"aaa1", bbb:1421307490, ccc:3, ddd:"20150116"}');
@EMIT('EMIT_3', '{t1:"2015-01-15-16-38", t2:16}');
@PLAY(60);

STOP TOPOLOGY each3;
