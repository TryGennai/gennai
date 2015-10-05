CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

set default.parallelism = 10;

FROM tuple1 USING kafka_spout()
EACH aaa AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=10
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> EACH_2
 EACH_2([aaa AS id]) parallelism=10
  -S-> PARTITION_3
 PARTITION_3(global grouping)
  -S-> EACH_4
 EACH_4([count() AS c]) parallelism=1
  -S-> PARTITION_5
 PARTITION_5(shuffle grouping)
  -S-> EMIT_6
 EMIT_6(mongo_persist(db1, collection1), [*]) parallelism=10
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2
 EACH_2
  incoming: PARTITION_1
  outgoing: PARTITION_3
 PARTITION_3
  incoming: EACH_2
  outgoing: EACH_4
 EACH_4
  incoming: PARTITION_3
  outgoing: PARTITION_5
 PARTITION_5
  incoming: EACH_4
  outgoing: EMIT_6
 EMIT_6
  incoming: PARTITION_5
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd]}
 EACH_2 {tuple1=[id]}
 PARTITION_3 {tuple1=[id]}
 EACH_4 {tuple1=[c]}
 PARTITION_5 {tuple1=[c]}
 EMIT_6 {tuple1=[c]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=10
 EXEC_BOLT_1 {
  PARTITION_1 -SingleDispatcher-> EACH_2
  EACH_2 -SingleDispatcher-> PARTITION_3
 } parallelism=10
 EXEC_BOLT_2 {
  PARTITION_3 -SingleDispatcher-> EACH_4
  EACH_4 -SingleDispatcher-> PARTITION_5
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_5 -SingleDispatcher-> EMIT_6
 } parallelism=10
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1
  -PARTITION_3-> EXEC_BOLT_2
 EXEC_BOLT_2
  -PARTITION_5-> EXEC_BOLT_1');
