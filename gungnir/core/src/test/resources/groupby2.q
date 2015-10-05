CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

SET default.parallelism = 3;

FROM tuple1 USING kafka_spout()
BEGIN GROUP BY aaa
EACH aaa, sum(bbb) AS s, count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection1', 'aaa');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=3
  -S-> PARTITION_1
 PARTITION_1(fields grouping(aaa))
  -GS[aaa]-> EACH_2
 EACH_2([aaa, sum(bbb) AS s, count() AS c]) parallelism=1
  -GS[aaa]-> EMIT_3
 EMIT_3(mongo_persist(db1, collection1, [aaa]), [*]) parallelism=3
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
 EACH_2 {tuple1=[aaa, s, c]}
 EMIT_3 {tuple1=[aaa, s, c]}
Group fields:
 EACH_2 aaa
 EMIT_3 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=3
 EXEC_BOLT_1 {
  PARTITION_1 -GroupingDispatcher(aaa)-> EACH_2
  EACH_2 -GroupingDispatcher(aaa)-> EMIT_3
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');
