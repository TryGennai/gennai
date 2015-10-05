CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING) PARTITIONED BY aaa;
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);

set default.parallelism = 5;

FROM tuple1, tuple2  USING kafka_spout() INTO s1;

FROM s1(tuple2)
EACH eee AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection1');

FROM s1(tuple1)
BEGIN GROUP BY aaa
EACH aaa AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection2');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING) partitioned by aaa, tuple2(eee STRING, fff INT, ggg INT, hhh STRING)]) parallelism=5
  -S(tuple2)-> PARTITION_1
  -S(tuple1)-> PARTITION_2
 PARTITION_1(shuffle grouping)
  -S(tuple2)-> EACH_3
 PARTITION_2(fields grouping(aaa))
  -GS[aaa](tuple1)-> EACH_4
 EACH_3([eee AS id]) parallelism=5
  -S(tuple2)-> PARTITION_5
 EACH_4([aaa AS id]) parallelism=5
  -GS[aaa](tuple1)-> EACH_6
 PARTITION_5(global grouping)
  -S(tuple2)-> EACH_7
 EACH_6([count() AS c]) parallelism=1
  -GS[aaa](tuple1)-> EMIT_8
 EACH_7([count() AS c]) parallelism=1
  -S(tuple2)-> PARTITION_9
 EMIT_8(mongo_persist(db1, collection2), [*]) parallelism=5
 PARTITION_9(shuffle grouping)
  -S(tuple2)-> EMIT_10
 EMIT_10(mongo_persist(db1, collection1), [*]) parallelism=5
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1, PARTITION_2
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_3
 PARTITION_2
  incoming: SPOUT_0
  outgoing: EACH_4
 EACH_3
  incoming: PARTITION_1
  outgoing: PARTITION_5
 EACH_4
  incoming: PARTITION_2
  outgoing: EACH_6
 PARTITION_5
  incoming: EACH_3
  outgoing: EACH_7
 EACH_6
  incoming: EACH_4
  outgoing: EMIT_8
 EACH_7
  incoming: PARTITION_5
  outgoing: PARTITION_9
 EMIT_8
  incoming: EACH_6
  outgoing: -
 PARTITION_9
  incoming: EACH_7
  outgoing: EMIT_10
 EMIT_10
  incoming: PARTITION_9
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 PARTITION_1 {tuple2=[eee, fff, ggg, hhh]}
 PARTITION_2 {tuple1=[aaa, bbb, ccc, ddd]}
 EACH_3 {tuple2=[id]}
 EACH_4 {tuple1=[id]}
 PARTITION_5 {tuple2=[id]}
 EACH_6 {tuple1=[c]}
 EACH_7 {tuple2=[c]}
 EMIT_8 {tuple1=[c]}
 PARTITION_9 {tuple2=[c]}
 EMIT_10 {tuple2=[c]}
Group fields:
 EACH_4 aaa
 EACH_6 aaa
 EMIT_8 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -MultiDispatcher[TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> PARTITION_1, TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> PARTITION_2]
 } parallelism=5
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_3
  EACH_3 -SingleDispatcher-> PARTITION_5
 } parallelism=5
 EXEC_BOLT_2 {
  PARTITION_2 TupleNameFilter(tupleName=[tuple1])-GroupingDispatcher(aaa)-> EACH_4
  EACH_4 -GroupingDispatcher(aaa)-> EACH_6
  EACH_6 -GroupingDispatcher(aaa)-> EMIT_8
 } parallelism=5
 EXEC_BOLT_3 {
  PARTITION_5 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_7
  EACH_7 -SingleDispatcher-> PARTITION_9
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_9 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EMIT_10
 } parallelism=5
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
  -PARTITION_2-> EXEC_BOLT_2
 EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_3
 EXEC_BOLT_2
 EXEC_BOLT_3
  -PARTITION_9-> EXEC_BOLT_1');
