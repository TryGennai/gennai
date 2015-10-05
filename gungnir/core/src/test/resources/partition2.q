CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING) PARTITIONED BY aaa;
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);

set default.parallelism = 5;

FROM tuple1, tuple2  USING kafka_spout() INTO s1;

FROM s1(tuple2)
EACH eee AS aaa
INTO s2;

FROM s1(tuple1), s2
BEGIN GROUP BY aaa
EACH count() AS c parallelism 12
INTO s2;

FROM s2
EMIT * USING mongo_persist('db1', 'collection1');

FROM s2
TO STREAM
EACH count() AS c2 parallelism 1
EMIT * USING mongo_persist('db1', 'collection2');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING) partitioned by aaa, tuple2(eee STRING, fff INT, ggg INT, hhh STRING)]) parallelism=5
  -S(tuple2)-> PARTITION_1
  -S(tuple1)-> MERGE_2
 PARTITION_1(shuffle grouping)
  -S(tuple2)-> EACH_3
 MERGE_2() parallelism=5
  -S(tuple1, tuple2)-> PARTITION_4
 EACH_3([eee AS aaa]) parallelism=5
  -S(tuple2)-> MERGE_2
 PARTITION_4(fields grouping(aaa))
  -GS[aaa](tuple1, tuple2)-> EACH_5
 EACH_5([count() AS c]) parallelism=12
  -GS[aaa](tuple1, tuple2)-> EMIT_6
  -S(tuple1, tuple2)-> PARTITION_7
 EMIT_6(mongo_persist(db1, collection1), [*]) parallelism=5
 PARTITION_7(global grouping)
  -S(tuple1, tuple2)-> EACH_8
 EACH_8([count() AS c2]) parallelism=1
  -S(tuple1, tuple2)-> PARTITION_9
 PARTITION_9(shuffle grouping)
  -S(tuple1, tuple2)-> EMIT_10
 EMIT_10(mongo_persist(db1, collection2), [*]) parallelism=5
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1, MERGE_2
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_3
 MERGE_2
  incoming: EACH_3, SPOUT_0
  outgoing: PARTITION_4
 EACH_3
  incoming: PARTITION_1
  outgoing: MERGE_2
 PARTITION_4
  incoming: MERGE_2
  outgoing: EACH_5
 EACH_5
  incoming: PARTITION_4
  outgoing: EMIT_6, PARTITION_7
 EMIT_6
  incoming: EACH_5
  outgoing: -
 PARTITION_7
  incoming: EACH_5
  outgoing: EACH_8
 EACH_8
  incoming: PARTITION_7
  outgoing: PARTITION_9
 PARTITION_9
  incoming: EACH_8
  outgoing: EMIT_10
 EMIT_10
  incoming: PARTITION_9
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 PARTITION_1 {tuple2=[eee, fff, ggg, hhh]}
 EACH_3 {tuple2=[aaa]}
 MERGE_2 {tuple2=[aaa], tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_4 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[aaa]}
 EACH_5 {tuple1=[c], tuple2=[c]}
 EMIT_6 {tuple1=[c], tuple2=[c]}
 PARTITION_7 {tuple1=[c], tuple2=[c]}
 EACH_8 {tuple1=[c2], tuple2=[c2]}
 PARTITION_9 {tuple1=[c2], tuple2=[c2]}
 EMIT_10 {tuple1=[c2], tuple2=[c2]}
Group fields:
 EACH_5 aaa
 EMIT_6 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -MultiDispatcher[TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> MERGE_2, TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> PARTITION_1]
  MERGE_2 -SingleDispatcher-> PARTITION_4
 } parallelism=5
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_3
  EACH_3 -SingleDispatcher-> MERGE_2
  MERGE_2 -SingleDispatcher-> PARTITION_4
 } parallelism=5
 EXEC_BOLT_2 {
  PARTITION_4 TupleNameFilter(tupleName=[tuple1, tuple2])-GroupingDispatcher(aaa)-> EACH_5
  EACH_5 -MultiDispatcher[-GroupingDispatcher(aaa)-> EMIT_6, -SingleDispatcher-> PARTITION_7]
 } parallelism=12
 EXEC_BOLT_3 {
  PARTITION_7 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> EACH_8
  EACH_8 -SingleDispatcher-> PARTITION_9
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_9 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> EMIT_10
 } parallelism=5
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
  -PARTITION_4-> EXEC_BOLT_2
 EXEC_BOLT_1
  -PARTITION_4-> EXEC_BOLT_2
 EXEC_BOLT_2
  -PARTITION_7-> EXEC_BOLT_3
 EXEC_BOLT_3
  -PARTITION_9-> EXEC_BOLT_1');
