CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING) PARTITIONED BY aaa;
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);

set default.parallelism = 5;

FROM tuple1, tuple2  USING kafka_spout() INTO s1;

FROM s1(tuple2)
EACH eee AS aaa
INTO s2;

FROM s1(tuple1), s2
EACH aaa AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING) partitioned by aaa, tuple2(eee STRING, fff INT, ggg INT, hhh STRING)]) parallelism=5
  -S(tuple2)-> PARTITION_1
  -S(tuple1)-> MERGE_2
 PARTITION_1(shuffle grouping)
  -S(tuple2)-> EACH_3
 MERGE_2() parallelism=5
  -S(tuple1, tuple2)-> EACH_4
 EACH_3([eee AS aaa]) parallelism=5
  -S(tuple2)-> MERGE_2
 EACH_4([aaa AS id]) parallelism=5
  -S(tuple1, tuple2)-> PARTITION_5
 PARTITION_5(global grouping)
  -S(tuple1, tuple2)-> EACH_6
 EACH_6([count() AS c]) parallelism=1
  -S(tuple1, tuple2)-> PARTITION_7
 PARTITION_7(shuffle grouping)
  -S(tuple1, tuple2)-> EMIT_8
 EMIT_8(mongo_persist(db1, collection1), [*]) parallelism=5
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1, MERGE_2
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_3
 MERGE_2
  incoming: EACH_3, SPOUT_0
  outgoing: EACH_4
 EACH_3
  incoming: PARTITION_1
  outgoing: MERGE_2
 EACH_4
  incoming: MERGE_2
  outgoing: PARTITION_5
 PARTITION_5
  incoming: EACH_4
  outgoing: EACH_6
 EACH_6
  incoming: PARTITION_5
  outgoing: PARTITION_7
 PARTITION_7
  incoming: EACH_6
  outgoing: EMIT_8
 EMIT_8
  incoming: PARTITION_7
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 PARTITION_1 {tuple2=[eee, fff, ggg, hhh]}
 EACH_3 {tuple2=[aaa]}
 MERGE_2 {tuple2=[aaa], tuple1=[aaa, bbb, ccc, ddd]}
 EACH_4 {tuple1=[id], tuple2=[id]}
 PARTITION_5 {tuple1=[id], tuple2=[id]}
 EACH_6 {tuple1=[c], tuple2=[c]}
 PARTITION_7 {tuple1=[c], tuple2=[c]}
 EMIT_8 {tuple1=[c], tuple2=[c]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -MultiDispatcher[TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> MERGE_2, TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> PARTITION_1]
  MERGE_2 -SingleDispatcher-> EACH_4
  EACH_4 -SingleDispatcher-> PARTITION_5
 } parallelism=5
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_3
  EACH_3 -SingleDispatcher-> MERGE_2
  MERGE_2 -SingleDispatcher-> EACH_4
  EACH_4 -SingleDispatcher-> PARTITION_5
 } parallelism=5
 EXEC_BOLT_2 {
  PARTITION_5 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> EACH_6
  EACH_6 -SingleDispatcher-> PARTITION_7
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_7 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> EMIT_8
 } parallelism=5
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_2
 EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_2
 EXEC_BOLT_2
  -PARTITION_7-> EXEC_BOLT_1');
