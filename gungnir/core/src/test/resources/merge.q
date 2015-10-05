CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING) PARTITIONED BY aaa;
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);

set default.parallelism = 4;

FROM tuple1, tuple2 USING kafka_spout() INTO s1;

FROM s1(tuple1)
EACH aaa AS id, bbb AS no
INTO s2;

FROM s1(tuple2)
EACH eee AS id, fff AS no
INTO s3;

FROM s2, s3
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING) partitioned by aaa, tuple2(eee STRING, fff INT, ggg INT, hhh STRING)]) parallelism=4
  -S(tuple1, tuple2)-> PARTITION_1
 PARTITION_1(tuple1(fields grouping(aaa)), tuple2(shuffle grouping))
  -S(tuple1)-> EACH_2
  -S(tuple2)-> EACH_3
 EACH_2([aaa AS id, bbb AS no]) parallelism=4
  -S(tuple1)-> MERGE_4
 EACH_3([eee AS id, fff AS no]) parallelism=4
  -S(tuple2)-> MERGE_4
 MERGE_4() parallelism=4
  -S(tuple1, tuple2)-> EMIT_5
 EMIT_5(mongo_persist(db1, collection1), [*]) parallelism=4
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2, EACH_3
 EACH_2
  incoming: PARTITION_1
  outgoing: MERGE_4
 EACH_3
  incoming: PARTITION_1
  outgoing: MERGE_4
 MERGE_4
  incoming: EACH_2, EACH_3
  outgoing: EMIT_5
 EMIT_5
  incoming: MERGE_4
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 EACH_2 {tuple1=[id, no]}
 EACH_3 {tuple2=[id, no]}
 MERGE_4 {tuple1=[id, no], tuple2=[id, no]}
 EMIT_5 {tuple1=[id, no], tuple2=[id, no]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> PARTITION_1
 } parallelism=4
 EXEC_BOLT_1 {
  PARTITION_1 -MultiDispatcher[TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> EACH_2, TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_3]
  EACH_2 -SingleDispatcher-> MERGE_4
  EACH_3 -SingleDispatcher-> MERGE_4
  MERGE_4 -SingleDispatcher-> EMIT_5
 } parallelism=4
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY merge;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:"eee1", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_5', '{id:"aaa1", no:10}');
@EMIT('EMIT_5', '{id:"eee1", no:11}');
@PLAY(60);

STOP TOPOLOGY merge;
