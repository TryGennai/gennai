CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

SET default.parallelism = 3;

FROM tuple1 USING kafka_spout() INTO s1;

FROM s1
EACH aaa, concat(aaa, '-', cast(bbb AS STRING)) AS xxx
INTO s2;

FROM s1(tuple1) AS tuple3
EACH ddd, concat(ddd, '-', cast(ccc AS STRING)) AS yyy
INTO s3;

FROM
 (s2(tuple1)
  JOIN s3(tuple3) ON tuple1.aaa = tuple3.ddd
  TO tuple1.*, tuple3.*
  EXPIRE 10sec
 ) AS tuple4
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=3
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> EACH_2
  -S(tuple1)-> RENAME_3
 EACH_2([aaa, concat(aaa, -, cast(bbb, STRING) AS bbb) AS xxx]) parallelism=3
  -S(tuple1)-> MERGE_4
 RENAME_3(tuple1, tuple3) parallelism=3
  -S(tuple3)-> EACH_5
 MERGE_4() parallelism=3
  -S(tuple1, tuple3)-> PARTITION_6
 EACH_5([ddd, concat(ddd, -, cast(ccc, STRING) AS ccc) AS yyy]) parallelism=3
  -S(tuple3)-> MERGE_4
 PARTITION_6(join key grouping(tuple1(tuple1:aaa) JOIN tuple3(tuple3:ddd)))
  -S(tuple1, tuple3)-> TUPLE_JOIN_7
 TUPLE_JOIN_7((tuple1(tuple1:aaa) JOIN tuple3(tuple3:ddd)), memory_cache(), 10SECONDS, tuple4, [tuple1:*, tuple3:*]) parallelism=3
  -S(tuple4)-> EMIT_8
 EMIT_8(mongo_persist(db1, collection1), [*]) parallelism=3
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2, RENAME_3
 EACH_2
  incoming: PARTITION_1
  outgoing: MERGE_4
 RENAME_3
  incoming: PARTITION_1
  outgoing: EACH_5
 MERGE_4
  incoming: EACH_2, EACH_5
  outgoing: PARTITION_6
 EACH_5
  incoming: RENAME_3
  outgoing: MERGE_4
 PARTITION_6
  incoming: MERGE_4
  outgoing: TUPLE_JOIN_7
 TUPLE_JOIN_7
  incoming: PARTITION_6
  outgoing: EMIT_8
 EMIT_8
  incoming: TUPLE_JOIN_7
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd]}
 EACH_2 {tuple1=[aaa, xxx]}
 RENAME_3 {tuple3=[aaa, bbb, ccc, ddd]}
 EACH_5 {tuple3=[ddd, yyy]}
 MERGE_4 {tuple1=[aaa, xxx], tuple3=[ddd, yyy]}
 PARTITION_6 {tuple1=[aaa, xxx], tuple3=[ddd, yyy]}
 TUPLE_JOIN_7 {tuple4=[aaa, xxx, ddd, yyy]}
 EMIT_8 {tuple4=[aaa, xxx, ddd, yyy]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=3
 EXEC_BOLT_1 {
  PARTITION_1 -MultiDispatcher[-SingleDispatcher-> EACH_2, TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> RENAME_3]
  EACH_2 TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> MERGE_4
  RENAME_3 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> EACH_5
  MERGE_4 -SingleDispatcher-> PARTITION_6
  EACH_5 -SingleDispatcher-> MERGE_4
 } parallelism=3
 EXEC_BOLT_2 {
  PARTITION_6 TupleNameFilter(tupleName=[tuple1, tuple3])-SingleDispatcher-> TUPLE_JOIN_7
  TUPLE_JOIN_7 TupleNameFilter(tupleName=[tuple4])-SingleDispatcher-> EMIT_8
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1
  -PARTITION_6-> EXEC_BOLT_2
 EXEC_BOLT_2');

SUBMIT TOPOLOGY tuplejoin8;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"aaa1"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"aaa7"}');
@POST('tuple1', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"aaa5"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"aaa4"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"aaa8"}');
@POST('tuple1', '{aaa:"aaa6", bbb:10, ccc:100, ddd:"aaa6"}');
@EMIT('EMIT_8', '{aaa:"aaa1", xxx:"aaa1-10", ddd:"aaa1", yyy:"aaa1-100"}');
@EMIT('EMIT_8', '{aaa:"aaa4", xxx:"aaa4-10", ddd:"aaa4", yyy:"aaa4-100"}');
@EMIT('EMIT_8', '{aaa:"aaa5", xxx:"aaa5-10", ddd:"aaa5", yyy:"aaa5-100"}');
@EMIT('EMIT_8', '{aaa:"aaa6", xxx:"aaa6-10", ddd:"aaa6", yyy:"aaa6-100"}');
@PLAY(60);

STOP TOPOLOGY tuplejoin8;
