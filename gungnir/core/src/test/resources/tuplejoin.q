CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);

SET default.parallelism = 3;

FROM
 (tuple1
  JOIN tuple2 ON tuple1.aaa = tuple2.eee parallelism 2
  TO tuple1.*, tuple2.*
  EXPIRE 1min
 ) AS tuple3 USING kafka_spout()
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING), tuple2(eee STRING, fff INT, ggg INT, hhh STRING)]) parallelism=3
  -S(tuple1, tuple2)-> PARTITION_1
 PARTITION_1(join key grouping(tuple1(tuple1:aaa) JOIN tuple2(tuple2:eee)))
  -S(tuple1, tuple2)-> TUPLE_JOIN_2
 TUPLE_JOIN_2((tuple1(tuple1:aaa) JOIN tuple2(tuple2:eee)), memory_cache(), 1MINUTES, tuple3, [tuple1:*, tuple2:*]) parallelism=2
  -S(tuple3)-> EMIT_3
 EMIT_3(mongo_persist(db1, collection1), [*]) parallelism=3
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: TUPLE_JOIN_2
 TUPLE_JOIN_2
  incoming: PARTITION_1
  outgoing: EMIT_3
 EMIT_3
  incoming: TUPLE_JOIN_2
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 TUPLE_JOIN_2 {tuple3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
 EMIT_3 {tuple3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> PARTITION_1
 } parallelism=3
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> TUPLE_JOIN_2
  TUPLE_JOIN_2 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> EMIT_3
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY tuplejoin;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:"aaa2", fff:11, ggg:101, hhh:"ddd2"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:"aaa3", fff:11, ggg:101, hhh:"ddd2"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:"aaa6", fff:11, ggg:101, hhh:"ddd2"}');
@POST('tuple2', '{eee:"aaa4", fff:11, ggg:101, hhh:"ddd2"}');
@POST('tuple2', '{eee:"aaa5", fff:11, ggg:101, hhh:"ddd2"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa6", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:"aaa1", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1", eee:"aaa1", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd1", eee:"aaa2", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd1", eee:"aaa3", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd1", eee:"aaa4", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"ddd1", eee:"aaa5", fff:11, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa6", bbb:10, ccc:100, ddd:"ddd1", eee:"aaa6", fff:11, ggg:101, hhh:"ddd2"}');
@PLAY(60);

STOP TOPOLOGY tuplejoin;
