CREATE TUPLE tuple1 (aaa STRUCT<a1 STRING, a2 INT>, bbb INT, ccc INT, ddd STRING);
CREATE TUPLE tuple2 (eee STRUCT<e1 INT, e2 STRING>, fff INT, ggg INT, hhh STRING);

SET topology.metrics.enabled = true;
SET topology.metrics.interval.secs = 3;

FROM
 (tuple1
  JOIN tuple2 ON tuple1.aaa.a1 = tuple2.eee.e2 AND tuple1.bbb = tuple2.fff parallelism 2
  TO tuple1.*, tuple2.*
  EXPIRE 1min
 ) AS tuple3 USING kafka_spout() parallelism 2
EMIT * USING mongo_persist('db1', 'collection1') parallelism 1;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRUCT<a1 STRING, a2 INT>, bbb INT, ccc INT, ddd STRING), tuple2(eee STRUCT<e1 INT, e2 STRING>, fff INT, ggg INT, hhh STRING)]) parallelism=2
  -S(tuple1, tuple2)-> PARTITION_1
 PARTITION_1(join key grouping(tuple1([tuple1:aaa.a1, tuple1:bbb]) JOIN tuple2([tuple2:eee.e2, tuple2:fff])))
  -S(tuple1, tuple2)-> TUPLE_JOIN_2
 TUPLE_JOIN_2((tuple1([tuple1:aaa.a1, tuple1:bbb]) JOIN tuple2([tuple2:eee.e2, tuple2:fff])), memory_cache(), 1MINUTES, tuple3, [tuple1:*, tuple2:*]) parallelism=2
  -S(tuple3)-> PARTITION_3
 PARTITION_3(global grouping)
  -S(tuple3)-> EMIT_4
 EMIT_4(mongo_persist(db1, collection1), [*]) parallelism=1
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: TUPLE_JOIN_2
 TUPLE_JOIN_2
  incoming: PARTITION_1
  outgoing: PARTITION_3
 PARTITION_3
  incoming: TUPLE_JOIN_2
  outgoing: EMIT_4
 EMIT_4
  incoming: PARTITION_3
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 TUPLE_JOIN_2 {tuple3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
 PARTITION_3 {tuple3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
 EMIT_4 {tuple3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> PARTITION_1
 } parallelism=2
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> TUPLE_JOIN_2
  TUPLE_JOIN_2 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> PARTITION_3
 } parallelism=2
 EXEC_BOLT_2 {
  PARTITION_3 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> EMIT_4
 } parallelism=1
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1
  -PARTITION_3-> EXEC_BOLT_2
 EXEC_BOLT_2');

SUBMIT TOPOLOGY tuplejoin2;

@POST('tuple1', '{aaa:{a1:"aaa1", a2:123}, bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:{e1:456, e2:"aaa2"}, fff:10, ggg:101, hhh:"ddd2"}');
@POST('tuple1', '{aaa:{a1:"aaa3", a2:123}, bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:{a1:"aaa5", a2:123}, bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:{e1:456, e2:"aaa3"}, fff:10, ggg:101, hhh:"ddd2"}');
@POST('tuple1', '{aaa:{a1:"aaa4", a2:123}, bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:{e1:456, e2:"aaa6"}, fff:10, ggg:101, hhh:"ddd2"}');
@POST('tuple2', '{eee:{e1:456, e2:"aaa4"}, fff:10, ggg:101, hhh:"ddd2"}');
@POST('tuple2', '{eee:{e1:456, e2:"aaa5"}, fff:10, ggg:101, hhh:"ddd2"}');
@POST('tuple1', '{aaa:{a1:"aaa2", a2:123}, bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:{a1:"aaa6", a2:123}, bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple2', '{eee:{e1:456, e2:"aaa1"}, fff:10, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_4', '{aaa:{a1:"aaa1", a2:123}, bbb:10, ccc:100, ddd:"ddd1", eee:{e1:456, e2:"aaa1"}, fff:10, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_4', '{aaa:{a1:"aaa2", a2:123}, bbb:10, ccc:100, ddd:"ddd1", eee:{e1:456, e2:"aaa2"}, fff:10, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_4', '{aaa:{a1:"aaa3", a2:123}, bbb:10, ccc:100, ddd:"ddd1", eee:{e1:456, e2:"aaa3"}, fff:10, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_4', '{aaa:{a1:"aaa4", a2:123}, bbb:10, ccc:100, ddd:"ddd1", eee:{e1:456, e2:"aaa4"}, fff:10, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_4', '{aaa:{a1:"aaa5", a2:123}, bbb:10, ccc:100, ddd:"ddd1", eee:{e1:456, e2:"aaa5"}, fff:10, ggg:101, hhh:"ddd2"}');
@EMIT('EMIT_4', '{aaa:{a1:"aaa6", a2:123}, bbb:10, ccc:100, ddd:"ddd1", eee:{e1:456, e2:"aaa6"}, fff:10, ggg:101, hhh:"ddd2"}');
@PLAY(60);

STOP TOPOLOGY tuplejoin2;
