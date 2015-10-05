CREATE TUPLE tuple1 (aaa STRING, bbb STRING, iii STRING);
CREATE TUPLE tuple2 (bbb STRING, ccc STRING, iii STRING);
CREATE TUPLE tuple3 (ddd STRING, eee STRING, iii STRING);
CREATE TUPLE tuple4 (fff STRING, ggg STRING, iii STRING);
CREATE TUPLE tuple5 (ggg STRING, hhh STRING, iii STRING);
CREATE TUPLE tuple6 (ggg STRING, hhh STRING, iii STRING);

SET topology.metrics.enabled = true;
SET topology.metrics.interval.secs = 3;

FROM
 (tuple1
  JOIN tuple2 ON tuple1.aaa = tuple2.bbb parallelism 2
  JOIN tuple3 ON tuple2.ccc = tuple3.ddd parallelism 3
  JOIN tuple4 ON tuple3.eee = tuple4.fff parallelism 4
  JOIN tuple5 ON tuple4.fff = tuple5.ggg parallelism 3
  JOIN tuple6 ON tuple5.ggg = tuple6.hhh parallelism 2
  TO tuple1.iii, tuple2.iii AS jjj, tuple3.iii AS kkk, tuple4.iii AS lll, tuple5.iii AS mmm, tuple6.iii AS nnn, tuple1.bbb
  EXPIRE 1min
  USING file_cache()
 ) AS tuple10
 USING kafka_spout() parallelism 4
 EMIT * USING web_emit('http://localhost:3000/update') parallelism 5;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb STRING, iii STRING), tuple2(bbb STRING, ccc STRING, iii STRING), tuple3(ddd STRING, eee STRING, iii STRING), tuple4(fff STRING, ggg STRING, iii STRING), tuple5(ggg STRING, hhh STRING, iii STRING), tuple6(ggg STRING, hhh STRING, iii STRING)]) parallelism=4
  -S(tuple1, tuple2)-> PARTITION_1
  -S(tuple3)-> MERGE_2
  -S(tuple4, tuple5, tuple6)-> MERGE_3
 PARTITION_1(join key grouping(tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb)))
  -S(tuple1, tuple2)-> TUPLE_JOIN_4
 MERGE_2() parallelism=1
  -S(tuple1, tuple3)-> PARTITION_5
 MERGE_3() parallelism=1
  -S(tuple1, tuple4, tuple5, tuple6)-> PARTITION_6
 TUPLE_JOIN_4((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb)), file_cache(), 1MINUTES) parallelism=2
  -S(tuple1)-> MERGE_2
 PARTITION_5(join key grouping((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb))(tuple2:ccc) JOIN tuple3(tuple3:ddd)))
  -S(tuple1, tuple3)-> TUPLE_JOIN_7
 PARTITION_6(join key grouping(((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb))(tuple2:ccc) JOIN tuple3(tuple3:ddd))(tuple3:eee) JOIN tuple4(tuple4:fff) JOIN tuple5(tuple5:ggg) JOIN tuple6(tuple6:hhh)))
  -S(tuple1, tuple4, tuple5, tuple6)-> TUPLE_JOIN_8
 TUPLE_JOIN_7(((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb))(tuple2:ccc) JOIN tuple3(tuple3:ddd)), file_cache(), 1MINUTES) parallelism=3
  -S(tuple1)-> MERGE_3
 TUPLE_JOIN_8((((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb))(tuple2:ccc) JOIN tuple3(tuple3:ddd))(tuple3:eee) JOIN tuple4(tuple4:fff) JOIN tuple5(tuple5:ggg) JOIN tuple6(tuple6:hhh)), file_cache(), 1MINUTES, tuple10, [tuple1:iii, tuple2:iii AS jjj, tuple3:iii AS kkk, tuple4:iii AS lll, tuple5:iii AS mmm, tuple6:iii AS nnn, tuple1:bbb]) parallelism=4
  -S(tuple10)-> EMIT_9
 EMIT_9(web_emit(http://localhost:3000/update), [*]) parallelism=5
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1, MERGE_2, MERGE_3
 PARTITION_1
  incoming: SPOUT_0
  outgoing: TUPLE_JOIN_4
 MERGE_2
  incoming: TUPLE_JOIN_4, SPOUT_0
  outgoing: PARTITION_5
 MERGE_3
  incoming: TUPLE_JOIN_7, SPOUT_0
  outgoing: PARTITION_6
 TUPLE_JOIN_4
  incoming: PARTITION_1
  outgoing: MERGE_2
 PARTITION_5
  incoming: MERGE_2
  outgoing: TUPLE_JOIN_7
 PARTITION_6
  incoming: MERGE_3
  outgoing: TUPLE_JOIN_8
 TUPLE_JOIN_7
  incoming: PARTITION_5
  outgoing: MERGE_3
 TUPLE_JOIN_8
  incoming: PARTITION_6
  outgoing: EMIT_9
 EMIT_9
  incoming: TUPLE_JOIN_8
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, iii], tuple2=[bbb, ccc, iii], tuple3=[ddd, eee, iii], tuple4=[fff, ggg, iii], tuple5=[ggg, hhh, iii], tuple6=[ggg, hhh, iii]}
 PARTITION_1 {tuple1=[aaa, bbb, iii], tuple2=[bbb, ccc, iii]}
 TUPLE_JOIN_4 {tuple1=[+tuple1:iii, +tuple1:bbb, jjj, +tuple2:ccc]}
 MERGE_2 {tuple1=[+tuple1:iii, +tuple1:bbb, jjj, +tuple2:ccc], tuple3=[ddd, eee, iii]}
 PARTITION_5 {tuple1=[+tuple1:iii, +tuple1:bbb, jjj, +tuple2:ccc], tuple3=[ddd, eee, iii]}
 TUPLE_JOIN_7 {tuple1=[+tuple1:iii, +tuple1:bbb, jjj, +tuple2:ccc, kkk, +tuple3:eee]}
 MERGE_3 {tuple1=[+tuple1:iii, +tuple1:bbb, jjj, +tuple2:ccc, kkk, +tuple3:eee], tuple4=[fff, ggg, iii], tuple5=[ggg, hhh, iii], tuple6=[ggg, hhh, iii]}
 PARTITION_6 {tuple1=[+tuple1:iii, +tuple1:bbb, jjj, +tuple2:ccc, kkk, +tuple3:eee], tuple4=[fff, ggg, iii], tuple5=[ggg, hhh, iii], tuple6=[ggg, hhh, iii]}
 TUPLE_JOIN_8 {tuple10=[iii, jjj, kkk, lll, mmm, nnn, bbb]}
 EMIT_9 {tuple10=[iii, jjj, kkk, lll, mmm, nnn, bbb]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -MultiDispatcher[TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> MERGE_2, TupleNameFilter(tupleName=[tuple4, tuple5, tuple6])-SingleDispatcher-> MERGE_3, TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> PARTITION_1]
  MERGE_2 -SingleDispatcher-> PARTITION_5
  MERGE_3 -SingleDispatcher-> PARTITION_6
 } parallelism=4
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> TUPLE_JOIN_4
  TUPLE_JOIN_4 TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> MERGE_2
  MERGE_2 -SingleDispatcher-> PARTITION_5
 } parallelism=2
 EXEC_BOLT_2 {
  PARTITION_5 TupleNameFilter(tupleName=[tuple1, tuple3])-SingleDispatcher-> TUPLE_JOIN_7
  TUPLE_JOIN_7 TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> MERGE_3
  MERGE_3 -SingleDispatcher-> PARTITION_6
 } parallelism=3
 EXEC_BOLT_3 {
  PARTITION_6 TupleNameFilter(tupleName=[tuple1, tuple4, tuple5, tuple6])-SingleDispatcher-> TUPLE_JOIN_8
  TUPLE_JOIN_8 TupleNameFilter(tupleName=[tuple10])-SingleDispatcher-> EMIT_9
 } parallelism=5
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_2
  -PARTITION_6-> EXEC_BOLT_3
 EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_2
 EXEC_BOLT_2
  -PARTITION_6-> EXEC_BOLT_3
 EXEC_BOLT_3');

SUBMIT TOPOLOGY tuplejoin3;

@POST('tuple1', '{aaa:"aaa1", bbb:"bbb1", iii:"iii1"}');
@POST('tuple2', '{bbb:"aaa1", ccc:"ccc1", iii:"iii2"}');
@POST('tuple3', '{ddd:"ccc1", eee:"eee1", iii:"iii3"}');
@POST('tuple4', '{fff:"eee1", ggg:"ggg1", iii:"iii4"}');
@POST('tuple5', '{ggg:"eee1", hhh:"hhh1", iii:"iii5"}');
@POST('tuple6', '{ggg:"ggg1", hhh:"eee1", iii:"iii6"}');
@EMIT('EMIT_9', '{iii:"iii1", jjj:"iii2", kkk:"iii3", lll:"iii4", mmm:"iii5", nnn:"iii6", bbb:"bbb1"}');
@PLAY(60);

STOP TOPOLOGY tuplejoin3;
