CREATE TUPLE tuple1 (aaa STRING, bbb STRING, iii STRING);
CREATE TUPLE tuple2 (bbb STRING, ccc STRING, iii STRING);
CREATE TUPLE tuple3 (ddd STRING, eee STRING, iii STRING);
CREATE TUPLE tuple4 (fff STRING, ggg STRING, iii STRING);
CREATE TUPLE tuple5 (ggg STRING, hhh STRING, iii STRING);
CREATE TUPLE tuple6 (ggg STRING, hhh STRING, iii STRING);

FROM
 (tuple1
  JOIN tuple2 ON tuple1.aaa = tuple2.bbb
  JOIN tuple3 ON tuple1.aaa = tuple3.ddd
  JOIN tuple4 ON tuple1.aaa = tuple4.fff
  JOIN tuple5 ON tuple1.aaa = tuple5.ggg
  JOIN tuple6 ON tuple1.aaa = tuple6.hhh
  TO tuple1.iii, tuple2.iii AS jjj, tuple3.iii AS kkk, tuple4.iii AS lll, tuple5.iii AS mmm, tuple6.iii AS nnn, tuple5.ggg, tuple6.hhh
  EXPIRE 1min
 ) AS tuple10
USING kafka_spout() parallelism 4
EMIT * USING web_emit('http://localhost:3000/update') parallelism 5;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb STRING, iii STRING), tuple2(bbb STRING, ccc STRING, iii STRING), tuple3(ddd STRING, eee STRING, iii STRING), tuple4(fff STRING, ggg STRING, iii STRING), tuple5(ggg STRING, hhh STRING, iii STRING), tuple6(ggg STRING, hhh STRING, iii STRING)]) parallelism=4
  -S(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6)-> PARTITION_1
 PARTITION_1(join key grouping(tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb) JOIN tuple3(tuple3:ddd) JOIN tuple4(tuple4:fff) JOIN tuple5(tuple5:ggg) JOIN tuple6(tuple6:hhh)))
  -S(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6)-> TUPLE_JOIN_2
 TUPLE_JOIN_2((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb) JOIN tuple3(tuple3:ddd) JOIN tuple4(tuple4:fff) JOIN tuple5(tuple5:ggg) JOIN tuple6(tuple6:hhh)), memory_cache(), 1MINUTES, tuple10, [tuple1:iii, tuple2:iii AS jjj, tuple3:iii AS kkk, tuple4:iii AS lll, tuple5:iii AS mmm, tuple6:iii AS nnn, tuple5:ggg, tuple6:hhh]) parallelism=1
  -S(tuple10)-> PARTITION_3
 PARTITION_3(shuffle grouping)
  -S(tuple10)-> EMIT_4
 EMIT_4(web_emit(http://localhost:3000/update), [*]) parallelism=5
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
 SPOUT_0 {tuple1=[aaa, bbb, iii], tuple2=[bbb, ccc, iii], tuple3=[ddd, eee, iii], tuple4=[fff, ggg, iii], tuple5=[ggg, hhh, iii], tuple6=[ggg, hhh, iii]}
 PARTITION_1 {tuple1=[aaa, bbb, iii], tuple2=[bbb, ccc, iii], tuple3=[ddd, eee, iii], tuple4=[fff, ggg, iii], tuple5=[ggg, hhh, iii], tuple6=[ggg, hhh, iii]}
 TUPLE_JOIN_2 {tuple10=[iii, jjj, kkk, lll, mmm, nnn, ggg, hhh]}
 PARTITION_3 {tuple10=[iii, jjj, kkk, lll, mmm, nnn, ggg, hhh]}
 EMIT_4 {tuple10=[iii, jjj, kkk, lll, mmm, nnn, ggg, hhh]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 TupleNameFilter(tupleName=[tuple1, tuple2, tuple3, tuple4, tuple5, tuple6])-SingleDispatcher-> PARTITION_1
 } parallelism=4
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple1, tuple2, tuple3, tuple4, tuple5, tuple6])-SingleDispatcher-> TUPLE_JOIN_2
  TUPLE_JOIN_2 TupleNameFilter(tupleName=[tuple10])-SingleDispatcher-> PARTITION_3
 } parallelism=1
 EXEC_BOLT_2 {
  PARTITION_3 TupleNameFilter(tupleName=[tuple10])-SingleDispatcher-> EMIT_4
 } parallelism=5
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1
  -PARTITION_3-> EXEC_BOLT_2
 EXEC_BOLT_2');

SUBMIT TOPOLOGY tuplejoin4;

@POST('tuple1', '{aaa:"aaa1", bbb:"bbb1", iii:"iii1"}');
@POST('tuple2', '{bbb:"aaa1", ccc:"ccc1", iii:"iii2"}');
@POST('tuple3', '{ddd:"aaa1", eee:"eee1", iii:"iii3"}');
@POST('tuple4', '{fff:"aaa1", ggg:"ggg1", iii:"iii4"}');
@POST('tuple5', '{ggg:"aaa1", hhh:"hhh1", iii:"iii5"}');
@POST('tuple6', '{ggg:"ggg1", hhh:"aaa1", iii:"iii6"}');
@EMIT('EMIT_4', '{iii:"iii1", jjj:"iii2", kkk:"iii3", lll:"iii4", mmm:"iii5", nnn:"iii6", ggg:"aaa1", hhh:"aaa1"}');
@PLAY(60);

STOP TOPOLOGY tuplejoin4;
