CREATE TUPLE tuple1 (aaa STRING, bbb STRING, ccc STRING, ddd STRING);
CREATE TUPLE tuple2 (aaa STRING, bbb STRING, ccc STRING, ddd STRING);
CREATE TUPLE tuple3 (aaa STRING, bbb STRING, ccc STRING, ddd STRING);
CREATE TUPLE tuple4 (aaa STRING, bbb STRING, ccc STRING, ddd STRING);
CREATE TUPLE tuple5 (aaa STRING, bbb STRING, ccc STRING, ddd STRING);

FROM
 tuple5 AS ua5,
 (tuple1
  JOIN tuple2 ON tuple1.aaa = tuple2.bbb parallelism 15
  JOIN tuple3 ON tuple2.bbb = tuple3.ddd
  TO tuple.bbb AS iii, tuple2.bbb AS jjj, tuple3.bbb AS kkk
  EXPIRE 1min
 ) AS tuple10,
 (tuple3
  JOIN tuple4 ON tuple3.aaa = tuple4.aaa
  TO tuple3.ddd AS xxx, tuple4.ddd AS yyy
  EXPIRE 2min
 ) AS tuple11
USING kafka_spout() parallelism 3
EMIT * USING web_emit('http://localhost:3000/update') parallelism 3;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple5(aaa STRING, bbb STRING, ccc STRING, ddd STRING), tuple1(aaa STRING, bbb STRING, ccc STRING, ddd STRING), tuple2(aaa STRING, bbb STRING, ccc STRING, ddd STRING), tuple3(aaa STRING, bbb STRING, ccc STRING, ddd STRING), tuple4(aaa STRING, bbb STRING, ccc STRING, ddd STRING)]) parallelism=3
  -S(tuple5)-> PARTITION_1
  -S(tuple1, tuple2, tuple3)-> PARTITION_2
  -S(tuple3, tuple4)-> PARTITION_3
 PARTITION_1(shuffle grouping)
  -S(tuple5)-> MERGE_4
 PARTITION_2(join key grouping(tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb) JOIN tuple3(tuple3:ddd)))
  -S(tuple1, tuple2, tuple3)-> TUPLE_JOIN_5
 PARTITION_3(join key grouping(tuple3(tuple3:aaa) JOIN tuple4(tuple4:aaa)))
  -S(tuple3, tuple4)-> TUPLE_JOIN_6
 MERGE_4() parallelism=1
  -S(tuple5, tuple10, tuple11)-> PARTITION_7
 TUPLE_JOIN_5((tuple1(tuple1:aaa) JOIN tuple2(tuple2:bbb) JOIN tuple3(tuple3:ddd)), memory_cache(), 1MINUTES, tuple10, [tuple:bbb AS iii, tuple2:bbb AS jjj, tuple3:bbb AS kkk]) parallelism=15
  -S(tuple10)-> MERGE_4
 TUPLE_JOIN_6((tuple3(tuple3:aaa) JOIN tuple4(tuple4:aaa)), memory_cache(), 2MINUTES, tuple11, [tuple3:ddd AS xxx, tuple4:ddd AS yyy]) parallelism=1
  -S(tuple11)-> MERGE_4
 PARTITION_7(shuffle grouping)
  -S(tuple5, tuple10, tuple11)-> EMIT_8
 EMIT_8(web_emit(http://localhost:3000/update), [*]) parallelism=3
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1, PARTITION_2, PARTITION_3
 PARTITION_1
  incoming: SPOUT_0
  outgoing: MERGE_4
 PARTITION_2
  incoming: SPOUT_0
  outgoing: TUPLE_JOIN_5
 PARTITION_3
  incoming: SPOUT_0
  outgoing: TUPLE_JOIN_6
 MERGE_4
  incoming: PARTITION_1, TUPLE_JOIN_5, TUPLE_JOIN_6
  outgoing: PARTITION_7
 TUPLE_JOIN_5
  incoming: PARTITION_2
  outgoing: MERGE_4
 TUPLE_JOIN_6
  incoming: PARTITION_3
  outgoing: MERGE_4
 PARTITION_7
  incoming: MERGE_4
  outgoing: EMIT_8
 EMIT_8
  incoming: PARTITION_7
  outgoing: -
Output fields:
 SPOUT_0 {tuple5=[aaa, bbb, ccc, ddd], tuple1=[aaa, bbb, ccc, ddd], tuple2=[aaa, bbb, ccc, ddd], tuple3=[aaa, bbb, ccc, ddd], tuple4=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple5=[aaa, bbb, ccc, ddd]}
 PARTITION_2 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[aaa, bbb, ccc, ddd], tuple3=[aaa, bbb, ccc, ddd]}
 PARTITION_3 {tuple3=[aaa, bbb, ccc, ddd], tuple4=[aaa, bbb, ccc, ddd]}
 TUPLE_JOIN_5 {tuple10=[iii, jjj, kkk]}
 TUPLE_JOIN_6 {tuple11=[xxx, yyy]}
 MERGE_4 {tuple5=[aaa, bbb, ccc, ddd], tuple10=[iii, jjj, kkk], tuple11=[xxx, yyy]}
 PARTITION_7 {tuple5=[aaa, bbb, ccc, ddd], tuple10=[iii, jjj, kkk], tuple11=[xxx, yyy]}
 EMIT_8 {tuple5=[aaa, bbb, ccc, ddd], tuple10=[iii, jjj, kkk], tuple11=[xxx, yyy]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -MultiDispatcher[TupleNameFilter(tupleName=[tuple5])-SingleDispatcher-> PARTITION_1, TupleNameFilter(tupleName=[tuple1, tuple2, tuple3])-SingleDispatcher-> PARTITION_2, TupleNameFilter(tupleName=[tuple3, tuple4])-SingleDispatcher-> PARTITION_3]
 } parallelism=3
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple5])-SingleDispatcher-> MERGE_4
  MERGE_4 -SingleDispatcher-> PARTITION_7
 } parallelism=1
 EXEC_BOLT_2 {
  PARTITION_2 TupleNameFilter(tupleName=[tuple1, tuple2, tuple3])-SingleDispatcher-> TUPLE_JOIN_5
  TUPLE_JOIN_5 TupleNameFilter(tupleName=[tuple10])-SingleDispatcher-> MERGE_4
  MERGE_4 -SingleDispatcher-> PARTITION_7
 } parallelism=15
 EXEC_BOLT_3 {
  PARTITION_3 TupleNameFilter(tupleName=[tuple3, tuple4])-SingleDispatcher-> TUPLE_JOIN_6
  TUPLE_JOIN_6 TupleNameFilter(tupleName=[tuple11])-SingleDispatcher-> MERGE_4
  MERGE_4 -SingleDispatcher-> PARTITION_7
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_7 TupleNameFilter(tupleName=[tuple5, tuple10, tuple11])-SingleDispatcher-> EMIT_8
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
  -PARTITION_2-> EXEC_BOLT_2
  -PARTITION_3-> EXEC_BOLT_3
 EXEC_BOLT_1
  -PARTITION_7-> EXEC_BOLT_1
 EXEC_BOLT_2
  -PARTITION_7-> EXEC_BOLT_1
 EXEC_BOLT_3
  -PARTITION_7-> EXEC_BOLT_1');
