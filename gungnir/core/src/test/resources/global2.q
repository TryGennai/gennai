CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING) PARTITIONED BY aaa;
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);
CREATE TUPLE tuple3 (iii STRING, jjj INT, kkk INT, lll STRING);

set default.parallelism = 4;

FROM tuple1, tuple2, tuple3  USING kafka_spout() INTO s1;

FROM s1(tuple1)
EACH aaa AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection1');

FROM s1(tuple2)
EACH eee AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection2');

FROM s1(tuple3)
EACH iii AS id
EACH count() AS c parallelism 1
EMIT * USING mongo_persist('db1', 'collection3');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING) partitioned by aaa, tuple2(eee STRING, fff INT, ggg INT, hhh STRING), tuple3(iii STRING, jjj INT, kkk INT, lll STRING)]) parallelism=4
  -S(tuple1, tuple2, tuple3)-> PARTITION_1
 PARTITION_1(tuple1(fields grouping(aaa)), tuple2(shuffle grouping), tuple3(shuffle grouping))
  -S(tuple1)-> EACH_2
  -S(tuple2)-> EACH_3
  -S(tuple3)-> EACH_4
 EACH_2([aaa AS id]) parallelism=4
  -S(tuple1)-> PARTITION_5
 EACH_3([eee AS id]) parallelism=4
  -S(tuple2)-> PARTITION_6
 EACH_4([iii AS id]) parallelism=4
  -S(tuple3)-> PARTITION_7
 PARTITION_5(global grouping)
  -S(tuple1)-> EACH_8
 PARTITION_6(global grouping)
  -S(tuple2)-> EACH_9
 PARTITION_7(global grouping)
  -S(tuple3)-> EACH_10
 EACH_8([count() AS c]) parallelism=1
  -S(tuple1)-> PARTITION_11
 EACH_9([count() AS c]) parallelism=1
  -S(tuple2)-> PARTITION_12
 EACH_10([count() AS c]) parallelism=1
  -S(tuple3)-> PARTITION_13
 PARTITION_11(shuffle grouping)
  -S(tuple1)-> EMIT_14
 PARTITION_12(shuffle grouping)
  -S(tuple2)-> EMIT_15
 PARTITION_13(shuffle grouping)
  -S(tuple3)-> EMIT_16
 EMIT_14(mongo_persist(db1, collection1), [*]) parallelism=4
 EMIT_15(mongo_persist(db1, collection2), [*]) parallelism=4
 EMIT_16(mongo_persist(db1, collection3), [*]) parallelism=4
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2, EACH_3, EACH_4
 EACH_2
  incoming: PARTITION_1
  outgoing: PARTITION_5
 EACH_3
  incoming: PARTITION_1
  outgoing: PARTITION_6
 EACH_4
  incoming: PARTITION_1
  outgoing: PARTITION_7
 PARTITION_5
  incoming: EACH_2
  outgoing: EACH_8
 PARTITION_6
  incoming: EACH_3
  outgoing: EACH_9
 PARTITION_7
  incoming: EACH_4
  outgoing: EACH_10
 EACH_8
  incoming: PARTITION_5
  outgoing: PARTITION_11
 EACH_9
  incoming: PARTITION_6
  outgoing: PARTITION_12
 EACH_10
  incoming: PARTITION_7
  outgoing: PARTITION_13
 PARTITION_11
  incoming: EACH_8
  outgoing: EMIT_14
 PARTITION_12
  incoming: EACH_9
  outgoing: EMIT_15
 PARTITION_13
  incoming: EACH_10
  outgoing: EMIT_16
 EMIT_14
  incoming: PARTITION_11
  outgoing: -
 EMIT_15
  incoming: PARTITION_12
  outgoing: -
 EMIT_16
  incoming: PARTITION_13
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 EACH_2 {tuple1=[id]}
 EACH_3 {tuple2=[id]}
 EACH_4 {tuple3=[id]}
 PARTITION_5 {tuple1=[id]}
 PARTITION_6 {tuple2=[id]}
 PARTITION_7 {tuple3=[id]}
 EACH_8 {tuple1=[c]}
 EACH_9 {tuple2=[c]}
 EACH_10 {tuple3=[c]}
 PARTITION_11 {tuple1=[c]}
 PARTITION_12 {tuple2=[c]}
 PARTITION_13 {tuple3=[c]}
 EMIT_14 {tuple1=[c]}
 EMIT_15 {tuple2=[c]}
 EMIT_16 {tuple3=[c]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 TupleNameFilter(tupleName=[tuple1, tuple2, tuple3])-SingleDispatcher-> PARTITION_1
 } parallelism=4
 EXEC_BOLT_1 {
  PARTITION_1 -MultiDispatcher[TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> EACH_2, TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_3, TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> EACH_4]
  EACH_2 -SingleDispatcher-> PARTITION_5
  EACH_3 -SingleDispatcher-> PARTITION_6
  EACH_4 -SingleDispatcher-> PARTITION_7
 } parallelism=4
 EXEC_BOLT_2 {
  PARTITION_5 TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> EACH_8
  EACH_8 -SingleDispatcher-> PARTITION_11
 } parallelism=1
 EXEC_BOLT_2 {
  PARTITION_6 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EACH_9
  EACH_9 -SingleDispatcher-> PARTITION_12
 } parallelism=1
 EXEC_BOLT_2 {
  PARTITION_7 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> EACH_10
  EACH_10 -SingleDispatcher-> PARTITION_13
 } parallelism=1
 EXEC_BOLT_3 {
  PARTITION_11 TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> EMIT_14
 } parallelism=4
 EXEC_BOLT_3 {
  PARTITION_12 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> EMIT_15
 } parallelism=4
 EXEC_BOLT_3 {
  PARTITION_13 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> EMIT_16
 } parallelism=4
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_2
  -PARTITION_6-> EXEC_BOLT_2
  -PARTITION_7-> EXEC_BOLT_2
 EXEC_BOLT_2
  -PARTITION_11-> EXEC_BOLT_3
  -PARTITION_12-> EXEC_BOLT_3
  -PARTITION_13-> EXEC_BOLT_3
 EXEC_BOLT_3');
