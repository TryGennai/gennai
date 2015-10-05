CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);
CREATE TUPLE tuple2 (eee STRING, fff INT, ggg INT, hhh STRING);
CREATE TUPLE tuple3 (iii STRING, jjj INT, kkk INT, lll STRING);

SET default.parallelism = 3;

FROM
 (tuple1
  JOIN tuple2 ON tuple1.aaa = tuple2.eee parallelism 2
  TO tuple1.*, tuple2.*
  EXPIRE 1min
 ) AS t1,
 tuple3 AS t2
 USING kafka_spout() INTO s1;

FROM s1(t1, t2)
FILTER GROUP EXPIRE 10sec t1.aaa = "aaa", t2.iii = "bbb"
EMIT * USING mongo_persist('db1', 'collection1');

FROM s1(t1) AS t3, s1(t2)
FILTER GROUP EXPIRE 10sec t3.aaa = "aaa", t2.iii != "bbb"
EMIT * USING mongo_persist('db1', 'collection2');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING), tuple2(eee STRING, fff INT, ggg INT, hhh STRING), tuple3(iii STRING, jjj INT, kkk INT, lll STRING)]) parallelism=3
  -S(tuple3)-> PARTITION_1
  -S(tuple1, tuple2)-> PARTITION_2
 PARTITION_1(shuffle grouping)
  -S(tuple3)-> MERGE_3
 PARTITION_2(join key grouping(tuple1(tuple1:aaa) JOIN tuple2(tuple2:eee)))
  -S(tuple1, tuple2)-> TUPLE_JOIN_4
 MERGE_3() parallelism=3
  -S(t1, tuple3)-> FILTER_GROUP_5
  -S(t1)-> RENAME_6
  -S(tuple3)-> MERGE_7
 TUPLE_JOIN_4((tuple1(tuple1:aaa) JOIN tuple2(tuple2:eee)), memory_cache(), 1MINUTES, t1, [tuple1:*, tuple2:*]) parallelism=2
  -S(t1)-> MERGE_3
 FILTER_GROUP_5(10SECONDS, [t1:aaa = aaa, tuple3:iii = bbb]) parallelism=3
  -S(t1, tuple3)-> EMIT_8
 RENAME_6(t1, t3) parallelism=3
  -S(t3)-> MERGE_7
 MERGE_7() parallelism=3
  -S(t3, tuple3)-> FILTER_GROUP_9
 EMIT_8(mongo_persist(db1, collection1), [*]) parallelism=3
 FILTER_GROUP_9(10SECONDS, [t3:aaa = aaa, tuple3:iii <> bbb]) parallelism=3
  -S(t3, tuple3)-> EMIT_10
 EMIT_10(mongo_persist(db1, collection2), [*]) parallelism=3
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1, PARTITION_2
 PARTITION_1
  incoming: SPOUT_0
  outgoing: MERGE_3
 PARTITION_2
  incoming: SPOUT_0
  outgoing: TUPLE_JOIN_4
 MERGE_3
  incoming: PARTITION_1, TUPLE_JOIN_4
  outgoing: FILTER_GROUP_5, RENAME_6, MERGE_7
 TUPLE_JOIN_4
  incoming: PARTITION_2
  outgoing: MERGE_3
 FILTER_GROUP_5
  incoming: MERGE_3
  outgoing: EMIT_8
 RENAME_6
  incoming: MERGE_3
  outgoing: MERGE_7
 MERGE_7
  incoming: RENAME_6, MERGE_3
  outgoing: FILTER_GROUP_9
 EMIT_8
  incoming: FILTER_GROUP_5
  outgoing: -
 FILTER_GROUP_9
  incoming: MERGE_7
  outgoing: EMIT_10
 EMIT_10
  incoming: FILTER_GROUP_9
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 PARTITION_1 {tuple3=[iii, jjj, kkk, lll]}
 PARTITION_2 {tuple1=[aaa, bbb, ccc, ddd], tuple2=[eee, fff, ggg, hhh]}
 TUPLE_JOIN_4 {t1=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
 MERGE_3 {tuple3=[iii, jjj, kkk, lll], t1=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
 FILTER_GROUP_5 {t1=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 RENAME_6 {t3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh]}
 EMIT_8 {t1=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 MERGE_7 {t3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 FILTER_GROUP_9 {t3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
 EMIT_10 {t3=[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh], tuple3=[iii, jjj, kkk, lll]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -MultiDispatcher[TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> PARTITION_1, TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> PARTITION_2]
 } parallelism=3
 EXEC_BOLT_1 {
  PARTITION_1 TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> MERGE_3
  MERGE_3 -MultiDispatcher[-SingleDispatcher-> FILTER_GROUP_5, TupleNameFilter(tupleName=[t1])-SingleDispatcher-> RENAME_6, TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> MERGE_7]
  FILTER_GROUP_5 -SingleDispatcher-> EMIT_8
  RENAME_6 TupleNameFilter(tupleName=[t3])-SingleDispatcher-> MERGE_7
  MERGE_7 -SingleDispatcher-> FILTER_GROUP_9
  FILTER_GROUP_9 -SingleDispatcher-> EMIT_10
 } parallelism=3
 EXEC_BOLT_2 {
  PARTITION_2 TupleNameFilter(tupleName=[tuple1, tuple2])-SingleDispatcher-> TUPLE_JOIN_4
  TUPLE_JOIN_4 TupleNameFilter(tupleName=[t1])-SingleDispatcher-> MERGE_3
  MERGE_3 -MultiDispatcher[-SingleDispatcher-> FILTER_GROUP_5, TupleNameFilter(tupleName=[t1])-SingleDispatcher-> RENAME_6, TupleNameFilter(tupleName=[tuple3])-SingleDispatcher-> MERGE_7]
  FILTER_GROUP_5 -SingleDispatcher-> EMIT_8
  RENAME_6 TupleNameFilter(tupleName=[t3])-SingleDispatcher-> MERGE_7
  MERGE_7 -SingleDispatcher-> FILTER_GROUP_9
  FILTER_GROUP_9 -SingleDispatcher-> EMIT_10
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
  -PARTITION_2-> EXEC_BOLT_2
 EXEC_BOLT_1
 EXEC_BOLT_2');
