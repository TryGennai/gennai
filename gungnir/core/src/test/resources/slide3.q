CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING, _time);

set default.parallelism = 2;

FROM tuple1 AS ua USING kafka_spout()
BEGIN GROUP BY aaa
EACH count() AS count_all, *
SLIDE LENGTH 5 sum(bbb) AS sum_bbb, count_all, aaa
EMIT * USING mongo_persist('db1', 'collection1', ['aaa'])
END GROUP;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING, _time)]) parallelism=2
  -S-> PARTITION_1
 PARTITION_1(fields grouping(aaa))
  -GS[aaa]-> EACH_2
 EACH_2([count() AS count_all, *]) parallelism=2
  -GS[aaa]-> SLIDE_3
 SLIDE_3(length(5tuples), [sum(bbb) AS sum_bbb, count_all, aaa]) parallelism=2
  -GS[aaa]-> EMIT_4
 EMIT_4(mongo_persist(db1, collection1, [aaa]), [*]) parallelism=2
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2
 EACH_2
  incoming: PARTITION_1
  outgoing: SLIDE_3
 SLIDE_3
  incoming: EACH_2
  outgoing: EMIT_4
 EMIT_4
  incoming: SLIDE_3
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd, _time]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd, _time]}
 EACH_2 {tuple1=[count_all, aaa, bbb, ccc, ddd, _time]}
 SLIDE_3 {tuple1=[sum_bbb, count_all, aaa]}
 EMIT_4 {tuple1=[sum_bbb, count_all, aaa]}
Group fields:
 EACH_2 aaa
 SLIDE_3 aaa
 EMIT_4 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=2
 EXEC_BOLT_1 {
  PARTITION_1 -GroupingDispatcher(aaa)-> EACH_2
  EACH_2 -GroupingDispatcher(aaa)-> SLIDE_3
  SLIDE_3 -GroupingDispatcher(aaa)-> EMIT_4
 } parallelism=2
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY slide3;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd3"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd5"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd6"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd9"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd10"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd11"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd12"}');
@EMIT('EMIT_4', '{sum_bbb:10, count_all:1, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:20, count_all:2, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:30, count_all:3, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:40, count_all:4, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:50, count_all:5, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:50, count_all:6, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:50, count_all:7, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:50, count_all:8, aaa:"aaa1"}');
@EMIT('EMIT_4', '{sum_bbb:10, count_all:1, aaa:"aaa2"}');
@EMIT('EMIT_4', '{sum_bbb:20, count_all:2, aaa:"aaa2"}');
@EMIT('EMIT_4', '{sum_bbb:10, count_all:1, aaa:"aaa3"}');
@EMIT('EMIT_4', '{sum_bbb:20, count_all:2, aaa:"aaa3"}');
@PLAY(60);

STOP TOPOLOGY slide3;
