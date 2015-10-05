CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

SET default.parallelism = 3;

FROM tuple1 USING kafka_spout()
BEGIN GROUP BY aaa
EACH aaa, sum(bbb) AS s, count() AS c
EMIT * USING mongo_persist('db1', 'collection1', 'aaa');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=3
  -S-> PARTITION_1
 PARTITION_1(fields grouping(aaa))
  -GS[aaa]-> EACH_2
 EACH_2([aaa, sum(bbb) AS s, count() AS c]) parallelism=3
  -GS[aaa]-> EMIT_3
 EMIT_3(mongo_persist(db1, collection1, [aaa]), [*]) parallelism=3
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EACH_2
 EACH_2
  incoming: PARTITION_1
  outgoing: EMIT_3
 EMIT_3
  incoming: EACH_2
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd]}
 EACH_2 {tuple1=[aaa, s, c]}
 EMIT_3 {tuple1=[aaa, s, c]}
Group fields:
 EACH_2 aaa
 EMIT_3 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=3
 EXEC_BOLT_1 {
  PARTITION_1 -GroupingDispatcher(aaa)-> EACH_2
  EACH_2 -GroupingDispatcher(aaa)-> EMIT_3
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY groupby;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd3"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{bbb:10, ccc:100, ddd:"ddd14"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd5"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd6"}');
@POST('tuple1', '{bbb:10, ccc:100, ddd:"ddd14"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{bbb:10, ccc:100, ddd:"ddd14"}');
@POST('tuple1', '{aaa:"aaa9", bbb:10, ccc:100, ddd:"ddd9"}');
@POST('tuple1', '{aaa:"aaa10", bbb:10, ccc:100, ddd:"ddd10"}');
@POST('tuple1', '{aaa:"aaa11", bbb:10, ccc:100, ddd:"ddd11"}');
@POST('tuple1', '{aaa:"aaa9", bbb:10, ccc:100, ddd:"ddd12"}');
@POST('tuple1', '{aaa:"aaa9", bbb:10, ccc:100, ddd:"ddd13"}');
@POST('tuple1', '{aaa:"aaa10", bbb:10, ccc:100, ddd:"ddd14"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:10, c:1}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:20, c:2}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:30, c:3}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:40, c:4}');
@EMIT('EMIT_3', '{aaa:"aaa2", s:10, c:1}');
@EMIT('EMIT_3', '{aaa:"aaa2", s:20, c:2}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:10, c:1}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:20, c:2}');
@EMIT('EMIT_3', '{aaa:"aaa9", s:10, c:1}');
@EMIT('EMIT_3', '{aaa:"aaa9", s:20, c:2}');
@EMIT('EMIT_3', '{aaa:"aaa9", s:30, c:3}');
@EMIT('EMIT_3', '{aaa:"aaa10", s:10, c:1}');
@EMIT('EMIT_3', '{aaa:"aaa10", s:20, c:2}');
@EMIT('EMIT_3', '{aaa:"aaa11", s:10, c:1}');
@EMIT('EMIT_3', '{aaa:null, s:10, c:1}');
@EMIT('EMIT_3', '{aaa:null, s:20, c:2}');
@EMIT('EMIT_3', '{aaa:null, s:30, c:3}');
@PLAY(60);

STOP TOPOLOGY groupby;
