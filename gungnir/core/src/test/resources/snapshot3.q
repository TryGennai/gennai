CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING, _time) PARTITIONED BY aaa, bbb;

FROM tuple1 USING kafka_spout()
BEGIN GROUP BY aaa
SNAPSHOT EVERY 1min aaa, sum(bbb + ccc) AS s, count(ddd) AS c, sum(DISTINCT bbb) AS s2, count(DISTINCT ddd) AS c2, _context.task_index AS k parallelism 3
EMIT * USING web_emit('http://localhost:3000/update') parallelism 2;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING, _time) partitioned by aaa, bbb]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(fields grouping(aaa))
  -GS[aaa]-> SNAPSHOT_2
 SNAPSHOT_2(interval(1MINUTES), [aaa, sum(eval(bbb + ccc)) AS s, count(ddd) AS c, sum(distinct(bbb)) AS s2, count(distinct(ddd)) AS c2, _context.task_index AS k]) parallelism=3
  -GS[aaa]-> EMIT_3
 EMIT_3(web_emit(http://localhost:3000/update), [*]) parallelism=2
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: SNAPSHOT_2
 SNAPSHOT_2
  incoming: PARTITION_1
  outgoing: EMIT_3
 EMIT_3
  incoming: SNAPSHOT_2
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd, _time]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd, _time]}
 SNAPSHOT_2 {tuple1=[aaa, s, c, s2, c2, k]}
 EMIT_3 {tuple1=[aaa, s, c, s2, c2, k]}
Group fields:
 SNAPSHOT_2 aaa
 EMIT_3 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -GroupingDispatcher(aaa)-> SNAPSHOT_2
  SNAPSHOT_2 -GroupingDispatcher(aaa)-> EMIT_3
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY snapshot3;

@SNAPSHOT(3);

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100}');
@POST('tuple1', '{aaa:"aaa1", bbb:3, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"ddd9"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:323, c:2, s2:13, c2:1, k:0}');
@EMIT('EMIT_3', '{aaa:"aaa2", s:330, c:3, s2:10, c2:2, k:1}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:110, c:1, s2:10, c2:1, k:2}');
@EMIT('EMIT_3', '{aaa:"aaa4", s:110, c:1, s2:10, c2:1, k:2}');
@EMIT('EMIT_3', '{aaa:"aaa5", s:110, c:1, s2:10, c2:1, k:0}');
@PLAY(60);

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd2"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:220, c:2, s2:10, c2:1, k:0}');
@EMIT('EMIT_3', '{aaa:"aaa4", s:220, c:2, s2:10, c2:1, k:2}');
@PLAY(60);

STOP TOPOLOGY snapshot3;
