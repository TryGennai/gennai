CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING, _time) PARTITIONED BY aaa, bbb;

FROM tuple1 USING kafka_spout()
SLIDE LENGTH 30sec BY _time sum(bbb) AS s, count() AS c, avg(bbb) AS a, _context.task_index AS k parallelism 3
EMIT * USING web_emit('http://localhost:3000/update') parallelism 2;

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING, _time) partitioned by aaa, bbb]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(fields grouping(aaa, bbb))
  -S-> SLIDE_2
 SLIDE_2(length(30SECONDS BY _time), [sum(bbb) AS s, count() AS c, avg(bbb) AS a, _context.task_index AS k]) parallelism=3
  -S-> EMIT_3
 EMIT_3(web_emit(http://localhost:3000/update), [*]) parallelism=2
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: SLIDE_2
 SLIDE_2
  incoming: PARTITION_1
  outgoing: EMIT_3
 EMIT_3
  incoming: SLIDE_2
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd, _time]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd, _time]}
 SLIDE_2 {tuple1=[s, c, a, k]}
 EMIT_3 {tuple1=[s, c, a, k]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -SingleDispatcher-> SLIDE_2
  SLIDE_2 -SingleDispatcher-> EMIT_3
 } parallelism=3
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY slide;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd5"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"ddd9"}');
@EMIT('EMIT_3', '{s:10, c:1, a:10, k:0}');
@EMIT('EMIT_3', '{s:10, c:1, a:10, k:1}');
@EMIT('EMIT_3', '{s:20, c:2, a:10, k:1}');
@EMIT('EMIT_3', '{s:30, c:3, a:10, k:1}');
@EMIT('EMIT_3', '{s:10, c:1, a:10, k:2}');
@EMIT('EMIT_3', '{s:20, c:2, a:10, k:2}');
@EMIT('EMIT_3', '{s:30, c:3, a:10, k:2}');
@PLAY(60);

@TIMER(+15);

@POST('tuple1', '{aaa:"aaa1", bbb:50, ccc:100, ddd:"ddd3"}');
@POST('tuple1', '{aaa:"aaa2", bbb:30, ccc:100, ddd:"ddd6"}');
@EMIT('EMIT_3', '{s:80, c:4, a:20, k:1}');
@EMIT('EMIT_3', '{s:60, c:4, a:15, k:2}');
@PLAY(60);

@TIMER(+30);

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd5"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"ddd9"}');
@EMIT('EMIT_3', '{s:10, c:1, a:10, k:0}');
@EMIT('EMIT_3', '{s:60, c:2, a:30, k:1}');
@EMIT('EMIT_3', '{s:70, c:3, a:23, k:1}');
@EMIT('EMIT_3', '{s:80, c:4, a:20, k:1}');
@EMIT('EMIT_3', '{s:40, c:2, a:20, k:2}');
@EMIT('EMIT_3', '{s:50, c:3, a:16, k:2}');
@EMIT('EMIT_3', '{s:60, c:4, a:15, k:2}');
@PLAY(60);

STOP TOPOLOGY slide;
