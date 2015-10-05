CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING, _time) PARTITIONED BY aaa, bbb;

FROM tuple1 USING kafka_spout()
BEGIN GROUP BY aaa
SNAPSHOT EVERY "0/3 * * * * ? *" aaa, sum(bbb) AS s, count() AS c, _context.task_index AS k EXPIRE "0/6 * * * * ? *" parallelism 3
EMIT * USING web_emit('http://localhost:3000/update') parallelism 2;

EXPLAIN EXTENDED;

SUBMIT TOPOLOGY snapshot4;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd3"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd5"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd6"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{aaa:"aaa5", bbb:10, ccc:100, ddd:"ddd9"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:30, c:3, k:0}');
@EMIT('EMIT_3', '{aaa:"aaa2", s:30, c:3, k:1}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:10, c:1, k:2}');
@EMIT('EMIT_3', '{aaa:"aaa4", s:10, c:1, k:2}');
@EMIT('EMIT_3', '{aaa:"aaa5", s:10, c:1, k:0}');
@PLAY(60);

@SLEEP(10);

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd8"}');
@POST('tuple1', '{aaa:"aaa4", bbb:10, ccc:100, ddd:"ddd8"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:20, c:2, k:0}');
@EMIT('EMIT_3', '{aaa:"aaa4", s:20, c:2, k:2}');
@PLAY(60);

STOP TOPOLOGY snapshot4;
