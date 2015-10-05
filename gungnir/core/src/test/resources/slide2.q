CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING, _time);

set default.parallelism = 2;

FROM tuple1 USING kafka_spout()
BEGIN GROUP BY aaa
SLIDE LENGTH 30sec BY _time aaa, sum(bbb) AS s, count() AS c, avg(bbb) AS a
EMIT * USING web_emit('http://localhost:3000/update');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING, _time)]) parallelism=2
  -S-> PARTITION_1
 PARTITION_1(fields grouping(aaa))
  -GS[aaa]-> SLIDE_2
 SLIDE_2(length(30SECONDS BY _time), [aaa, sum(bbb) AS s, count() AS c, avg(bbb) AS a]) parallelism=2
  -GS[aaa]-> EMIT_3
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
 SLIDE_2 {tuple1=[aaa, s, c, a]}
 EMIT_3 {tuple1=[aaa, s, c, a]}
Group fields:
 SLIDE_2 aaa
 EMIT_3 aaa
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=2
 EXEC_BOLT_1 {
  PARTITION_1 -GroupingDispatcher(aaa)-> SLIDE_2
  SLIDE_2 -GroupingDispatcher(aaa)-> EMIT_3
 } parallelism=2
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY slide2;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd2"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd3"}');
@POST('tuple1', '{aaa:"aaa2", bbb:10, ccc:100, ddd:"ddd4"}');
@POST('tuple1', '{aaa:"aaa3", bbb:10, ccc:100, ddd:"ddd5"}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:100, ddd:"ddd6"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:10, c:1, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:20, c:2, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa2", s:10, c:1, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa2", s:20, c:2, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:10, c:1, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:20, c:2, a:10}');
@PLAY(60);

@POST('tuple1', '{aaa:"aaa1", bbb:50, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa3", bbb:60, ccc:100, ddd:"ddd3"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:70, c:3, a:23}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:80, c:3, a:26}');
@PLAY(60);

@TIMER(+15);

@POST('tuple1', '{aaa:"aaa1", bbb:60, ccc:100, ddd:"ddd7"}');
@POST('tuple1', '{aaa:"aaa3", bbb:20, ccc:100, ddd:"ddd3"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:130, c:4, a:32}');
@EMIT('EMIT_3', '{aaa:"aaa3", s:100, c:4, a:25}');
@PLAY(60);

@TIMER(+30);

@POST('tuple1', '{aaa:"aaa1", bbb:20, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa10", bbb:10, ccc:100, ddd:"ddd10"}');
@POST('tuple1', '{aaa:"aaa11", bbb:10, ccc:100, ddd:"ddd11"}');
@POST('tuple1', '{aaa:"aaa1", bbb:20, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa1", bbb:20, ccc:100, ddd:"ddd1"}');
@POST('tuple1', '{aaa:"aaa10", bbb:10, ccc:100, ddd:"ddd14"}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:80, c:2, a:40}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:100, c:3, a:33}');
@EMIT('EMIT_3', '{aaa:"aaa1", s:120, c:4, a:30}');
@EMIT('EMIT_3', '{aaa:"aaa10", s:10, c:1, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa10", s:20, c:2, a:10}');
@EMIT('EMIT_3', '{aaa:"aaa11", s:10, c:1, a:10}');
@PLAY(60);

STOP TOPOLOGY slide2;
