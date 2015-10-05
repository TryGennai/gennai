CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING, eee LIST<STRING>, _time);

FROM tuple1 USING kafka_spout()
INTO s1;

FROM s1
FILTER ifnull(ccc, 0) <= 100 AND bbb IN (10, 20) AND ddd IS NOT NULL AND eee ALL ("456", "123") parallelism 3
EACH aaa, bbb, ccc, ddd
EMIT * USING web_emit('http://localhost:3000/update1') parallelism 2;

FROM s1
FILTER aaa != 'aaa1' AND bbb != 0 parallelism 3
EACH aaa, bbb, ccc, ddd
EMIT * USING web_emit('http://localhost:3000/update2') parallelism 2;

EXPLAIN EXTENDED;

SUBMIT TOPOLOGY filter;

@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:null, ddd:"ddd1", eee:["123", "456", "789"]}');
@POST('tuple1', '{aaa:"aaa1", bbb:10, ccc:1000, ddd:"ddd1", eee:["123", "456", "789"]}');
@POST('tuple1', '{aaa:"aaa1", bbb:30, ccc:80, ddd:"ddd1", eee:["123", "456", "789"]}');
@POST('tuple1', '{aaa:"aaa2", bbb:80, ccc:100, ddd:"ddd1", eee:["123", "444", "789"]}');
@POST('tuple1', '{aaa:"aaa3", bbb:20, ccc:100, ddd:"ddd1", eee:["123", "444", "789"]}');
@POST('tuple1', '{aaa:"aaa4", bbb:0, ccc:100, ddd:"ddd1", eee:["123", "444", "789"]}');
@EMIT('EMIT_8', '{aaa:"aaa1", bbb:10, ccc:null, ddd:"ddd1"}');
@EMIT('EMIT_9', '{aaa:"aaa2", bbb:80, ccc:100, ddd:"ddd1"}');
@EMIT('EMIT_9', '{aaa:"aaa3", bbb:20, ccc:100, ddd:"ddd1"}');
@PLAY(60);

STOP TOPOLOGY filter;
