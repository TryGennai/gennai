CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

FROM tuple1 USING kafka_spout()
EACH bbb + 6 AS a, bbb * (ccc + 123) AS b, sum(ccc) AS c, sum(ccc * 10) * count() AS d, aaa AS e,
 ddd, cast(aaa AS INT), cast(aaa AS INT) AS f, concat(cast(bbb AS STRING), ifnull(ddd, '-')) AS g,
 count(DISTINCT ddd) AS h, count(DISTINCT concat(aaa, ifnull(ddd, '-'))) AS i,
 cast((ccc + 123) DIV 10 AS STRING) AS j
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> EACH_2
 EACH_2([eval(bbb + 6) AS a, eval(bbb * (ccc + 123)) AS b, sum(ccc) AS c, eval(sum(eval(ccc * 10)) * count()) AS d, aaa AS e, ddd, cast(aaa, INT) AS aaa, cast(aaa, INT) AS f, concat(cast(bbb, STRING) AS bbb, ifnull(ddd, -)) AS g, count(distinct(ddd)) AS h, count(distinct(concat(aaa, ifnull(ddd, -)))) AS i, cast(eval((ccc + 123) DIV 10), STRING) AS j]) parallelism=1
  -S-> EMIT_3
 EMIT_3(mongo_persist(db1, collection1), [*]) parallelism=1
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
 EACH_2 {tuple1=[a, b, c, d, e, ddd, aaa, f, g, h, i, j]}
 EMIT_3 {tuple1=[a, b, c, d, e, ddd, aaa, f, g, h, i, j]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -SingleDispatcher-> EACH_2
  EACH_2 -SingleDispatcher-> EMIT_3
 } parallelism=1
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY each;

@POST('tuple1', '{aaa:"999", bbb:10, ccc:100, ddd:"ddd1"}');
@EMIT('EMIT_3', '{a:16, b:2230, c:100, d:1000, e:"999", ddd:"ddd1", aaa:999, f:999, g:"10ddd1", h:1, i:1, j:"22"}');
@PLAY(60);
@POST('tuple1', '{aaa:"888", bbb:10, ccc:100, ddd:"ddd1"}');
@EMIT('EMIT_3', '{a:16, b:2230, c:200, d:4000, e:"888", ddd:"ddd1", aaa:888, f:888, g:"10ddd1", h:1, i:2, j:"22"}');
@PLAY(60);
@POST('tuple1', '{aaa:"777", bbb:10, ccc:100}');
@EMIT('EMIT_3', '{a:16, b:2230, c:300, d:9000, e:"777", ddd:null, aaa:777, f:777, g:"10-", h:1, i:3, j:"22"}');
@PLAY(60);
@POST('tuple1', '{aaa:"666", bbb:3, ccc:30, ddd:"ddd2"}');
@EMIT('EMIT_3', '{a:9, b:459, c:330, d:13200, e:"666", ddd:"ddd2", aaa:666, f:666, g:"3ddd2", h:2, i:4, j:"15"}');
@PLAY(60);
@POST('tuple1', '{aaa:"666", bbb:3, ccc:30, ddd:"ddd2"}');
@EMIT('EMIT_3', '{a:9, b:459, c:360, d:18000, e:"666", ddd:"ddd2", aaa:666, f:666, g:"3ddd2", h:2, i:4, j:"15"}');
@PLAY(60);

STOP TOPOLOGY each;
