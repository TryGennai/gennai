CREATE TUPLE tuple1 (aaa STRING, bbb INT, ccc INT, ddd STRING);

FROM tuple1 USING kafka_spout()
EACH bbb + ccc AS a, bbb * ccc AS b, bbb / ccc AS c, bbb % ccc AS d, bbb DIV ccc AS e, bbb MOD ccc AS f, count(ddd) AS g , sum(bbb) AS h
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa STRING, bbb INT, ccc INT, ddd STRING)]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> EACH_2
 EACH_2([eval(bbb + ccc) AS a, eval(bbb * ccc) AS b, eval(bbb / ccc) AS c, eval(bbb % ccc) AS d, eval(bbb DIV ccc) AS e, eval(bbb % ccc) AS f, count(ddd) AS g, sum(bbb) AS h]) parallelism=1
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
 EACH_2 {tuple1=[a, b, c, d, e, f, g, h]}
 EMIT_3 {tuple1=[a, b, c, d, e, f, g, h]}
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

SUBMIT TOPOLOGY each2;

@POST('tuple1', '{aaa:"aaa1", bbb:2147483647, ccc:3, ddd:"ddd1"}');
@EMIT('EMIT_3', '{a:2147483650, b:6442450941, c:7.158278823333334E8, d:1, e:715827882, f:1, g:1, h:2147483647}');
@PLAY(60);
@POST('tuple1', '{aaa:"aaa1", bbb:9, ccc:3}');
@EMIT('EMIT_3', '{a:12, b:27, c:3.0, d:0, e:3, f:0, g:1, h:2147483656}');
@PLAY(60);
@POST('tuple1', '{aaa:"aaa1", bbb:9, ccc:3, ddd:"ddd1"}');
@EMIT('EMIT_3', '{a:12, b:27, c:3.0, d:0, e:3, f:0, g:2, h:2147483665}');
@PLAY(60);
@POST('tuple1', '{aaa:"aaa1", ccc:3, ddd:"ddd2"}');
@EMIT('EMIT_3', '{a:3, b:0, c:0.0, d:0, e:0, f:0, g:3, h:2147483665}');
@PLAY(60);
@POST('tuple1', '{aaa:"aaa1", bbb:9, ddd:"ddd1"}');
@EMIT('EMIT_3', '{a:9, b:0, c:null, d:null, e:null, f:null, g:4, h:2147483674}');
@PLAY(60);

STOP TOPOLOGY each2;
