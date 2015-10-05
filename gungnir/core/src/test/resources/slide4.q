CREATE TUPLE tuple1 (
 name STRING,
 t1 TINYINT,
 t2 TINYINT,
 s1 SMALLINT,
 s2 SMALLINT,
 i1 INT,
 i2 INT,
 b1 BIGINT,
 b2 BIGINT,
 f1 FLOAT,
 f2 FLOAT,
 d1 DOUBLE,
 d2 DOUBLE
);

FROM tuple1 USING kafka_spout()
SLIDE LENGTH 3 sum(i1 + i2) AS s
EMIT * USING mongo_persist('test', 'output');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(name STRING, t1 TINYINT, t2 TINYINT, s1 SMALLINT, s2 SMALLINT, i1 INT, i2 INT, b1 BIGINT, b2 BIGINT, f1 FLOAT, f2 FLOAT, d1 DOUBLE, d2 DOUBLE)]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> SLIDE_2
 SLIDE_2(length(3tuples), [sum(eval(i1 + i2)) AS s]) parallelism=1
  -S-> EMIT_3
 EMIT_3(mongo_persist(test, output), [*]) parallelism=1
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
 SPOUT_0 {tuple1=[name, t1, t2, s1, s2, i1, i2, b1, b2, f1, f2, d1, d2]}
 PARTITION_1 {tuple1=[name, t1, t2, s1, s2, i1, i2, b1, b2, f1, f2, d1, d2]}
 SLIDE_2 {tuple1=[s]}
 EMIT_3 {tuple1=[s]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -SingleDispatcher-> SLIDE_2
  SLIDE_2 -SingleDispatcher-> EMIT_3
 } parallelism=1
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1');

SUBMIT TOPOLOGY slide4;

@POST('tuple1', '{t1:3, t2:4, s1:5, s2:6, i1:7, i2:8, b1:9, b2:10, f1:11.2, f2:12.3, d1:13.4, d2:14.5}');
@POST('tuple1', '{t1:3, t2:4, s1:5, s2:6, i1:7, i2:8, b1:9, b2:10, f1:11.2, f2:12.3, d1:13.4, d2:14.5}');
@POST('tuple1', '{t1:3, t2:4, s1:5, s2:6, i1:7, i2:8, b1:9, b2:10, f1:11.2, f2:12.3, d1:13.4, d2:14.5}');
@POST('tuple1', '{t1:3, t2:4, s1:5, s2:6, i1:7, i2:8, b1:9, b2:10, f1:11.2, f2:12.3, d1:13.4, d2:14.5}');
@EMIT('EMIT_3', '{s:15}');
@EMIT('EMIT_3', '{s:30}');
@EMIT('EMIT_3', '{s:45}');
@EMIT('EMIT_3', '{s:45}');
@PLAY(60);

STOP TOPOLOGY slide4;
