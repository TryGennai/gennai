CREATE TUPLE tuple1 (uid STRING, beacon STRING, _time);

FROM tuple1 USING kafka_spout()
INTO s1;

FROM s1
BEGIN GROUP BY uid
SLIDE LENGTH 1days BY _time uid, _time, collect_list(beacon) AS route
END GROUP
INTO s2;

FROM s2
FILTER size(route) >= 2
BEGIN GROUP BY route
SLIDE LENGTH 10days BY _time route, count() AS c
EACH slice(route, -5, -1) AS sp, route[size(route) - 1] AS ep, c
EMIT sp, ep, c USING mongo_persist('db1', 'collection1')
END GROUP;

FROM s2
JOIN mode_beacon USING mongo_fetch('db1', 'collection1', '{sp:@route}', ['ep'], '{c:-1}', 1, 1min)
EMIT uid, mode_beacon USING kafka_emit('topic1');

FROM s1
EMIT uid USING mongo_persist('db1', 'collection2');

EXPLAIN EXTENDED;

SUBMIT TOPOLOGY test;

@POST('tuple1', '{uid:"uid1", beacon:"A"}');
@POST('tuple1', '{uid:"uid2", beacon:"A"}');
@FETCH('JOIN_8', '["route"]', '{"[A]":[["X"]]}');
@EMIT('EMIT_4', '{uid:"uid1"}');
@EMIT('EMIT_4', '{uid:"uid2"}');
@EMIT('EMIT_10', '{uid:"uid1", mode_beacon:"X"}');
@EMIT('EMIT_10', '{uid:"uid2", mode_beacon:"X"}');
@PLAY(60);

@POST('tuple1', '{uid:"uid1", beacon:"B"}');
@FETCH('JOIN_8', '["route"]', '{"[A, B]":[["Y"]]}');
@EMIT('EMIT_4', '{uid:"uid1"}');
@EMIT('EMIT_10', '{uid:"uid1", mode_beacon:"Y"}');
@EMIT('EMIT_12', '{sp:["A"], ep:"B", c:1}');
@PLAY(60);

@POST('tuple1', '{uid:"uid1", beacon:"C"}');
@POST('tuple1', '{uid:"uid2", beacon:"B"}');
@FETCH('JOIN_8', '["route"]', '{"[A, B]":[["Y"]], "[A, B, C]":[["Z"]]}');
@EMIT('EMIT_4', '{uid:"uid1"}');
@EMIT('EMIT_4', '{uid:"uid2"}');
@EMIT('EMIT_10', '{uid:"uid2", mode_beacon:"Y"}');
@EMIT('EMIT_10', '{uid:"uid1", mode_beacon:"Z"}');
@EMIT('EMIT_12', '{sp:["A", "B"], ep:"C", c:1}');
@EMIT('EMIT_12', '{sp:["A"], ep:"B", c:2}');
@PLAY(60);

@POST('tuple1', '{uid:"uid3", beacon:"A"}');
@FETCH('JOIN_8', '["route"]', '{"[A]":[["X"]]}');
@EMIT('EMIT_4', '{uid:"uid3"}');
@EMIT('EMIT_10', '{uid:"uid3", mode_beacon:"X"}');
@PLAY(60);

@POST('tuple1', '{uid:"uid3", beacon:"B"}');
@FETCH('JOIN_8', '["route"]', '{"[A, B]":[["Y"]]}');
@EMIT('EMIT_4', '{uid:"uid3"}');
@EMIT('EMIT_10', '{uid:"uid3", mode_beacon:"Y"}');
@EMIT('EMIT_12', '{sp:["A"], ep:"B", c:3}');
@PLAY(60);

@POST('tuple1', '{uid:"uid3", beacon:"D"}');
@FETCH('JOIN_8', '["route"]', '{"[A, B, D]":[["O"]]}');
@EMIT('EMIT_4', '{uid:"uid3"}');
@EMIT('EMIT_10', '{uid:"uid3", mode_beacon:"O"}');
@EMIT('EMIT_12', '{sp:["A", "B"], ep:"D", c:1}');
@PLAY(60);

@POST('tuple1', '{uid:"uid2", beacon:"D"}');
@FETCH('JOIN_8', '["route"]', '{"[A, B, D]":[["O"]]}');
@EMIT('EMIT_4', '{uid:"uid2"}');
@EMIT('EMIT_10', '{uid:"uid2", mode_beacon:"O"}');
@EMIT('EMIT_12', '{sp:["A", "B"], ep:"D", c:2}');
@PLAY(60);

STOP TOPOLOGY test;
