CREATE TUPLE tuple1 (aaa BIGINT, bbb STRUCT<b1 STRING, b2 INT, b3 LIST<INT>, b4 STRING>, ccc STRING, ddd MAP<DOUBLE, BIGINT>);

FROM tuple1 USING kafka_spout()
INTO s1;

FROM s1
EMIT * USING web_emit('http://localhost:3000/update');

FROM s1(tuple1) AS tuple2
FILTER bbb.b3[0] >= 10
JOIN books_id, books_price USING web_fetch('http://localhost:3000/solr/select?q=books.title:@ccc+AND+books.author:@bbb.b1&fl=id,price&wt=json', 'response.docs', ['id', 'price'], 1min)
EACH aaa, books_id, books_price, split(parse_url(bbb.b4, 'PATH'), '/') AS path, ccc
EMIT * USING mongo_persist('db1', 'collection1');

EXPLAIN EXTENDED;

@IS('Explain:
 SPOUT_0(kafka_spout(), [tuple1(aaa BIGINT, bbb STRUCT<b1 STRING, b2 INT, b3 LIST<INT>, b4 STRING>, ccc STRING, ddd MAP<DOUBLE,BIGINT>)]) parallelism=1
  -S-> PARTITION_1
 PARTITION_1(shuffle grouping)
  -S-> EMIT_2
  -S(tuple1)-> RENAME_3
 EMIT_2(web_emit(http://localhost:3000/update), [*]) parallelism=1
 RENAME_3(tuple1, tuple2) parallelism=1
  -S(tuple2)-> FILTER_4
 FILTER_4(bbb.b3[0] >= 10) parallelism=1
  -S(tuple2)-> PARTITION_5
 PARTITION_5(fields grouping(ccc, bbb.b1))
  -S(tuple2)-> JOIN_6
 JOIN_6(web_fetch(http://localhost:3000/solr/select?q=books.title:@ccc+AND+books.author:@bbb.b1&fl=id,price&wt=json, response.docs, [id, price]), [books_id, books_price]) parallelism=1
  -S(tuple2)-> EACH_7
 EACH_7([aaa, books_id, books_price, split(parse_url(bbb.b4, PATH), /) AS path, ccc]) parallelism=1
  -S(tuple2)-> EMIT_8
 EMIT_8(mongo_persist(db1, collection1), [*]) parallelism=1
Stream edges:
 SPOUT_0
  incoming: -
  outgoing: PARTITION_1
 PARTITION_1
  incoming: SPOUT_0
  outgoing: EMIT_2, RENAME_3
 EMIT_2
  incoming: PARTITION_1
  outgoing: -
 RENAME_3
  incoming: PARTITION_1
  outgoing: FILTER_4
 FILTER_4
  incoming: RENAME_3
  outgoing: PARTITION_5
 PARTITION_5
  incoming: FILTER_4
  outgoing: JOIN_6
 JOIN_6
  incoming: PARTITION_5
  outgoing: EACH_7
 EACH_7
  incoming: JOIN_6
  outgoing: EMIT_8
 EMIT_8
  incoming: EACH_7
  outgoing: -
Output fields:
 SPOUT_0 {tuple1=[aaa, bbb, ccc, ddd]}
 PARTITION_1 {tuple1=[aaa, bbb, ccc, ddd]}
 EMIT_2 {tuple1=[aaa, bbb, ccc, ddd]}
 RENAME_3 {tuple2=[aaa, bbb, ccc, ddd]}
 FILTER_4 {tuple2=[aaa, bbb, ccc, ddd]}
 PARTITION_5 {tuple2=[aaa, bbb, ccc, ddd]}
 JOIN_6 {tuple2=[aaa, bbb, ccc, ddd, books_id, books_price]}
 EACH_7 {tuple2=[aaa, books_id, books_price, path, ccc]}
 EMIT_8 {tuple2=[aaa, books_id, books_price, path, ccc]}
Group fields:
Components:
 EXEC_SPOUT {
  SPOUT_0 -SingleDispatcher-> PARTITION_1
 } parallelism=1
 EXEC_BOLT_1 {
  PARTITION_1 -MultiDispatcher[-SingleDispatcher-> EMIT_2, TupleNameFilter(tupleName=[tuple1])-SingleDispatcher-> RENAME_3]
  RENAME_3 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> FILTER_4
  FILTER_4 -SingleDispatcher-> PARTITION_5
 } parallelism=1
 EXEC_BOLT_2 {
  PARTITION_5 TupleNameFilter(tupleName=[tuple2])-SingleDispatcher-> JOIN_6
  JOIN_6 -SingleDispatcher-> EACH_7
  EACH_7 -SingleDispatcher-> EMIT_8
 } parallelism=1
Topology:
 EXEC_SPOUT
  -PARTITION_1-> EXEC_BOLT_1
 EXEC_BOLT_1
  -PARTITION_5-> EXEC_BOLT_2
 EXEC_BOLT_2');

SUBMIT TOPOLOGY join;

@POST('tuple1', '{aaa:1234567890, bbb:{b1:"author1", b2:999, b3:[11, 22, 33], b4:"https://www.gennai.org/blog/13?debug=true"}, ccc:"title1", ddd:{"12.3":123, "45.6":456}}');
@POST('tuple1', '{aaa:1234567890, bbb:{b1:"author2", b2:999, b3:[11, 22, 33], b4:"https://www.gennai.org/blog/14?debug=true"}, ccc:"title2", ddd:{"12.3":123, "45.6":456}}');
@POST('tuple1', '{aaa:1234567890, bbb:{b1:"author3", b2:999, b3:[6, 22, 33], b4:"https://www.gennai.org/blog/15?debug=true"}, ccc:"title3", ddd:{"12.3":123, "45.6":456}}');
@FETCH('JOIN_6', '["ccc", "bbb.b1"]', '{"title1+author1":[["978-1234567890", 1234]], "title2+author2":[["978-1234567891", 5678], ["654-1234567891", 5700]]}');
@EMIT('EMIT_2', '{aaa:1234567890, bbb:{b1:"author1", b2:999, b3:[11, 22, 33], b4:"https://www.gennai.org/blog/13?debug=true"}, ccc:"title1", ddd:{"12.3":123, "45.6":456}}');
@EMIT('EMIT_2', '{aaa:1234567890, bbb:{b1:"author2", b2:999, b3:[11, 22, 33], b4:"https://www.gennai.org/blog/14?debug=true"}, ccc:"title2", ddd:{"12.3":123, "45.6":456}}');
@EMIT('EMIT_2', '{aaa:1234567890, bbb:{b1:"author3", b2:999, b3:[6, 22, 33], b4:"https://www.gennai.org/blog/15?debug=true"}, ccc:"title3", ddd:{"12.3":123, "45.6":456}}');
@EMIT('EMIT_8', '{aaa:1234567890, books_id:"978-1234567890", books_price:1234, path:["", "blog", "13"], ccc:"title1"}');
@EMIT('EMIT_8', '{aaa:1234567890, books_id:"978-1234567891", books_price:5678, path:["", "blog", "14"], ccc:"title2"}');
@EMIT('EMIT_8', '{aaa:1234567890, books_id:"654-1234567891", books_price:5700, path:["", "blog", "14"], ccc:"title2"}');
@PLAY(60);

STOP TOPOLOGY join;
