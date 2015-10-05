![画像](http://pages.genn.ai/img/gennai.png "Image")
[![wercker status](https://app.wercker.com/status/b1066a3c6ecfbf4df18c706e60fbed08/s "wercker status")](https://app.wercker.com/project/bykey/b1066a3c6ecfbf4df18c706e60fbed08)

## Overview

genn.ai(源内)は、ストリーム処理を簡単に利用できるようにするフレームワークです。
[Hive](https://hive.apache.org/)が、[Hadoop](http://hadoop.apache.org/)を使ったデータ処理をより手軽にしているように、[Apache Storm](https://storm.apache.org/)を使ったストリーム処理を手軽に、特別なプログラミングを行うことなく試し、本番での利用へ、さらに必要に応じてスケールしてゆける仕組みです。

溜めたデータを処理するバッチ型でのデータ処理とは異なり、現在流れているデータを今まさに手に汲み取るようにして確認、理解、分析、他のシステムとの連携、が行える仕組みとなることを目指しています。

## Structure

[Apache Storm](https://storm.apache.org/)は、ストリームとしてデータを吸い込む部分からプログラミングが必要ですが、genn.aiでは設定後、すぐにREST(JSON)の形で受け取れるようRESTサーバの機能を提供します。
そして、そこで受け取ったデータを(Storm上のトポロジとして)どう処理するかは簡単な独自のクエリ言語で記述することが可能です。

RESTサーバで受け取るデータの形や、そこに対する処理を定義するクエリなどを設定するために、多くのデータベースと同様のコマンドラインツール(gungnir)を準備しています。
このツールを用いることで、それら設定を(Storm上に)有効化する、取り外す、といった必要な操作一式も簡単に行うことが可能です。

## Documentation

[ドキュメントサイト](http://pages.genn.ai/index_ja.html)にて、同コマンドラインツール(gungnir)の使い方や、クエリの書き方などをご確認頂くことが可能です。
現在、リクルート社内での利用に伴い改訂がかかっているため情報が追いついていない可能性があります。
随時更新していきますが、ずれがある場合はご容赦下さい。
(また、同時に[ご連絡](http://pages.genn.ai/disqus.html)頂けると幸いです)

## Getting started

genn.aiは、下記2つの方法でご利用頂くことが可能です。

### 1. [ソースコードからインストール](gungnir)

ご自身の環境に、ソースコードからインストールする方法を記載しています。1台のホストにインストールする手順を記載していますが、下記の設定例を参考にしていただきますと、複数台ホストにおける分散環境において動作するgenn.aiの環境構築を行うことが可能です。

* [MetaStoreをMongoDBに設定](http://pages.genn.ai/example_ja.html#mongodbmetastore)
* [TupleStoreServerをKafkaに設定](http://pages.genn.ai/example_ja.html#kafkaemitter)
* [GungnirServerを複数ホストで冗長化](http://pages.genn.ai/example_ja.html#distributedgs1)
* [TupleStoreServerを複数ホストで冗長化](http://pages.genn.ai/example_ja.html#distributedts1)

genn.aiを完全分散環境で利用するには、[Apache Storm](http://storm.apache.org/)、[Apache Kafka](http://kafka.apache.org/)、[Apache ZooKeeper](http://zookeeper.apache.org/)、[MongoDB](http://www.mongodb.org/)が必要となります。

### 2. [Vagrantを利用](https://github.com/trygennai/gennai.vagrant)

Vagrantを利用して、容易にgenn.aiをお試しいただくことが可能です。Vagrantにおいても、下記設定を行うことで各モードを利用することが可能です。

* [genn.ai疑似分散モードを設定](https://github.com/TryGennai/gennai.vagrant/blob/master/CONFIG.md#pseudo)
* [1台でgenn.ai分散モードを設定](https://github.com/TryGennai/gennai.vagrant/blob/master/CONFIG.md#distributed)
* [1台のホストマシン上にVagrantで複数台起動して分散](https://github.com/TryGennai/gennai.vagrant/blob/master/CONFIG.md#multiservers)

### Mode

genn.aiには、完全分散環境での使用を含めた計3つのモードがあります。ご使用の環境に合わせて、各モードを選択してください。

* standalone
* local
* distributed

各モードについては、[ドキュメントサイト](http://pages.genn.ai/config_ja.html#mode)に記載しています。

##<a name="sample"></a>Simple example

ここでは、極力シンプルな例を用い、genn.aiを用いたストリーム処理の全容を見てゆきます。
genn.aiは外部からRESTにてデータを受け止め、Stormのトポロジでそれを処理してゆきます。

このため、全体としては下記のような流れになります。

- 受け取るデータを定義(スキーマの設定)
- そのデータをどう処理するかを定義(トポロジの設定と有効化)
- テストデータの投入

では始めましょう。

ここでは、[公開しているVagrant環境](https://github.com/trygennai/gennai.vagrant)を用います。
genn.aiはユーザ管理機能を提供していますが、このVagrant環境では事前に作成されています。
このため、VM起動後は即、(genn.aiのコマンドラインツールである)gungnirコマンドを実行する事が可能です。

```
$ /opt/gungnir-client/bin/gungnir -u gennai -p gennai
Gungnir server connecting ...
Gungnir version 0.0.1 build at 20141203-002625
Welcome gennai (Account ID: 548dbd9d0cf26eec29240527)
gungnir>
```

ここから、通常必要となる作業(先に上げたスキーマの設定、トポロジの入力と投入、テストデータの投入)を順に見てゆきます。

### スキーマの設定

サンプルは一式がホームのsample/simpleディレクトリに格納されています。
まず、この内にある"tuple.q"ファイルを参考に、

```
[vagrant@internal-vagrant simple]$ cat tuple.q
CREATE TUPLE simple (
    Id INT,
    Content STRING
);
[vagrant@internal-vagrant simple]$
```

genn.aiに待ち受けさせるスキーマ(simple)を作成します。

```
gungnir> CREATE TUPLE simple (
      ->     Id INT,
      ->     Content STRING
      -> );
OK
gungnir>
```

この定義が、genn.aiが受け取るストリームデータのJSON書式となり、(gennn.aiが準備する)RESTサーバがこの情報を利用します。



### トポロジの設定と有効化

次に、同sample/simple内にある"query.q"ファイルを参考に、受け取ったストリームデータに対しての処理をgungnirから入力します。
ここに上げた例の処理内容は「simpleスキーマで受けたデータについて、ContentカラムのデータがAから始まる文字列の場合のみMongoDBのtestデータベース中のsimple_outputコレクションに全カラムを出力せよ」というクエリです。
(おおよそ、クエリからお分かり頂けるかと思います)

```
gungnir> FROM simple
      -> USING kafka_spout()
      -> FILTER Content REGEXP '^A[A-Z]*$'
      -> EMIT * USING mongo_persist('test', 'simple_output');
OK
gungnir>
```

では、このクエリをStormに対してトポロジとして有効化(SUBMIT)しましょう。
このときトポロジの名前として **simple_t** を与えています。

```
gungnir> SUBMIT TOPOLOGY simple_t;
OK
Starting ... Done
{"id":"547b01de0cf218509e5b6e0d","name":"simple_t","status":"RUNNING","owner":"gennai","createTime":"2014-11-30T11:39:10.287Z","summary":{"name":"gungnir_547b01de0cf218509e5b6e0d","status":"ACTIVE","uptimeSecs":2,"numWorkers":1,"numExecutors":3,"numTasks":3}}
gungnir>
```

Done以降に返却されているJSONはトポロジ登録時(有効化時)の情報であり、例えは、"id"はトポロジにふられた固有のid、また、"status"は現在のトポロジの状態(ここではRUNNINGなのでもう起動し、処理するデータを待ち受けている状態)であることが分かります。

なお、このトポロジの状態については、DESCコマンドでも調べることができます。

```
gungnir> DESC TOPOLOGY simple_t;
{"id":"548dbd9d0cf26eec29240527","name":"simple_t","status":"STOPPED","owner":"gennai","createTime":"2014-11-30T11:39:10.287Z"}
gungnir>
```

### 動作確認

では、動作を確認するため、早速データをデバッグ投入(POST)してみましょう。
以下では2つのデータを投入しています。

```
gungnir> POST simple {"Id":4,"Content":"ABCDEF"};
POST /gungnir/v0.1/548dbd9d0cf26eec29240527/simple/json
OK
gungnir> POST simple {"Id":4,"Content":"BCDEFA"};
POST /gungnir/v0.1/548dbd9d0cf26eec29240527/simple/json
OK
gungnir>
```

クエリに従い、結果は最初のデータ1つだけがMongoDBに登録されているはずです。
以下のとおり、そのように正しく登録されているかどうかを確認てみましょう。

```
[vagrant@internal-vagrant ~]$ mongo
MongoDB shell version: 2.6.5
connecting to: test
> db.simple_output.find();
{ "_id" : ObjectId("547b02300cf23dc96705ef62"), "Id" : 4, "Content" : "ABCDEF" }
> exit
```

では次に、[curlコマンド](http://curl.haxx.se/docs/)を用いて外部からhttpにてデータ登録を行ってみましょう。

このとき投げ込む先のURLは、先のPOSTコマンド実行時に表示されているURLを用います。
ホスト名はlocalhost、RESTサーバは7200番ポートで待ち受けています。
別の接続を用いて投入します。

```
$ vagrant ssh
Last login: Mon Dec 15 01:28:55 2014 from 10.0.2.2
Welcome to your Vagrant-built virtual machine.
[vagrant@internal-vagrant ~]$
[vagrant@internal-vagrant ~]$ curl -v -H "Content-Type: application/json" -X POST -d '{Id:6,Content:"AZYXWV"}' http://localhost:7200/gungnir/v0.1/548dbd9d0cf26eec29240527/simple/json
* About to connect() to localhost port 7200 (#0)
*   Trying ::1... connected
* Connected to localhost (::1) port 7200 (#0)
> POST /gungnir/v0.1/546f4f480cf2cde01845629f/simple/json HTTP/1.1
> User-Agent: curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.13.6.0 zlib/1.2.3 libidn/1.18 libssh2/1.4.2
> Host: localhost:7200
> Accept: */*
> Content-Type: application/json
> Content-Length: 23
>
< HTTP/1.1 204 No Content
< Content-Length: 0
< Date: Sun, 30 Nov 2014 12:27:54 GMT
<
* Connection #0 to host localhost left intact
* Closing connection #0
[vagrant@internal-vagrant ~]$
```


### 動作確認(負荷がけツール)

では次に、genn.aiに付属しているデータ投入ツールを使ってみましょう。
ツールは、bin/postという名前で格納されています。
このツールは標準入力からデータを受け取ることもできますが、ここではファイルからデータを読み上げる-fオプションを使います。

なお、送信するファイルの中身は以下となっています。

```
[vagrant@internal-vagrant simple]$ cat data.json
{"Id":0, "Content":"ABCDEF"}
{"Id":1, "Content":"BCDEFA"}
{"Id":2, "Content":"CDEFAB"}
[vagrant@internal-vagrant simple]$
```

また、送信時には-aオプションにgenn.aiにおけるユーザIDを指定する必要があります。
これは以下gungnir内でDESCコマンドを用いることで確認が可能です。
(もしくは、gungnirコマンド起動時のwelcomeメッセージでも表示されます)

```
gungnir> DESC USER;
{"id":"546f4f480cf2cde01845629f","name":"gennai","createTime":"2014-11-21T14:42:16.333Z"}
gungnir>
```

では、送信してみましょう。
-tオプションに、投げ込むスキーマ名を指定します。

```
[vagrant@internal-vagrant simple]$ post -a 546f4f480cf2cde01845629f -f data.json -t simple -v
POST http://localhost:7200/gungnir/v0.1/546f4f480cf2cde01845629f/simple/json
HTTP/1.1 204 No Content
Content-Length: 0
[vagrant@internal-vagrant simple]$
```

そして、このツールは-nというオプションを持ち、そこに指定した回数分、ファイルの内容を繰り返し送信させることができます。
(つまりここではdata.jsonに3行のデータが入っているため300件送信されます)

```
[vagrant@internal-vagrant simple]$ post -a 546f4f480cf2cde01845629f -n 100 -v -f data.json -t simple
POST http://localhost:7200/gungnir/v0.1/546f4f480cf2cde01845629f/simple/json
HTTP/1.1 204 No Content
Content-Length: 0
Date: Sun, 30 Nov 2014 12:04:22 GMT
HTTP/1.1 204 No Content
Content-Length: 0
Date: Sun, 30 Nov 2014 12:04:22 GMT
HTTP/1.1 204 No Content
Content-Length: 0
--省略(合計300回レスポンスである204を受け取る)--
[vagrant@internal-vagrant simple]$
```

MongoDBを確認すると、データが増えていることが分かります。

```
[vagrant@internal-vagrant simple]$ mongo
MongoDB shell version: 2.6.6
connecting to: test
> db.simple_output.find();
{ "_id" : ObjectId("548dc8140cf27f4e3ceff38c"), "Id" : 4, "Content" : "ABCDEF" }
{ "_id" : ObjectId("548dc8d30cf27f4e3ceff38d"), "Id" : 6, "Content" : "AZYXWV" }
{ "_id" : ObjectId("548dd1190cf27f4e3ceff38e"), "Id" : 0, "Content" : "ABCDEF" }
>
```

### 別のトポロジを追加する

では、ここで更に別のトポロジを追加＝処理を追加してみましょう。
同様に「simpleスキーマで受けたデータについて、ContentカラムのデータがBから始まっているものを、別のMongoDBコレクションに格納する」処理を作ります。

```
FROM simple
USING kafka_spout()
FILTER Content REGEXP '^B[A-Z]*$'
EMIT * USING mongo_persist('test', 'simple_output_B');
```

そして、同様に登録します。
ここでは、SUBMITコマンドにあるCOMMENT機能を使い、トポロジの機能についてもメモを一緒に書き入れました。
このメモは後からトポロジの機能について確認するための手助けとなります(DESCコマンドにて確認が可能です)。

```
gungnir> SUBMIT TOPOLOGY simple_t_B COMMENT "You can get the data with the content starting letter B.";
OK
Starting ... Done
{"id":"548dd20d0cf26eec2924052a","name":"simple_t_B","status":"RUNNING","owner":"gennai","createTime":"2014-12-14T18:08:13.607Z","comment":"You can get the data with the content starting letter B.","summary":{"name":"gungnir_548dd20d0cf26eec2924052a","status":"ACTIVE","uptimeSecs":2,"numWorkers":1,"numExecutors":3,"numTasks":3}}
gungnir>
```

これにより、RESTサーバが受け取るデータ(先のtuple設定のとおりsimpleという名前がついている)1つに対し、2つのトポロジが登録されたことになります。

言うなれば、これまではsimpleにはsimple_tトポロジのみが紐づいていましたが、このSUBMIT以後は(simpleに)simple_t_Bというトポロジも紐づいた、2つのトポロジが紐づいた状態となっています。

このことはsimpleタプルの情報を確認すること出来ます。
(以下、戻されるJSONにあるtopologiesに、二つのトポロジが格納されています)

```
gungnir> DESC TUPLE simple;
{"name":"simple","fields":{"Id":{"type":"INT"},"Content":{"type":"STRING"}},
"topologies":["548dc7e40cf26eec29240529","548dd20d0cf26eec2924052a"],"owner":"gennai","createTime":"2014-12-14T17:17:46.048Z"}
gungnir>
```

これ以降、simpleにデータを受けると、同じデータが2つのトポロジに流れ込み、それぞれの処理がなされるはずです。
では、実際にこの動作を確認しましょう。
また300個のデータを投入します。

```
[vagrant@internal-vagrant simple]$ post -a 546f4f480cf2cde01845629f -n 100 -v -f data.json -t simple
POST http://localhost:7200/gungnir/v0.1/546f4f480cf2cde01845629f/simple/json
HTTP/1.1 204 No Content
Content-Length: 0
Date: Sun, 30 Nov 2014 12:04:22 GMT
HTTP/1.1 204 No Content
Content-Length: 0
Date: Sun, 30 Nov 2014 12:04:22 GMT
HTTP/1.1 204 No Content
Content-Length: 0
--省略--
[vagrant@internal-vagrant simple]$
```

このように1つのデータに対して複数の処理を紐づけられる機能は、**試験的に新たな処理を追加する**ときなどに便利です。

最後にMongoDBを確認します。
MongoDBのコレクションsimple_outputには、simple_tトポロジによりContentカラムのデータにおいて先頭文字がAのデータが、simple_output_Bにはsimple_t_Bにより先頭文字がBのデータが格納されたことが分かります。

```
[vagrant@internal-vagrant ~]$ mongo
MongoDB shell version: 2.6.5
connecting to: test
> db.simple_output.find();
{ "_id" : ObjectId("547b07580cf23dc96705ef73"), "Id" : 0, "Content" : "ABCDEF" }
{ "_id" : ObjectId("547b07580cf23dc96705ef73"), "Id" : 0, "Content" : "ABCDEF" }
{ "_id" : ObjectId("547b07580cf23dc96705ef73"), "Id" : 0, "Content" : "ABCDEF" }
--省略--
Type "it" for more
> db.simple_output_B.find();
{ "_id" : ObjectId("547b07c60cf245af63550606"), "Id" : 1, "Content" : "BCDEFA" }
{ "_id" : ObjectId("547b07c60cf245af63550607"), "Id" : 1, "Content" : "BCDEFA" }
{ "_id" : ObjectId("547b07c60cf245af63550608"), "Id" : 1, "Content" : "BCDEFA" }
--省略--
Type "it" for more
>
```

なお、simple_t_Bトポロジをsimpleスキーマに紐づく処理から外すには、STOPコマンド、その後同トポロジを削除するにはDROPコマンドを用います。
STOPコマンドで止めた状態であれば、STARTコマンドにて処理を再開させることができます。

```
gungnir> STOP TOPOLOGY simple_t_B;
```

```
gungnir> DROP TOPOLOGY simple_t_B;
```

```
gungnir> START TOPOLOGY simple_t_B;
```

これまでVagrantに格納されているサンプルを用いて、genn.aiのほんの一機能について確認してゆく方法をご紹介しました。
より複雑な、より高度なクエリについては、[こちら](http://pages.genn.ai/)のページを参考にして下さい。



## Getting help

現在、メーリングリスト等は準備できておりませんが、
[ドキュメントサイト](http://pages.genn.ai/) 下段にあるDisqusか、もしくはGitHub上でのやり取りにて出来る限りご質問等にはお答えするようにしています。

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Main developper

* Ikumasa Mukai ([@ikumasa](https://github.com/ikumasa))

## Project lead

* Takeshi Nakano ([@tf0054](https://github.com/tf0054))

## Committers

* Shinji Iida ([@siniida](https://github.com/siniida))
* Gaute Lambertsen ([@gautela](https://github.com/gautela))

## Contributors

* Masaru Makino ([@kotetsu33](https://github.com/kotetsu33))
* Takahiko Ito ([@takahi-i](https://github.com/takahi-i))
