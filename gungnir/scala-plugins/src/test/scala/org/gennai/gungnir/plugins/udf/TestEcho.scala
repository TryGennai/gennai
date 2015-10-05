package org.gennai.gungnir.plugins.udf

import org.gennai.gungnir.FunctionInvoker
import org.gennai.gungnir.ql.QueryOperations._
import org.gennai.gungnir.tuple.GungnirTuple
import org.gennai.gungnir.tuple.Struct
import org.gennai.gungnir.tuple.schema.TupleSchema
import org.scalatest._

import org.gennai.gungnir.topology.udf.InvokeFunction

class EchoSpec extends FlatSpec with Matchers {

  "Echo" should "The result of function matches" in {
    val list = new java.util.ArrayList[String]
    list.add("abc")
    list.add("def")
    val list2 = new java.util.ArrayList[String]
    list2.add("abc")
    list2.add("def")
    list2.add("scala")
    val map = new java.util.HashMap[String, Any]
    map.put("xxx", 12)
    map.put("yyy", 34)
    val map2 = new java.util.HashMap[String, Any]
    map2.put("xxx", 12)
    map2.put("yyy", 34)
    map2.put("scala", "value")
    val fieldNames = new java.util.ArrayList[String]
    fieldNames.add("s1")
    fieldNames.add("s2")
    val values  = new java.util.ArrayList[Object]
    values.add("v1")
    values.add("v2")
    val values2  = new java.util.ArrayList[Object]
    values2.add("v1")
    values2.add("v2")
    val struct = new Struct(fieldNames, values)

    val  schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3").field("f4")
      .field("f5")
    val tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).put("f3", list)
      .put("f4", map).put("f5", struct).build

    FunctionInvoker.create(classOf[Echo], field("f1")).evaluate(tuple) should be ("scala 1:test")
    FunctionInvoker.create(classOf[Echo], field("f1"), field("f2"))
      .evaluate(tuple) should be ("scala 1:test, 2:123")
    FunctionInvoker.create(classOf[Echo], "test2", field("f2"))
      .evaluate(tuple) should be ("scala 1:test2, 2:123")
    FunctionInvoker.create(classOf[Echo], field("f3")).evaluate(tuple) should be (list2)
    FunctionInvoker.create(classOf[Echo], field("f4")).evaluate(tuple) should be (map2)
    FunctionInvoker.create(classOf[Echo], field("f5")).evaluate(tuple) should be (values2)
  }
}
