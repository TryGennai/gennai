package org.gennai.gungnir.plugins.udf

import org.gennai.gungnir.FunctionInvoker
import org.gennai.gungnir.ql.QueryOperations._
import org.gennai.gungnir.tuple.GungnirTuple
import org.gennai.gungnir.tuple.schema.TupleSchema
import org.scalatest._

import org.gennai.gungnir.topology.udf.InvokeFunction

class ScalaParams {

  def evaluate(value: Seq[Any]) = {
    value :+ "scala"
  }

  def evaluate(value: Map[String, String]) = {
    value + ("scala" -> "value")
  }
}

class FunctionSpec extends FlatSpec with Matchers {

  "ScalaParams" should "The result of function matches" in {
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
    val  schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3")
    val tuple = GungnirTuple.builder(schema).put("f1", list).put("f2", map).build

    FunctionInvoker.create(classOf[ScalaParams], field("f1")).evaluate(tuple) should be (list2)
    FunctionInvoker.create(classOf[ScalaParams], field("f2")).evaluate(tuple) should be (map2)
  }
}
