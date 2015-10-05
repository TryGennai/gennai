package org.gennai.gungnir.plugins.udf

import org.gennai.gungnir.FunctionInvoker
import org.gennai.gungnir.ql.QueryOperations._
import org.gennai.gungnir.tuple.GungnirTuple
import org.gennai.gungnir.tuple.schema.TupleSchema
import org.scalatest._

class SumSpec extends FlatSpec with Matchers {

  "Sum" should "The result of function matches" in {
    val  schema = new TupleSchema("tuple1").field("f1").field("f2")
    val tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build
    val tuple2 = GungnirTuple.builder(schema).put("f1", "test").put("f2", 37).build
    val tuple3 = GungnirTuple.builder(schema).put("f1", "test").put("f2", 20).build
    val tuple4 = GungnirTuple.builder(schema).put("f1", "test").put("f2", 100.5).build
    val tuple5 = GungnirTuple.builder(schema).put("f1", "test").put("f2", null).build
    val tuple6 = GungnirTuple.builder(schema).put("f1", "test").put("f2", "test").build

    val invoker = FunctionInvoker.create(classOf[Sum], field("f2"))
    invoker.evaluate(tuple) should be (123L)
    invoker.evaluate(tuple2) should be (160L)
    invoker.evaluate(tuple5) should be (160L)
    invoker.evaluate(tuple6) should be (160L)
    invoker.evaluate(tuple3) should be (180L)
    invoker.exclude(tuple) should be (57L)
    invoker.clear
    invoker.evaluate(tuple3) should be (20L)
    invoker.evaluate(tuple4) should be (120.5)
    invoker.exclude(tuple4) should be (20.0)
    invoker.exclude(tuple5) should be (20.0)
    invoker.exclude(tuple6) should be (20.0)
  }
}
