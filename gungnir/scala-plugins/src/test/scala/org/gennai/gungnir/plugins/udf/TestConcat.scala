package org.gennai.gungnir.plugins.udf

import org.gennai.gungnir.FunctionInvoker
import org.gennai.gungnir.ql.QueryOperations._
import org.gennai.gungnir.topology.udf.InvokeAggregateFunction
import org.gennai.gungnir.tuple.GungnirTuple
import org.gennai.gungnir.tuple.schema.TupleSchema
import org.scalatest._

import org.gennai.gungnir.topology.udf.InvokeFunction

class ConcatSpec extends FlatSpec with Matchers {

  "Concat" should "The result of function matches" in {
    val  schema = new TupleSchema("tuple1").field("f1").field("f2")
    val tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build

    FunctionInvoker.create(classOf[Concat], field("f1"), "-", field("f2"))
      .evaluate(tuple) should be ("test-123")
  }
}
