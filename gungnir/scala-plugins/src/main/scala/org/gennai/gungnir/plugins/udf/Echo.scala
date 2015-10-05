package org.gennai.gungnir.plugins.udf

import org.gennai.gungnir.tuple.Struct

class Echo {

  def evaluate(value1: Any) = {
    if (value1.isInstanceOf[Seq[_]]) {
      value1.asInstanceOf[Seq[AnyRef]] :+ "scala"
    } else if (value1.isInstanceOf[Map[_, _]]) {
      value1.asInstanceOf[Map[AnyRef, AnyRef]] + ("scala" -> "value")
    } else if (value1.isInstanceOf[Struct]) {
      value1.asInstanceOf[Struct].getValues
    } else {
      "scala 1:" + value1
    }
  }

  def evaluate(value1: Any, value2: Any) = {
    "scala 1:" + value1 + ", 2:" + value2
  }
}
