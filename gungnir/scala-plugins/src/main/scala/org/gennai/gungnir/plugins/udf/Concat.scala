package org.gennai.gungnir.plugins.udf

import scala.annotation.varargs

class Concat {

  @varargs def evaluate(values: Any*) = {
    values.mkString("")
  }
}
