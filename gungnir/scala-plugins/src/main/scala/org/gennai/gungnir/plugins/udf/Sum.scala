package org.gennai.gungnir.plugins.udf

class Sum {

   private var total: AnyVal = 0L

   def evaluate(value: Byte) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d += value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l += value
      total = l
    }
    total
  }

  def evaluate(value: Short) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d += value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l += value
      total = l
    }
    total
  }

  def evaluate(value: Int) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d += value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l += value
      total = l
    }
    total
  }

  def evaluate(value: Long) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d += value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l += value
      total = l
    }
    total
  }

  def evaluate(value: Float) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d += value
      total = d
    } else if (total.isInstanceOf[Long]) {
      var d = total.asInstanceOf[Long].doubleValue
      d += value
      total = d
    }
    total
  }

  def evaluate(value: Double) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d += value
      total = d
    } else if (total.isInstanceOf[Long]) {
      var d = total.asInstanceOf[Long].doubleValue
      d += value
      total = d
    }
    total
  }

  def evaluate(value: AnyRef) = total

  def exclude(value: Byte) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d -= value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l -= value
      total = l
    }
    total
  }

  def exclude(value: Short) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d -= value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l -= value
      total = l
    }
    total
  }

  def exclude(value: Int) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d -= value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l -= value
      total = l
    }
    total
  }

  def exclude(value: Long) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d -= value
      total = d
    } else {
      var l = total.asInstanceOf[Long]
      l -= value
      total = l
    }
    total
  }

 def exclude(value: Float) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d -= value
      total = d
    } else {
      var d = total.asInstanceOf[Long].doubleValue
      d -= value
      total = d
    }
    total
  }

 def exclude(value: Double) = {
    if (total.isInstanceOf[Double]) {
      var d = total.asInstanceOf[Double]
      d -= value
      total = d
    } else {
      var d = total.asInstanceOf[Long].doubleValue
      d -= value
      total = d
    }
    total
  }

  def exclude(value: AnyVal) = total

  def clear { total = 0L }
}
