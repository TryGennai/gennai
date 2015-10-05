from java.util import List
from java.util import Map
from org.gennai.gungnir.tuple import Struct

def evaluate(value):
  if isinstance(value, List):
    value.add("py");
    return value;
  elif isinstance(value, Map):
    value.put("py", "value")
    return value;
  elif isinstance(value, Struct):
    return value.getValues();
  else:
    return "py 1:" + str(value)