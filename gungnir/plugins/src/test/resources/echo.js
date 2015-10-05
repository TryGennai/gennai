var List = Java.type('java.util.List');
var Map = Java.type('java.util.Map');
var Struct = Java.type('org.gennai.gungnir.tuple.Struct');

function evaluate(value) {
  if (value instanceof List) {
    value.add("js");
    return value;
  } else if (value instanceof Map) {
    value.put("js", "value");
    return value;
  } else if (value instanceof Struct) {
    return value.getValues();
  } else {
    return "js 1:" + value;
  }
}