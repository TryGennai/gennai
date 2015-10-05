importClass(java.util.List);
importClass(java.util.Map);
importClass(org.gennai.gungnir.tuple.Struct);

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