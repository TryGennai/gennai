total = 0

def evaluate(value):
  global total
  if value is not None and isinstance(value, (int, long, float, complex)):
    total += value
  return total

def exclude(value):
  global total
  if value is not None and isinstance(value, (int, long, float, complex)):
    total -= value
  return total

def clear():
  global total
  total = 0