var total = 0;

function evaluate(value) {
  if (value != null && typeof value == 'number') {
    total += value;
  }
  return total;
}

function exclude(value) {
  if (value != null && typeof value == 'number') {
    total -= value;
  }
  return total;
}

function clear() {
  total = 0;
}