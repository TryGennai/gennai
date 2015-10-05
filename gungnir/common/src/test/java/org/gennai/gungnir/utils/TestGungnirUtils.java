/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir.utils;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TestGungnirUtils {
  @Test
  public void testToTinyIntFromBoolean() throws TypeCastException {
    Byte result = GungnirUtils.toTinyint(true);
    assertEquals(1, result.byteValue());
  }

  @Test
  public void testToTinyIntFromString() throws TypeCastException {
    Byte result = GungnirUtils.toTinyint("10");
    assertEquals(10, result.byteValue());
  }

  @Test
  public void testToTinyIntFromFloat() throws TypeCastException {
    Byte result = GungnirUtils.toTinyint(0.1);
    assertEquals(0, result.byteValue());
  }

  @Test
  public void testToTinyIntFromFloat2() throws TypeCastException {
    Byte result = GungnirUtils.toTinyint(1.1);
    assertEquals(1, result.byteValue());
  }

  @Test
  public void testToTinyIntFromLargeInteger() throws TypeCastException {
    Byte result = GungnirUtils.toTinyint(0x12345610);
    assertEquals(16, result.byteValue()); // lowest byte is extracted
  }

  @Test(expected = TypeCastException.class)
  public void testToTinyIntFromInvalidString() throws TypeCastException {
    GungnirUtils.toTinyint("1#0");
  }

  @Test
  public void testToSmallIntFromBoolean() throws TypeCastException {
    Short result = GungnirUtils.toSmallint(true);
    assertEquals(1, result.shortValue());
  }

  @Test
  public void testToSmallIntFromString() throws TypeCastException {
    Short result = GungnirUtils.toSmallint("10");
    assertEquals(10, result.shortValue());
  }

  @Test
  public void testToSmallIntFromFloat() throws TypeCastException {
    Short result = GungnirUtils.toSmallint(0.1);
    assertEquals(0, result.shortValue());
  }

  @Test
  public void testToSmallIntFromFloat2() throws TypeCastException {
    Short result = GungnirUtils.toSmallint(1.1);
    assertEquals(1, result.shortValue());
  }

  @Test
  public void testToSmallIntFromLargeInteger() throws TypeCastException {
    Short result = GungnirUtils.toSmallint(0x12341000);
    assertEquals(4096, result.shortValue()); // lowest two bytes are extracted
  }

  @Test(expected = TypeCastException.class)
  public void testToTSmallIntFromInvalidString() throws TypeCastException {
    GungnirUtils.toSmallint("1#0");
  }

  @Test
  public void testToIntFromBoolean() throws TypeCastException {
    Integer result = GungnirUtils.toInt(true);
    assertEquals(1, result.intValue());
  }

  @Test
  public void testToIntFromString() throws TypeCastException {
    Integer result = GungnirUtils.toInt("10");
    assertEquals(10, result.intValue());
  }

  @Test
  public void testToIntFromFloat() throws TypeCastException {
    Integer result = GungnirUtils.toInt(0.1);
    assertEquals(0, result.intValue());
  }

  @Test
  public void testToIntFromFloat2() throws TypeCastException {
    Integer result = GungnirUtils.toInt(1.1);
    assertEquals(1, result.intValue());
  }

  @Test
  public void testToIntFromLargeInteger() throws TypeCastException {
    Integer result = GungnirUtils.toInt(0x10000);
    assertEquals(65536, result.intValue()); // lowest four bytes are extracted
  }

  @Test(expected = TypeCastException.class)
  public void testToTIntFromInvalidString() throws TypeCastException {
    GungnirUtils.toInt("1#0");
  }

  @Test
  public void testToBigintFromBoolean() throws TypeCastException {
    Long result = GungnirUtils.toBigint(true);
    assertEquals(1, result.intValue());
  }

  @Test
  public void testToBigintFromString() throws TypeCastException {
    Long result = GungnirUtils.toBigint("10");
    assertEquals(10, result.intValue());
  }

  @Test
  public void testToBigintFromFloat() throws TypeCastException {
    Long result = GungnirUtils.toBigint(0.1);
    assertEquals(0, result.intValue());
  }

  @Test
  public void testToBigintFromFloat2() throws TypeCastException {
    Long result = GungnirUtils.toBigint(1.1);
    assertEquals(1, result.intValue());
  }

  @Test
  public void testToBigintFromLargeInteger() throws TypeCastException {
    Long result = GungnirUtils.toBigint(0x100000000L);
    assertEquals(4294967296L, result.longValue());
  }

  @Test(expected = TypeCastException.class)
  public void testToTBigintFromInvalidString() throws TypeCastException {
    GungnirUtils.toBigint("1#0");
  }

  @Test
  public void testToFloatFromBoolean() throws TypeCastException {
    Float result = GungnirUtils.toFloat(true);
    assertEquals(1.0, result.floatValue(), 0.00001);
  }

  @Test
  public void testToFloatFromString() throws TypeCastException {
    Float result = GungnirUtils.toFloat("10.01");
    assertEquals(10.01f, result.floatValue(), 0.00001);
  }

  @Test
  public void testToFloatFromFloat() throws TypeCastException {
    Float result = GungnirUtils.toFloat(0.01f);
    assertEquals(0.01f, result.floatValue(), 0.00001);
  }

  @Test
  public void testToFloatFromInteger() throws TypeCastException {
    Float result = GungnirUtils.toFloat(1024);
    assertEquals(1024f, result.floatValue(), 0.00001);
  }

  @Test
  public void testToFloatFromDouble() throws TypeCastException {
    Float result = GungnirUtils.toFloat(1.234566); // literals without suffix l becomes double
    assertEquals(1.234566, result.floatValue(), 0.00001);
  }

  @Test
  public void testToDoubleFromString() throws TypeCastException {
    Double result = GungnirUtils.toDouble("10.01");
    assertEquals(10.01, result.floatValue(), 0.00001);
  }

  @Test
  public void testToDoubleFromFloat() throws TypeCastException {
    Double result = GungnirUtils.toDouble(0.01f);
    assertEquals(0.01, result.floatValue(), 0.00001);
  }

  @Test
  public void testToDoubleFromInteger() throws TypeCastException {
    Double result = GungnirUtils.toDouble(1024);
    assertEquals(1024, result.floatValue(), 0.00001);
  }

  @Test
  public void testToDbouleFromDouble() throws TypeCastException {
    Double result = GungnirUtils.toDouble(1.234566);
    assertEquals(1.234566, result.floatValue(), 0.00001);
  }

  @Test
  public void testToBooleanFromString() {
    Boolean result = GungnirUtils.toBoolean("it is simple");
    assertEquals(true, result.booleanValue());
  }

  @Test
  public void testToBooleanFromVoidString() {
    Boolean result = GungnirUtils.toBoolean("");
    assertEquals(false, result.booleanValue());
  }

  @Test
  public void testToBooleanFromFloat() {
    Boolean result = GungnirUtils.toBoolean(0.01f);
    assertEquals(true, result.booleanValue());
  }

  @Test
  public void testToBooleanFromZeroFloatValue() {
    Boolean result = GungnirUtils.toBoolean(0.0f);
    assertEquals(false, result.booleanValue());
  }

  @Test
  public void testToBooleanFromInteger() {
    Boolean result = GungnirUtils.toBoolean(1024);
    assertEquals(true, result.booleanValue());
  }

  @Test
  public void testToBooleanFromZeroIntegerValue() {
    Boolean result = GungnirUtils.toBoolean(0);
    assertEquals(false, result.booleanValue());
  }

  @Test
  public void testToBooleanFromDouble() {
    Boolean result = GungnirUtils.toBoolean(1.234566);
    assertEquals(true, result.booleanValue());
  }

  @Test
  public void testToBooleanFromZeroDoubleValue() {
    Boolean result = GungnirUtils.toBoolean(0.0);
    assertEquals(false, result.booleanValue());
  }

  @Test
  public void testToBooleanFromDate() {
    Boolean result = GungnirUtils.toBoolean(new Date());
    assertEquals(true, result.booleanValue());
  }

  @Test
  public void testToBooleanFromStartDate() {
    Boolean result = GungnirUtils.toBoolean(new Date(0));
    assertEquals(false, result.booleanValue());
  }

  @Test
  public void testToTimestampFromString() throws TypeCastException {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    assertEquals(cal.getTime(), GungnirUtils.toTimestamp(sdf.format(cal.getTime()), "yyyyMMdd"));
  }

  @Test(expected = TypeCastException.class)
  public void testToTimestampFromInvalidFormat() throws TypeCastException {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    assertEquals(cal.getTime(), GungnirUtils.toTimestamp("yyyy-MM-dd", sdf.format(cal.getTime())));
  }

  @Test(expected = TypeCastException.class)
  public void testToTimestampFromNonString() throws TypeCastException {
    assertEquals(new Date(), GungnirUtils.toTimestamp(0, "yyyyMMdd"));
  }

  @Test
  public void testToTimestampFromNumber() throws TypeCastException {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MILLISECOND, 0);
    assertEquals(cal.getTime(),
        GungnirUtils.toTimestamp(TimeUnit.MILLISECONDS.toSeconds(cal.getTimeInMillis())));
  }

  @Test(expected = TypeCastException.class)
  public void testToTimestampFromNonNumber() throws TypeCastException {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MILLISECOND, 0);
    assertEquals(cal.getTime(), GungnirUtils.toTimestamp("-"));
  }

  @Test
  public void testParallelToTimestamp() {
    Executor executor = Executors.newCachedThreadPool();
    Random random = new Random();
    for (int i = 0; i < 100000; i++) {
      final int sec = i * random.nextInt(100);
      executor.execute(new Runnable() {

        @Override
        public void run() {
          Calendar cal = Calendar.getInstance();
          cal.set(Calendar.MILLISECOND, 0);
          cal.add(Calendar.SECOND, sec);
          SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          try {
            assertEquals(cal.getTime(),
                GungnirUtils.toTimestamp(sdf.format(cal.getTime()), "yyyyMMddHHmmss"));
          } catch (TypeCastException e) {
            fail(e.getMessage());
          }
        }
      });
    }
  }
}
