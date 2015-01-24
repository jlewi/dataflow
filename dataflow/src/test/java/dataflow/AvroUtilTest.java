/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dataflow;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AvroUtilTest {
  @Test
  public void testAvroRecordToJson() {
    Left left = new Left();
    left.setLeftValue(10);

    Right right = new Right();
    right.setRightValue(12);

    // Run the encoder twice and make sure the values are the same.
    byte[] first = AvroUtil.avroRecordToJson(left);
    byte[] second = AvroUtil.avroRecordToJson(left);
    assertArrayEquals(first, second);

    // Make sure the different records evaluate to different values.
    byte[] different = AvroUtil.avroRecordToJson(right);
    boolean isDifferent = false;
    if (first.length != different.length) {
      isDifferent = true;
    } else {
      for (int i = 0; i < first.length; ++i) {
        if (first[i] != different[i]) {
          isDifferent = true;
          break;
        }
      }
    }

    assertTrue(isDifferent);
  }

  @Test
  public void testAvroToFromJson() {
    Left left = new Left();
    left.setLeftValue(10);

    // Run the encoder twice and make sure the values are the same.
    byte[] first = AvroUtil.avroRecordToJson(left);
    Left decoded = new Left();
    AvroUtil.avroJsonToRecord(first, decoded);

    assertEquals(left, decoded);
  }
}
