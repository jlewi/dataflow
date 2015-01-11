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
// Author: Jeremy Lewi (jeremy@lewi.us)
package dataflow;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFileUtil {
  private static final Logger sLogger = LoggerFactory.getLogger(AvroFileUtil.class);

  /**
   * Write a collection of records to an avro file.
   *
   * Use this function when the schema can't be inferred from the type of
   * record.
   *
   * The stream is closed by the function.
   *
   * @param conf
   * @param path
   * @param records
   */
  public static <T extends GenericContainer> void writeRecords(
      OutputStream outputStream, Iterable<? extends Object> records,
      Schema schema) {
    // Write the data to the file.
    DatumWriter<Object> datumWriter = new SpecificDatumWriter<Object>(schema);
    DataFileWriter<Object> writer = new DataFileWriter<Object>(datumWriter);

    try {
      writer.create(schema, outputStream);
      for (Object record : records) {
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      sLogger.error(
          "There was a problem writing the records to an avro file. " +
              "Exception: " + exception.getMessage(), exception);
    }
  }
}
