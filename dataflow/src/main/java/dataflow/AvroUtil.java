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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides various utilities such as routines for encoding AvroRecords as json. This
 * is useful for using AvroRecords as keys.
 *
 */
public class AvroUtil {
  private static final Logger sLogger = LoggerFactory.getLogger(
      AvroUtil.class);

//  // Coder Factory allows us to register coders as defaults.
//  public static class CoderFactory extends CoderRegistry.CoderFactory {
//    private final Class type;
//
//    public CoderFactory(Class type) {
//      this.type = type;
//    }
//
//    @Override
//    public Coder<?> create(List<? extends Coder<?>> typeArgumentCoders) {
//      Schema schema = ReflectData.get().getSchema(type);
//      if (AvroDeterministicCoder.isDeterministic(schema)) {
//        return AvroDeterministicCoder.of(type);
//      } else {
//        return AvroCoder.of(type);
//      }
//    }
//
//    @Override
//    public List<Object> getInstanceComponents(Object value) {
//      return null;
//    }
//  }

  /**
   * Return the json representation of an avro record.
   * @param record
   * @return
   */
  public static <T extends SpecificRecordBase> byte[] avroRecordToJson(
      T record) {
    Schema schema = record.getSchema();
    try {
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      DatumWriter<T> writer = new SpecificDatumWriter<T>(schema);

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          schema, generator);

      writer.write(record, encoder);
      encoder.flush();

      return outStream.toByteArray();
    } catch(IOException e){
      sLogger.error(
          "There was a problem writing the record. Exception: " + e.getMessage(), e);
    }
    return null;
  }

  /**
   * Decode the json encoded avro record into the provided datum
   */
  public static <T extends SpecificRecordBase> void avroJsonToRecord(
      byte[] json, T record) {
    Schema schema = record.getSchema();
    try {
      ByteArrayInputStream inStream = new ByteArrayInputStream(json);
      DatumReader<T> reader = new SpecificDatumReader<T>(schema);


      JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, inStream);

      reader.read(record, decoder);
    } catch(IOException e){
      sLogger.error(
          "There was a problem reading the record. Exception: " + e.getMessage(), e);
    }
  }
}
