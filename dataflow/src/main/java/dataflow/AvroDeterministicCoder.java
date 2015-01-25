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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;

/**
 * A subclass of AvroCoder which only works for schemas which can be encoded
 * determnistically.
 *
 * This coder allows deterministic schemas to be used as keys in Dataflow.
 */
public class AvroDeterministicCoder<T> extends AvroCoder<T>{
  /**
   * Returns an {@code AvroCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T> AvroDeterministicCoder<T> of(Class<T> type) {
    return new AvroDeterministicCoder<>(type, ReflectData.get().getSchema(type));
  }

  /**
   * Returns an {@code AvroDeterministicCoder} instance for the Avro schema. The implicit
   * type is GenericRecord.
   */
  public static AvroDeterministicCoder<GenericRecord> of(Schema schema) {
    return new AvroDeterministicCoder<>(GenericRecord.class, schema);
  }

  protected AvroDeterministicCoder(Class<T> type, Schema schema) {
    super(type, schema);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @JsonCreator
  public static AvroDeterministicCoder<?> of(
      @JsonProperty("type") String classType,
      @JsonProperty("schema") String schema) throws ClassNotFoundException {
    Schema.Parser parser = new Schema.Parser();
    return new AvroDeterministicCoder(Class.forName(classType), parser.parse(schema));
  }

  /**
   * Return true if the specified schema can be encoded deterministically.
   *
   * @param schema
   */
  public static boolean isDeterministic(Schema schema) {
    if(schema.getType() == Type.ARRAY) {
      // Arrays aren't deterministic because the blocks can be encoded
      // two different ways.
      return false;
    } else if (schema.getType() == Type.MAP) {
      // Maps aren't deterministic because the blocks can be encoded
      // two different ways and the order of the elements isn't deterministic.
      return false;
    } else if (schema.getType() == Type.UNION) {
      // A union is encoded deterministically if its schemas are all deterministic.
      for (Schema subSchema : schema.getTypes()) {
        if (!isDeterministic(subSchema)) {
          return false;
        }
      }
    } else if (schema.getType() == Type.RECORD) {
      // A record is deterministic as long as all its fields can be encoded
      // deterministically.
      for (Field f : schema.getFields()) {
        if (!isDeterministic(f.schema())) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public boolean isDeterministic() {
    return true;
  }
}
