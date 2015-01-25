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

import java.util.ArrayList;

import org.apache.avro.Schema;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * A simple example which uses an avro record as the key for a GroupBy
 * operation.
 */
public class AvroKeyExample {
  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    String getOutput();
    void setOutput(String value);
  }

  /** A DoFn that Keys records by value records. */
  static class KeyLeftFn extends DoFn<Left, KV<Left, Integer>> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(KV.of(c.element(), c.element().getLeftValue()));
    }
  }


  static class SumFn extends DoFn<KV<Left, Iterable<Integer>>, Left> {
    @Override
    public void processElement(ProcessContext c) {
      Iterable<Integer> values = c.element().getValue();

      Left output = new Left();
      output.setLeftValue(0);

      for (Integer i : values) {
        output.setLeftValue(i + output.getLeftValue());
      }
      c.output(output);
    }
  }

  public static PCollection buildPipeline(PCollection<Left> inputs) {
    PCollection<Left> outputs = inputs
        .apply(ParDo.of(new KeyLeftFn()))
        .setCoder(KvCoder.of(AvroDeterministicCoder.of(Left.class),
            VarIntCoder.of()))
            .apply(GroupByKey.<Left, Integer>create())
            .setCoder(KvCoder.of(AvroDeterministicCoder.of(Left.class),
                IterableCoder.of(VarIntCoder.of())))
                .apply(ParDo.of(new SumFn()))
                .setCoder(AvroDeterministicCoder.of(Left.class));
    return outputs;
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(Left.SCHEMA$);
    schemas.add(Right.SCHEMA$);
    Schema unionSchema = Schema.createUnion(schemas);

    PCollection<Left> inputs = p.apply(
        AvroIO.Read.named("Read")
        .from(options.getInput())
        .withSchema(Left.class)).setCoder(AvroDeterministicCoder.of(Left.class));

    PCollection<Left> outputs = buildPipeline(inputs);
    outputs.apply(AvroIO.Write.named("Write")
        .to(options.getOutput())
        .withSchema(Left.class));
    p.run();
  }
}
