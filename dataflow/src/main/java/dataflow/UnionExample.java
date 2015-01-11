package dataflow;

import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * An example that counts words in Shakespeare. For a detailed walkthrough of this
 * example see:
 *   https://cloud.google.com/dataflow/java-sdk/wordcount-example
 *
 * <p> Concepts: Reading/writing text files; counting a PCollection; user-defined PTransforms
 *
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 *   --project=<PROJECT ID>
 * and a local output file or output prefix on GCS:
 *   --output=[<LOCAL FILE> | gs://<OUTPUT PREFIX>]
 *
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 *   --project=<PROJECT ID>
 *   --stagingLocation=gs://<STAGING DIRECTORY>
 *   --runner=BlockingDataflowPipelineRunner
 * and an output prefix on GCS:
 *   --output=gs://<OUTPUT PREFIX>
 *
 * <p> The input file defaults to gs://dataflow-samples/shakespeare/kinglear.txt and can be
 * overridden with --input.
 */
public class UnionExample {
  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    /**
     * Returns gs://${STAGING_LOCATION}/"counts.txt" as the default destination.
     */
    public static class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        if (dataflowOptions.getStagingLocation() != null) {
          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
              .resolve("counts.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
        }
      }
    }

    /**
     * By default (numShards == 0), the system will choose the shard count.
     * Most programs will not need this option.
     */
    @Description("Number of output shards (0 if the system should choose automatically)")
    int getNumShards();
    void setNumShards(int value);
  }

  /** A DoFn that converts records. */
  static class ConvertRecordsFn extends DoFn<GenericRecord, LeftAndRight> {
    @Override
    public void processElement(ProcessContext c) {
      LeftAndRight leftAndRight = new LeftAndRight();
      leftAndRight.setRight(new Right());
      leftAndRight.setLeft(new Left());
      leftAndRight.getLeft().setLeftValue(-1);
      leftAndRight.getRight().setRightValue(-1);

      GenericRecord r = c.element();
      String schemeName = r.getSchema().getFullName();
      if (schemeName.equals("dataflow.Left")) {
        leftAndRight.getLeft().setLeftValue((Integer) r.get("left_value"));
        System.out.println("Output left");
        c.output(leftAndRight);
      }
      if (schemeName.equals("dataflow.Right")) {

        leftAndRight.getRight().setRightValue((Integer) r.get("right_value"));
        System.out.println("Output right");
        c.output(leftAndRight);
      }
      System.out.println("Schema:" +schemeName);
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(Left.SCHEMA$);
    schemas.add(Right.SCHEMA$);
    Schema unionSchema = Schema.createUnion(schemas);

    PCollection<GenericRecord> inputs = p.apply(AvroIO.Read.named("Read").from(options.getInput()).withSchema(unionSchema));

    inputs.apply(ParDo.of(new ConvertRecordsFn())).setCoder(AvroCoder.of(LeftAndRight.class))
    .apply(AvroIO.Write.named("Write")
        .to(options.getOutput())
        .withNumShards(options.getNumShards())
        .withSchema(LeftAndRight.class));
    p.run();
  }
}
