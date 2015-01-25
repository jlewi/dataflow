package dataflow;

import java.util.ArrayList;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class AvroKeyExampleTest {
  // Test the full pipeline.
  @Test
  public void testPipeline() throws Exception {
    Pipeline p = TestPipeline.create();

    ArrayList<Left> lefts = new ArrayList<Left>();
    for (int i = 0; i < 10; ++i) {
      Left l = new Left();
      l.setLeftValue(1);
      lefts.add(l);
    }

    PCollection<Left> inputs = p.apply(
        Create.of(lefts)).setCoder(
            AvroDeterministicCoder.of(Left.class));

    PCollection<Left> outputs = AvroKeyExample.buildPipeline(inputs);

    ArrayList<Left> expectedElements = new ArrayList<Left>();
    Left expected = new Left();
    expected.setLeftValue(10);
    expectedElements.add(expected);

    // You can use DataflowAssert to check the output matches the expected output.
    // DataflowAssert.that(outputs).containsInAnyOrder(expectedElements);

    // Run the pipeline.
    p.run();
  }
}
