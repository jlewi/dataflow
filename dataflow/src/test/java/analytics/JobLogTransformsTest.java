package analytics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import analytics.records.JobLogMessage;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;

public class JobLogTransformsTest {
  @Test
  public void testParsJsonDoFn() throws IOException {
    // This test uses a json file that contains the josn version of some records
    String jsonResourcePath =
        "analytics/job_log_messages.json";

    InputStream inStream = this.getClass().getClassLoader().getResourceAsStream(
        jsonResourcePath);

    if (inStream == null) {
      fail("Could not find resource:" + jsonResourcePath);
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));

    List<String> inputs = new ArrayList<String>();
    String line = reader.readLine();
    while (line != null) {
      inputs.add(line);
      line = reader.readLine();
    }
    reader.close();
    inStream.close();

    JogLogTransforms.ParsJsonDoFn doFn = new JogLogTransforms.ParsJsonDoFn();

    DoFnTester<String, JobLogMessage> fnTester = DoFnTester.of(doFn);

    List<JobLogMessage> outputs = fnTester.processBatch(inputs.toArray(new String[inputs.size()]));

    List<JobLogMessage> expected = new ArrayList<JobLogMessage>();

    JobLogMessage expectedMessage = new JobLogMessage();
    expectedMessage.jobId = "2015-03-28_21_53_20-13877069909479840012";
    expectedMessage.timestampMs = 1427604929975L;
    expectedMessage.workflowStatus = "SUCCEEDED";
    expectedMessage.jobStatus = "";

    expected.add(expectedMessage);
    expectedMessage = new JobLogMessage();
    expectedMessage.jobId = "2015-03-28_21_53_20-13877069909479840012";
    expectedMessage.projectId = "dataflow-jlewi";
    expectedMessage.timestampMs = 1427604800205L;
    expected.add(expectedMessage);

    assertEquals(outputs.size(), expected.size());
    Assert.assertThat(
        outputs,
        JUnitMatchers.hasItems(expected.toArray(new JobLogMessage[expected.size()])));
  }
}
