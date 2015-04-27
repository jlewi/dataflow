package analytics;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import analytics.records.JobLogMessage;
import analytics.records.JobStatus;
import analytics.records.JobSummary;
import analytics.records.WorkflowStatus;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class BuildJobSummaryTest {
  @Test
  public void testBuildJobSummary() {
    Pipeline p = TestPipeline.create();

    List<JobLogMessage> inputs = new ArrayList<JobLogMessage>();

    String jobId = "1";
    JobLogMessage m = new JobLogMessage();
    m.jobId = jobId;
    m.projectId = "my-project";
    m.createWorkflowJobRequest = "";
    inputs.add(m);

    m = new JobLogMessage();
    m.jobId = jobId;
    m.timestampMs = 1L;
    m.jobStatus = "started";
    inputs.add(m);

    m = new JobLogMessage();
    m.jobId = jobId;
    m.timestampMs = 2L;
    m.jobStatus = "finished";
    inputs.add(m);

    m = new JobLogMessage();
    m.jobId = jobId;
    m.timestampMs = 1L;
    m.workflowStatus = "startedWorkflow";
    inputs.add(m);

    m = new JobLogMessage();
    m.jobId = jobId;
    m.timestampMs = 2L;
    m.workflowStatus = "finishedWorkflow";
    inputs.add(m);

    PCollection<JobSummary> outputs = p.apply(Create.of(inputs)).apply(
        new BuildJobSummary());

    JobSummary expected = new JobSummary();
    expected.jobId = jobId;
    expected.projectId = "my-project";
    expected.jobStatus = new JobStatus();
    expected.jobStatus.timestampMs = 2L;
    expected.jobStatus.status = "finished";
    expected.workflowStatus = new WorkflowStatus();
    expected.workflowStatus.timestampMs = 2L;
    expected.workflowStatus.status = "finishedWorkflow";

    DataflowAssert.that(outputs).containsInAnyOrder(expected);
    p.run();
  }
}
