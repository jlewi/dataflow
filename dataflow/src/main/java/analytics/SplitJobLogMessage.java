package analytics;

import analytics.records.JobLogMessage;
import analytics.records.JobStatus;
import analytics.records.JobSummary;
import analytics.records.WorkflowStatus;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

/**
 * Split a JobLogMessage into multiple streams of data.
 */
public class SplitJobLogMessage extends DoFn<JobLogMessage, JobSummary> {
  private TupleTag<KV<String, JobStatus>> jobStatusTag;
  private TupleTag<KV<String, WorkflowStatus>> workflowStatusTag;

  public SplitJobLogMessage(TupleTag<KV<String,JobStatus>> jobStatusTag,
      TupleTag<KV<String, WorkflowStatus>> workflowStatusTag) {
    this.jobStatusTag = jobStatusTag;
    this.workflowStatusTag = workflowStatusTag;
  }

  @Override
  public void processElement(DoFn<JobLogMessage, JobSummary>.ProcessContext c)
      throws Exception {
    JobLogMessage m = c.element();
    if (m.jobId == null) {
      return;
    }
    if (m.timestampMs != null && m.workflowStatus != null) {
      WorkflowStatus workflowStatus = new WorkflowStatus();
      workflowStatus.timestampMs = m.timestampMs;
      workflowStatus.status = m.workflowStatus;
      c.sideOutput(workflowStatusTag, KV.of(m.jobId,workflowStatus));
    }

    if (m.timestampMs != null && m.jobStatus != null) {
      JobStatus jobStatus = new JobStatus();
      jobStatus.timestampMs = m.timestampMs;
      jobStatus.status = m.jobStatus;
      c.sideOutput(jobStatusTag, KV.of(m.jobId, jobStatus));
    }

    if (m.createWorkflowJobRequest != null) {
      JobSummary summary = new JobSummary();
      summary.jobId = m.jobId;
      summary.projectId = m.projectId;
      c.output(summary);
    }
  }
}
