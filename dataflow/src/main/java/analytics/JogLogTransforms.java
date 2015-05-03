package analytics;

import analytics.records.JobLogMessage;
import analytics.records.JobStatus;
import analytics.records.WorkflowStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Transforms and DoFns for working with JobLogMessages.
 */
public class JogLogTransforms {

  /**
   * DoFn to parse the json representation of a job logmessage into the
   * JobLogMessage class.
   */
  public static class ParsJsonDoFn extends DoFn<String, JobLogMessage> {

    @Override
    public void processElement(DoFn<String, JobLogMessage>.ProcessContext c)
        throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(c.element());

      JobLogMessage log = new JobLogMessage();
      JsonNode jobId = node.get("job_id");
      if (jobId != null) {
        log.jobId = jobId.asText();
      }

      JsonNode project = node.get("project_id");
      if (project != null) {
        log.projectId = project.asText();
      }

      JsonNode jobStatus = node.get("job_status");
      if (jobStatus != null) {
        log.jobStatus = jobStatus.asText();
      }

      JsonNode workflowStatus = node.get("workflow_status");
      if (jobStatus != null) {
        log.workflowStatus = workflowStatus.asText();
      }

      JsonNode timestampMs = node.get("timestamp_ms");
      if (timestampMs != null) {
        log.timestampMs = timestampMs.asLong();
      }

      JsonNode createWorkflowJobRequest = node.get("create_workflow_job_request");
      if (createWorkflowJobRequest != null) {
        log.createWorkflowJobRequest = createWorkflowJobRequest.asText();
      }

      c.output(log);
    }
  }

  /**
   * Extract a WorkflowStatus keyed by JobId from the JobLogMessage.
   */
  public static class EmitWorkflowStatusDoFn extends DoFn<JobLogMessage, KV<String, WorkflowStatus>> {
    @Override
    public void processElement(
        DoFn<JobLogMessage, KV<String, WorkflowStatus>>.ProcessContext c)
            throws Exception {
      JobLogMessage log = c.element();
      if (log.timestampMs == null || log.workflowStatus == null ||
          log.jobId == null) {
        return;
      }
      WorkflowStatus status = new WorkflowStatus();
      status.status = log.workflowStatus;
      status.timestampMs = log.timestampMs;

      c.output(KV.of(log.jobId, status));
    }
  }

  /**
   * Combiner to output the most recent workflow status.
   */
  public static class CombineWorkflowStatus extends KeyedCombineFn<String, WorkflowStatus, WorkflowStatus, WorkflowStatus> {
    @Override
    public WorkflowStatus createAccumulator(String key) {
      WorkflowStatus status = new WorkflowStatus();
      status.timestampMs = Long.MIN_VALUE;
      status.status = "";
      return status;
    }

    @Override
    public WorkflowStatus addInput(
        String key, WorkflowStatus accumulator, WorkflowStatus value) {
      if (accumulator.timestampMs < value.timestampMs) {
        accumulator.timestampMs = value.timestampMs;
        accumulator.status = value.status;
      }
      return accumulator;
    }

    @Override
    public WorkflowStatus mergeAccumulators(String key,
        Iterable<WorkflowStatus> accumulators) {
      WorkflowStatus result = null;
      for (WorkflowStatus item : accumulators) {
        if (result == null || result.timestampMs < item.timestampMs) {
          result = item;
        }
      }
      return result;
    }

    @Override
    public WorkflowStatus extractOutput(String key, WorkflowStatus accumulator) {
      return accumulator;
    }
  }

  /**
   * Extract a JobStatus keyed by JobId from the JobLogMessage.
   */
  public static class EmitJobStatusDoFn extends DoFn<JobLogMessage, KV<String, JobStatus>> {
    @Override
    public void processElement(
        DoFn<JobLogMessage, KV<String, JobStatus>>.ProcessContext c)
            throws Exception {
      JobLogMessage log = c.element();
      if (log.timestampMs == null || log.workflowStatus == null ||
          log.jobId == null) {
        return;
      }
      JobStatus status = new JobStatus();
      status.status = log.jobStatus;
      status.timestampMs = log.timestampMs;

      c.output(KV.of(log.jobId, status));
    }
  }


  /**
   * Combiner to output the most recent job status.
   */
  public static class CombineJobStatus extends KeyedCombineFn<String, JobStatus, JobStatus, JobStatus> {
    @Override
    public JobStatus createAccumulator(String key) {
      JobStatus status = new JobStatus();
      status.timestampMs = Long.MIN_VALUE;
      status.status = "";
      return status;
    }

    @Override
    public JobStatus addInput(String key, JobStatus accumulator, JobStatus value) {
      if (accumulator.timestampMs < value.timestampMs) {
        accumulator.timestampMs = value.timestampMs;
        accumulator.status = value.status;
      }
      return accumulator;
    }

    @Override
    public JobStatus mergeAccumulators(String key,
        Iterable<JobStatus> accumulators) {
      JobStatus result = new JobStatus();
      result.timestampMs = Long.MIN_VALUE;
      for (JobStatus item : accumulators) {
        if (result == null || result.timestampMs < item.timestampMs) {
          result = item;
        }
      }
      return result;
    }

    @Override
    public JobStatus extractOutput(String key, JobStatus accumulator) {
      return accumulator;
    }
  }
}
