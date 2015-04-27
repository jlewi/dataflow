package analytics;

import analytics.records.JobLogMessage;
import analytics.records.JobStatus;
import analytics.records.JobSummary;
import analytics.records.WorkflowStatus;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

/**
 * Transform a stream of JobLogMessage's into a JobSummary of each job.
 */
public class BuildJobSummary extends PTransform<PCollection<JobLogMessage>, PCollection<JobSummary>> {
  private static class KeyJobSummaryByJobId extends DoFn<JobSummary, KV<String, JobSummary>> {
    @Override
    public void processElement(
        DoFn<JobSummary, KV<String, JobSummary>>.ProcessContext c)
            throws Exception {
      JobSummary summary = c.element();
      if (summary.jobId == null) {
        return;
      }
      c.output(KV.of(summary.jobId, summary));
    }
  }

  private static class GroupSummaryAndStatus extends DoFn<KV<String, CoGbkResult>, JobSummary> {
    private TupleTag<JobSummary> jobSummaryTag;
    private  TupleTag<JobStatus> jobStatusTag;
    private  TupleTag<WorkflowStatus> workflowStatusTag;

    public GroupSummaryAndStatus(
        TupleTag<JobSummary> jobSummaryTag,
        TupleTag<JobStatus> jobStatusTag,
        TupleTag<WorkflowStatus> workflowStatusTag) {
      this.jobSummaryTag = jobSummaryTag;
      this.jobStatusTag = jobStatusTag;
      this.workflowStatusTag = workflowStatusTag;
    }

    @Override
    public void processElement(
        DoFn<KV<String, CoGbkResult>, JobSummary>.ProcessContext c)
            throws Exception {
      JobSummary output = new JobSummary();

      CoGbkResult groups = c.element().getValue();
      Iterable<JobSummary> summaries = groups.getAll(jobSummaryTag);
      for (JobSummary s : summaries) {
        // TODO: We should use avro serialization to make a complete copy of the first
        // summary tag.
        output = s;
        break;
      }
      for (JobStatus s : groups.getAll(jobStatusTag)) {
        output.jobStatus = s;
        break;
      }
      for (WorkflowStatus s : groups.getAll(workflowStatusTag)) {
        output.workflowStatus = s;
        break;
      }
      c.output(output);
    }
  }

  @Override
  public PCollection<JobSummary> apply(PCollection<JobLogMessage> messages) {
    TupleTag<JobSummary> jobSummaryTag = new TupleTag<JobSummary>(){};
    TupleTag<JobStatus> jobStatusTag = new TupleTag<JobStatus>(){};
    TupleTag<WorkflowStatus> workflowStatusTag = new TupleTag<WorkflowStatus>(){};

    TupleTag<KV<String, JobStatus>> keyedJobStatusTag = new TupleTag<KV<String, JobStatus>>(){};
    TupleTag<KV<String, WorkflowStatus>> keyedWorkflowStatusTag = new TupleTag<KV<String, WorkflowStatus>>(){};
    TupleTag<KV<String, JobSummary>> keyedJobSummaryTag = new TupleTag<KV<String, JobSummary>>(){};

    SplitJobLogMessage splitDoFn = new SplitJobLogMessage(keyedJobStatusTag, keyedWorkflowStatusTag);

    PCollectionTuple streams = messages.apply(
        ParDo.of(splitDoFn)
        .withOutputTags(
            jobSummaryTag,
            TupleTagList.of(keyedJobStatusTag).and(keyedWorkflowStatusTag)));

    PCollection<JobSummary> jobSummary = streams.get(jobSummaryTag);
    PCollection<KV<String, JobStatus>> jobStatus = streams.get(keyedJobStatusTag);
    PCollection<KV<String, WorkflowStatus>> workflowStatus = streams.get(keyedWorkflowStatusTag);

    PCollection<KV<String, JobStatus>> finalJobStatus = jobStatus.apply(
        Combine.perKey(new JogLogTransforms.CombineJobStatus()));
    PCollection<KV<String, WorkflowStatus>> finalWorkflowStatus = workflowStatus.apply(
        Combine.perKey(new JogLogTransforms.CombineWorkflowStatus()));

    PCollection<KV<String, JobSummary>> keyedSummaries = jobSummary.apply(
        ParDo.of(new KeyJobSummaryByJobId()));

    PCollection<KV<String, CoGbkResult>> groupedCollection =
        KeyedPCollectionTuple.of(jobSummaryTag, keyedSummaries)
        .and(jobStatusTag, finalJobStatus)
        .and(workflowStatusTag, finalWorkflowStatus)
        .apply(CoGroupByKey.<String>create());

    PCollection<JobSummary> outputs = groupedCollection.apply(ParDo.of(
        new GroupSummaryAndStatus(jobSummaryTag, jobStatusTag, workflowStatusTag))).setCoder(
            AvroCoder.of(JobSummary.class));
    return outputs;
  }
}
