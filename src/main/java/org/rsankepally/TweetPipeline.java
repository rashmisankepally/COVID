package org.rsankepally;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
/**
 * Dataflow streaming pipeline to read tweets from PubSub topic and write the payload to BigQuery
 */

public class TweetPipeline {
    private static final String TOPIC = "projects/bigdata-273220/topics/got_COVID_tweets";
    private static final String BIGQUERY_DESTINATION = "%s:got_COVID_tweets.hashtags";

    public static void main(String[] args) {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Tweets_Read_PubSub", PubsubIO
                .readMessagesWithAttributes()
                .fromTopic(TOPIC))
                .apply("Tweets_Extract_Payload", ParDo.of(new ExtractTweetPayload()))
                .apply("Tweets_Write_BigQuery", BigQueryIO.writeTableRows()
                        .to(String.format(BIGQUERY_DESTINATION, options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchema()));
        pipeline.run();
    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("timestamp").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("hashtag").setType("STRING"));
        return new TableSchema().setFields(fields);
    }

    public static class ExtractTweetPayload extends DoFn<PubsubMessage, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String payload = new String(c.element().getPayload(), StandardCharsets.UTF_8);
            c.output(new TableRow()
                    .set("timestamp", System.currentTimeMillis())
                    .set("payload", payload)
            );
        }
    }
}
