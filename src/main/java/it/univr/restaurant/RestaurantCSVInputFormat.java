package it.univr.restaurant;

import it.univr.hadoop.input.ContextBasedInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
/**
 * Processing a whole file as a record
 */
public class RestaurantCSVInputFormat
  extends ContextBasedInputFormat<LongWritable, RestaurantWritable> {

    @Override
    public RecordReader createRecordReader
      (InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
        return new RestaurantRecordReader();
    }
}
