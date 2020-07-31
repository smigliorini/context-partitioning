package it.univr.veronacard;

import it.univr.hadoop.input.ContextBasedInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class VeronaCardCSVInputFormat
  extends ContextBasedInputFormat<LongWritable, VeronaCardWritable> {

    @Override
    public RecordReader createRecordReader
      (InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
        return new VeronaCardRecordReader();
    }
}
