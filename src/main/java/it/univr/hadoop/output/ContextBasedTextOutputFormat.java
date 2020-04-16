package it.univr.hadoop.output;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ContextBasedTextOutputFormat extends TextOutputFormat{

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext taskContext) throws IOException {
        Path jobOutputPath = getOutputPath(taskContext);
        return new ContextBasedOutputCommitter(jobOutputPath, taskContext);
    }
}
