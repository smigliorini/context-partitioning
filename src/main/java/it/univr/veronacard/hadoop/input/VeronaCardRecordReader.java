package it.univr.veronacard.hadoop.input;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import java.io.IOException;
import java.io.InputStream;

public class VeronaCardRecordReader<V extends VeronaCardWritable> extends RecordReader<IntWritable, V> {

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
