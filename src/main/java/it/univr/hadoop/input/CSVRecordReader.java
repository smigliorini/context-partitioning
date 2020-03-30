package it.univr.hadoop.input;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public abstract class CSVRecordReader<K extends WritableComparable, V extends Writable> extends RecordReader<LongWritable, V> {

    public static final String DEFAULT_DELIMITER = "\"";
    public static final String DEFAULT_SEPARATOR = ",";

    protected K key;
    protected V value;

    protected LineRecordReader reader;
    protected long linesRead;

    public CSVRecordReader() {
        super();
        this.reader = new LineRecordReader();

    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(reader.nextKeyValue()) {
            Pair<K, V> tuple = parseLine(reader.getCurrentValue().toString());
            key = tuple.getKey();
            value = tuple.getValue();
            return true;
        }
        value = null;
        return false;
    }

    protected abstract Pair<K, V> parseLine(String line);


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        if(reader != null) {
            reader.close();

        }
    }
}
