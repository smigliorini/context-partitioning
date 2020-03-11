package it.univr.veronacard.hadoop.input;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class CSVInputFormat<K, V> extends FileInputFormat<K ,V> {
    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

        return null;
    }

    @Override
    protected long computeSplitSize(long goalSize, long minSize, long blockSize) {

        return super.computeSplitSize(goalSize, minSize, blockSize);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return super.getSplits(job, numSplits);
    }
}
