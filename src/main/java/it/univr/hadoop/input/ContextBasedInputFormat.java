package it.univr.hadoop.input;


import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

import static java.lang.String.format;

public abstract  class ContextBasedInputFormat<K, V extends ContextData> extends FileInputFormat<K ,V> {
    static final Logger LOGGER = LogManager.getLogger(ContextBasedInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        if(OperationConf.getSplitNumberFiles(job.getConfiguration()) < 1)
            OperationConf.setSplitNumberFiles(job.getConfiguration(), splits.size());
        LOGGER.info(format("Splits number are: %d", splits.size()));
        return splits;
    }
}