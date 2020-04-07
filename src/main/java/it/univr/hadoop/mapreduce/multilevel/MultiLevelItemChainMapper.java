package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import it.univr.hadoop.writable.TextPairWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


public class MultiLevelItemChainMapper <V extends ContextData> extends MultiBaseMapper<TextPairWritable, V> {

    @Override
    protected void map(LongWritable key, ContextData value, Context context) throws IOException, InterruptedException {

    }
}
