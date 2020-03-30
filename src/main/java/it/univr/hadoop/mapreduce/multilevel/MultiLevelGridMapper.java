package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultiLevelGridMapper extends Mapper<LongWritable, ContextData, LongWritable, ContextData> {
    long splitSize;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

    }
}
