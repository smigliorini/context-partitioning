package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MultiLevelGridMapper <V extends ContextData> extends MultiBaseMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
        StringBuilder keyBuilder = new StringBuilder("part-");
        //TODO does it make sense to use a string as key instead a long, as sum of all realative values?
        long keyValue = 0;

        keyBuilder.deleteCharAt(keyBuilder.length()-1);
        context.write(new Text(keyBuilder.toString()), contextData);
    }

}
