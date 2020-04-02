package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiLevelCombiner<V extends ContextData> extends Reducer<Text, V, Text, V> {

    @Override
    protected void reduce(Text key, Iterable<V> values, Context context) throws IOException, InterruptedException {

    }
}
