package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiLevelGridReducer extends MultiBaseReducer {

    @Override
    protected void reduce(Text key, Iterable<ContextData> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
