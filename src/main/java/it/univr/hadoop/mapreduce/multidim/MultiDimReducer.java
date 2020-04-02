package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MultiDimReducer extends MultiBaseReducer {

    @Override
    protected void reduce(Text key, Iterable<ContextData> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
