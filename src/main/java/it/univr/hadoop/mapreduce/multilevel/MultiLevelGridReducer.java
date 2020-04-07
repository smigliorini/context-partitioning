package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

public class MultiLevelGridReducer<K extends WritableComparable, V extends ContextData> extends MultiBaseReducer<K, V> {

    @Override
    protected void reduce(K key, Iterable<V> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
