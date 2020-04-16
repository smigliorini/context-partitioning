package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.ContextBasedReducer;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

public class MultiDimReducer<K extends WritableComparable, VIN extends ContextData, VOUT extends ContextData>
        extends ContextBasedReducer<K, VIN, VOUT> {

    @Override
    protected void reduce(K key, Iterable<VIN> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }


}
