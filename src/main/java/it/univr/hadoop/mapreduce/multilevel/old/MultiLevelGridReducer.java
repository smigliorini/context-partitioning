package it.univr.hadoop.mapreduce.multilevel.old;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import it.univr.hadoop.writable.TextPairWritable;

import java.io.IOException;

public class MultiLevelGridReducer<VIN extends ContextData, VOUT extends ContextData> extends MultiBaseReducer<TextPairWritable,
        VIN, VOUT> {

    @Override
    protected void reduce(TextPairWritable key, Iterable<VIN> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
