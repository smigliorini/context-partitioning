package it.univr.veronacard.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceProcedure extends Reducer {

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
