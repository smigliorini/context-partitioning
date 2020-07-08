package it.univr.hadoop.mapreduce.boxcount;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.ContextBasedReducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class BoxCountingReducer <K extends WritableComparable, VIN extends ContextData, VOUT extends ContextData>
  extends ContextBasedReducer<K, VIN, VOUT> {

  protected void reduce(K key, Iterable<VIN> values, Context context) throws IOException, InterruptedException {
    super.reduce(key, values, context);
  }
}
