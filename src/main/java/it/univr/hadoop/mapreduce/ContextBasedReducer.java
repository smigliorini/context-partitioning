package it.univr.hadoop.mapreduce;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.util.Pair;
import it.univr.util.ReflectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.*;

public class ContextBasedReducer<KEYIN extends WritableComparable, VIN extends ContextData, VOUT extends Writable>
        extends MultiBaseReducer<KEYIN, VIN, VOUT>{

    protected SortedMap<String, SortedMap<String, Pair<Double, Double>>> minMaxReducerFiles;
    public static String SPLITERATOR = ",";

    @Override
    protected void setup(Context context) {
        super.setup(context);
        minMaxReducerFiles = new TreeMap<>();
    }

    @Override
    protected void foreachOperation(KEYIN key, VIN contextData, Configuration configuration) {
        if(OperationConf.isMasterFileEnabled(configuration)) {
            SortedMap<String, Pair<Double, Double>> minMaxProperties =
                    minMaxReducerFiles.computeIfAbsent(key.toString(), k -> new TreeMap<>());
            for (String propertyName : contextData.getContextFields()) {
                Pair<Double, Double> minMax = minMaxProperties.get(propertyName);
                Double value = readPropertyValue(propertyName, contextData);
                if(minMax == null) {
                    minMax = Pair.of(value, value);
                    minMaxProperties.put(propertyName, minMax);
                } else {
                    Double min = Math.min(minMax.getLeft(), value);
                    Double max = Math.max(minMax.getRight(), value);
                    minMaxProperties.put(propertyName, Pair.of(min, max));
                }
            }
        }
    }

    @Override
    protected void masterOutputWriteTask(Configuration configuration) throws IOException, InterruptedException {
        if(OperationConf.isMasterFileEnabled(configuration)) {
            for (Map.Entry<String, SortedMap<String, Pair<Double, Double>>> entry : minMaxReducerFiles.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                String key = entry.getKey();
                for (Pair<Double, Double> value : entry.getValue().values()) {
                    stringBuilder.append(value.getLeft().doubleValue());
                    stringBuilder.append(SPLITERATOR);
                    stringBuilder.append(value.getRight().doubleValue());
                    stringBuilder.append(SPLITERATOR);
                }
                stringBuilder.append(key);
                multipleOutputs.write(NullWritable.get(), new Text(stringBuilder.toString()), MASTER_FILE_NAME);
            }
        }
    }

    protected Double readPropertyValue(String propertyName, ContextData contextData) {
        Object propertyValue = ReflectionUtil.readMethod(propertyName, contextData);
        Double value;
        if (propertyValue instanceof WritableComparable)
            value = Double.valueOf(WritablePrimitiveMapper
                    .getBeanFromWritable((WritableComparable) propertyValue).toString());
        else
            value = Double.valueOf(propertyValue.toString());
        return value;
    }
}
