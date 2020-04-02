package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.util.ReflectionUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

public class MultiLevelGridMapper extends MultiBaseMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
        super.map(key, contextData, context);
        StringBuilder keyBuilder = new StringBuilder("part-");
        //TODO does it make sense to use a string as key instead a long, as sum of all realative values?
        long keyValue = 0;
        for (String property : contextData.getContextFields()) {
            Pair<Double, Double> minMax = (Pair<Double, Double>) map.get(property);
            if(minMax == null) {
                Double min = OperationConf.getMinProperty(property, context.getConfiguration());
                Double max = OperationConf.getMaxProperty(property, context.getConfiguration());
                minMax = Pair.of(min, max);
                map.put(property, minMax);
            }
            //TODO how to manage data types?
            Double min = minMax.getLeft();
            Double max = minMax.getRight();
            Double width = (max - min) / numCellPerSide;
            Object propertyValue = ReflectionUtil.readMethod(property, contextData);
            Double value;
            if(propertyValue instanceof WritableComparable)
                value = Double.valueOf(WritablePrimitiveMapper
                        .getBeanFromWritable((WritableComparable) propertyValue).toString());
            else
                value = Double.valueOf(propertyValue.toString());
            if(value == max) {
                keyValue += numCellPerSide -1;
                keyBuilder.append(numCellPerSide-1);
            } else {
                int keyval = (int) ( ( value - min ) / width);
                keyValue += keyval;
                keyBuilder.append(keyval);
            }
            keyBuilder.append("-");
        }
    }
}
