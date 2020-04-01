package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.util.ReflectionUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.lang.String.format;

public class MultiDimMapper <V extends ContextData> extends Mapper<LongWritable, ContextData, Text, V> {

    static final Logger LOGGER = LogManager.getLogger(MultiDimMapper.class);

    HashMap<String, Pair<Double, Double>> map;
    int numCellPerSide;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Long splitNumberFiles = OperationConf.getSplitNumberFiles(context.getConfiguration());
        Long contextSetDim = OperationConf.getContextSetDim(context.getConfiguration());
        map = new HashMap<>(contextSetDim.intValue());
        final double powerIndex = 1.0 / contextSetDim;
        LOGGER.info(format("Splits are %d", splitNumberFiles));
        LOGGER.info(format("Power Index %f", powerIndex));
        numCellPerSide = (int) ceil( pow(splitNumberFiles, powerIndex));
        LOGGER.info(format("Num cell per side is: %d", numCellPerSide));
    }

    @Override
    protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
        StringBuilder keyBuilder = new StringBuilder("part-");

        //TODO does it make sense to use a string as key instead a long, as sum of all realative values?
        long keyValue = 0;
        for (String property : contextData.getContextFields()) {
            Pair<Double, Double> minMax = map.get(property);
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
        keyBuilder.deleteCharAt(keyBuilder.length()-1);
        context.write(new Text(keyBuilder.toString()), (V) contextData);
        //context.write(new LongWritable(keyValue), (V) contextData);
    }


}
