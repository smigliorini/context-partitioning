package it.univr.hadoop.mapreduce;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.multidim.MultiDimMapper;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.util.ReflectionUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.lang.String.format;

public abstract class MultiBaseMapper <K extends WritableComparable, V extends ContextData> extends Mapper<LongWritable, ContextData, K, V> {

    private static final Logger BASE_LOGGER = LogManager.getLogger(MultiDimMapper.class);

    protected HashMap<String, Pair<Double, Double>> map;
    protected int numCellPerSide;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Long splitNumberFiles = OperationConf.getSplitNumberFiles(context.getConfiguration());
        Long contextSetDim = OperationConf.getContextSetDim(context.getConfiguration());
        map = new HashMap<>(contextSetDim.intValue());
        final double powerIndex = 1.0 / contextSetDim;
        BASE_LOGGER.info(format("Splits are %d", splitNumberFiles));
        BASE_LOGGER.info(format("Power Index %f", powerIndex));
        numCellPerSide = (int) ceil( pow(splitNumberFiles, powerIndex));
        BASE_LOGGER.info(format("Num cell per side is: %d", numCellPerSide));
    }

    @Override
    protected void map(LongWritable key, ContextData value, Context context) throws IOException, InterruptedException {

    }

    //Key
    protected long propertyOperationPartition (String property, ContextData contextData, Configuration configuration) {
        Pair<Double, Double> minMax = map.get(property);
        if (minMax == null) {
            minMax = OperationConf.getMinMax(property, configuration);
            map.put(property, minMax);
        }

        //TODO how to manage data types? Assuming every context field as Double compatible
        Double min = minMax.getLeft();
        Double max = minMax.getRight();
        Double width = (max - min) / numCellPerSide;
        Object propertyValue = ReflectionUtil.readMethod(property, contextData);
        Double value;
        if (propertyValue instanceof WritableComparable)
            value = Double.valueOf(WritablePrimitiveMapper
                    .getBeanFromWritable((WritableComparable) propertyValue).toString());
        else
            value = Double.valueOf(propertyValue.toString());
        if (value == max) {
            return numCellPerSide - 1;
        } else {
            return (int) ((value - min) / width);
        }
    }
}
