package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.util.ReflectionUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Stream;

import static java.lang.Math.ceil;
import static java.lang.Math.pow;

public class MultiDimMapper extends Mapper<LongWritable, ContextData, LongWritable, ContextData> {

    int numCellPerSide;
    HashMap<String, Double> ranges;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Long splitNumberFiles = OperationConf.getSplitNumberFiles(context.getConfiguration());
        Long contextSetDim = OperationConf.getContextSetDim(context.getConfiguration());
        final double powerIndex = 1 / contextSetDim;
        numCellPerSide = (int) ceil( pow( splitNumberFiles, powerIndex));
        ranges = new HashMap<String, Double>(contextSetDim.intValue());
    }

    @Override
    protected void map(LongWritable key, ContextData value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        Stream.of(value.getContextFields()).map(propertyName -> ReflectionUtil.readMethod(propertyName, value));
    }
}
