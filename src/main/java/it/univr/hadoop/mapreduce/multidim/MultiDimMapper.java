package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
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
import java.util.stream.Stream;

import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.lang.String.format;

public class MultiDimMapper <V extends ContextData> extends MultiBaseMapper {

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
        Stream.of(contextData.getContextFields())
                .forEach(property -> {
                    //TODO do we format the long.
                    keyBuilder.append(propertyOperationPartition(property, contextData, context.getConfiguration()));
                    keyBuilder.append("-");
                });
        keyBuilder.deleteCharAt(keyBuilder.length()-1);

        context.write(new Text(keyBuilder.toString()), contextData);
    }


}
