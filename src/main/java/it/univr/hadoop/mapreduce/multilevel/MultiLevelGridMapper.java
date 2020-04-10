package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import it.univr.hadoop.writable.TextPairWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import static java.lang.String.format;

public class MultiLevelGridMapper extends MultiBaseMapper<LongWritable, ContextData,
        TextPairWritable, ContextData> {

    private static final Logger LOGGER = LogManager.getLogger(MultiLevelGridMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
        StringBuilder keyBuilder = new StringBuilder("part-");
        String[] contextFields = contextData.getContextFields();
        String property = contextFields[0];
        long keyValue = propertyOperationPartition(property, contextData, context.getConfiguration());
        keyBuilder.append(format(keyFormat, keyValue));
        Pair<String, Integer> outputKey;
        if(contextFields.length > 1) {
            //1 is the next property position in contextfield array
            outputKey = Pair.of(keyBuilder.toString(), 1);
        } else {
            outputKey = Pair.of(keyBuilder.toString(), -1);
        }
        context.write(new TextPairWritable(outputKey.getLeft(), outputKey.getRight().toString()), contextData);

    }

}
