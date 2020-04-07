package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;

public class MultiLevelGridMapper <V extends ContextData> extends MultiBaseMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
        StringBuilder keyBuilder = new StringBuilder("part-");
        Optional<String> property = Stream.of(contextData.getContextFields()).findFirst();
        if(property.isPresent()) {
            long keyValue = propertyOperationPartition(property.get(), contextData, context.getConfiguration());
            keyBuilder.append(format(keyFormat, keyValue));
            context.write(new Text(keyBuilder.toString()), contextData);
        }

    }

}
