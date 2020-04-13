package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.util.Optional;

import static java.lang.String.format;

public class MultiLevelGridMapper extends MultiBaseMapper<LongWritable, ContextData,
        Text, ContextData> {

    private static final Logger LOGGER = LogManager.getLogger(MultiLevelGridMapper.class);
    public static final String START_CAPTION_FILE = "part-";
    String propertyName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.propertyName = OperationConf.getMultiLevelMapperProperty(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
         StringBuilder keyBuilder = new StringBuilder(START_CAPTION_FILE);
        long keyValue = propertyOperationPartition(propertyName, contextData, context.getConfiguration());
        keyBuilder.append(format(keyFormat, keyValue));
        context.write(new Text(keyBuilder.toString()), contextData);
    }

}
