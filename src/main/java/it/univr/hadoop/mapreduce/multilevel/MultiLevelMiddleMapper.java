package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import it.univr.hadoop.writable.TextPairWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class MultiLevelMiddleMapper extends MultiBaseMapper<Text, Text, Text, ContextData> {

    private static final Logger LOGGER = LogManager.getLogger(MultiLevelMiddleMapper.class);
    protected String currentPropertyName;
    protected String previousProperty;
    protected Class parserClass;
    protected String parserMethodName;

    protected HashMap<String, Pair<Double, Double>> hashMap;

    protected Text keyInputMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.currentPropertyName = OperationConf.getMultiLevelMapperProperty(context.getConfiguration());
        hashMap = new HashMap<>();

        String parserClassName = OperationConf.getMultiLevelParserClass(context.getConfiguration());
        parserMethodName = OperationConf.getMultiLevelParserMethodName(context.getConfiguration());
        try {
            parserClass = Class.forName(parserClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            LOGGER.error("Class not found");
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        this.keyInputMap = key;
        try {
            Object instance = parserClass.getConstructor().newInstance();
            Method method = parserClass.getMethod(parserMethodName, String.class);
            ContextData contextData = (ContextData) method.invoke(instance, value.toString());
            if(previousProperty == null) {
                List<String> fields = Arrays.asList(contextData.getContextFields());
                int index = fields.indexOf(currentPropertyName);
                previousProperty = fields.get(index-1);
            };
            long keyValue = propertyOperationPartition(currentPropertyName, contextData, context.getConfiguration());
            StringBuilder keyBuilder = new StringBuilder(key.toString());
            keyBuilder.append("-");
            keyBuilder.append(format(keyFormat, keyValue));
            context.write(new Text(keyBuilder.toString()), contextData);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }

    }

    @Override
    protected long propertyOperationPartition(String property, ContextData contextData, Configuration configuration) throws IOException {
        Pair<Double, Double> minMax = getMinMax(keyInputMap.toString(), configuration);
        Double value = readPropertyValue(property, contextData);
        return  getCellPartition(minMax, value);
    }

    @Override
    protected Pair<Double, Double> getMinMax(String key, Configuration configuration) throws IOException {
        Pair<Double, Double> doubleDoublePair = hashMap.get(key);
        if(doubleDoublePair == null) {
            //Retrieve data information from the last middle mapper, to take information about the max min of the current property
            String outputPath = OperationConf.getMultiLevelOutputPath(configuration);
            FileSystem fileSystem = FileSystem.get(configuration);
            Path path = new Path(outputPath, previousProperty);
            List<FileStatus> fileStatusList = Arrays.stream(fileSystem.listStatus(path)).filter(f -> f.isFile()
                            && f.getPath().getName().contains(MultiLevelMiddleReducer.MINMAX_FILE_NAME)
                    ).collect(Collectors.toList());
            for(FileStatus fileStatus : fileStatusList) {
                KeyValueLineRecordReader reader = new KeyValueLineRecordReader(configuration,
                        new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), new String[0]));
                Text keyReader = reader.createKey();
                Text valueReader = reader.createValue();
                while (reader.next(keyReader, valueReader)) {
                    TextPairWritable keypair = new TextPairWritable(keyReader);
                    TextPairWritable valuePair = new TextPairWritable(valueReader);
                    Text indexKey = keypair.getFirst();
                    if (indexKey.toString().equals(key)) {
                        Pair<Double, Double> newMinMax = Pair.of(Double.valueOf(valuePair.getFirst().toString()),
                                Double.valueOf(valuePair.getSecond().toString()));
                        hashMap.put(key, newMinMax);
                        return newMinMax;
                    }
                }
            }
        }
        return doubleDoublePair;
    }
}
