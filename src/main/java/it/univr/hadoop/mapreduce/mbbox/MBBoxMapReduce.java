package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static java.lang.String.format;

/**
 * Minimum Bounding
 */
public class MBBoxMapReduce {
    static final Logger LOGGER = LogManager.getLogger(MBBoxMapReduce.class);

    public static void main (String... args) throws IOException, InterruptedException, ClassNotFoundException {
        OperationConf configuration = new OperationConf(new GenericOptionsParser(args));
        if(!configuration.validInputOutputFiles()) {
            LOGGER.error("Invalid input files");
            System.exit(1);
        }
        Path[] inputPaths = new Path[configuration.getFileInputPaths().size()];
        configuration.getFileInputPaths().toArray(inputPaths);
        runMBBoxMapReduce(configuration, VeronaCardCSVInputFormat.class,true);
    }

    /**
     * Retrieve Minimum bounding box of the data, by returning a context place holder for minimum bound values.
     * The return value is a context data schema which is equal to the value genetic class of the FileInputFormat class
     * passed as argument.
     * @param config
     * @param inputFormatClass
     * @param storeResult
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static ContextData runMBBoxMapReduce(OperationConf config, Class< ? extends FileInputFormat> inputFormatClass, boolean storeResult) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "MBBoxMapReduce");
        config.hContextBasedConf.ifPresent(customConf -> {
            FileInputFormat.setMinInputSplitSize(job, customConf.getSplitSize(config.technique));
            FileInputFormat.setMaxInputSplitSize(job, customConf.getSplitSize(config.technique));
        });
        if (storeResult) {
            job.setJarByClass(MBBoxMapReduce.class);
        }
        Path[] inputPaths = new Path[config.getFileInputPaths().size()];
        config.getFileInputPaths().toArray(inputPaths);

        job.setMapperClass(MBBoxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ObjectWritable.class);
        job.setCombinerClass(MBBoxCombiner.class);
        job.setReducerClass(MBBoxReducer.class);

        ContextData contextData = null;
        job.setInputFormatClass(inputFormatClass);
        Type genericSuperclass = inputFormatClass.getGenericSuperclass();

        //Retrieve InputFormat information to process the data and return a Context data value holder of the range.
        ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
        Type[] actualTypeArgument = parameterizedType.getActualTypeArguments();
        Type type = actualTypeArgument[1];
        if (ContextData.class.isAssignableFrom((Class<?>) type)) {
            Class<? extends ContextData> valueClass = (Class<? extends ContextData>) type;
            try {
                contextData = valueClass.getConstructor().newInstance();
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }

            FileInputFormat.setInputPaths(job, inputPaths);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, config.getOutputPath());

            //output
            job.waitForCompletion(true);
            Counters counters = job.getCounters();
            Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
            outputRecordCounter.getValue();
            ContextData mbrContextData = contextData;
            FileSystem fileSystem = FileSystem.get(config);
            for (FileStatus fileStatus : fileSystem.listStatus(config.getOutputPath())) {
                if (!fileStatus.isDirectory()) {
                    KeyValueLineRecordReader reader = new KeyValueLineRecordReader(config,
                            new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), new String[0]));
                    Text key = reader.createKey();
                    Text value = reader.createValue();
                    while (reader.next(key, value)) {
                        try {
                            PropertyDescriptor descriptor = new PropertyDescriptor(key.toString(),
                                    mbrContextData.getClass());
                            Object propertyValue = WritablePrimitiveMapper.getBeanObjectFromText(value, descriptor.getPropertyType());
                            descriptor.getWriteMethod().invoke(mbrContextData, propertyValue);
                            LOGGER.info(format("MBR output field: %s, value %s", key, value));
                        } catch (IllegalAccessException | InvocationTargetException | IntrospectionException e) {
                            e.printStackTrace();
                        }
                    }
                    reader.close();
                }
            }

            if (!storeResult)
                fileSystem.delete(config.getOutputPath(), true);
            LOGGER.warn(mbrContextData.toString());
            return mbrContextData;
        }

        return null;
    }
}
