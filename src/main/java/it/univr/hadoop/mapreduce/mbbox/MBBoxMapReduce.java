package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import it.univr.veronacard.VeronaCardWritable;
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
        fileMBBoxMapReduce(configuration, true);
    }

    public static void fileMBBoxMapReduce(OperationConf config, boolean storeResult) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "MBBoxMapReduce");
        config.mbrContextData = new VeronaCardWritable();
        config.hContextBasedConf.ifPresent(customConf -> {
            FileInputFormat.setMinInputSplitSize(job, customConf.getSplitSize(config.technique));
            FileInputFormat.setMaxInputSplitSize(job, customConf.getSplitSize(config.technique));
        });
        if(storeResult)
            job.setJarByClass(MBBoxMapReduce.class);
        Path[] inputPaths = new Path[config.getFileInputPaths().size()];
        config.getFileInputPaths().toArray(inputPaths);

        job.setMapperClass(MBBoxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        //TODO make it generic
        job.setMapOutputValueClass(ObjectWritable.class);
        job.setCombinerClass(MBBoxCombiner.class);
        job.setReducerClass(MBBoxReducer.class);

        //TODO: give the input format as parameter, to make it more abstracted
        job.setInputFormatClass(VeronaCardCSVInputFormat.class);
        VeronaCardCSVInputFormat.setInputPaths(job, inputPaths);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, config.getOutputPath());

        //output
        job.waitForCompletion(true);
        Counters counters = job.getCounters();
        Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
        outputRecordCounter.getValue();
        ContextData mbrContextData = config.getMbrContextData();
        FileSystem fileSystem = FileSystem.get(config);
        for (FileStatus fileStatus : fileSystem.listStatus(config.getOutputPath())) {
            if(!fileStatus.isDirectory()) {
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
        if(!storeResult)
            fileSystem.delete(config.getOutputPath(), true);
    }

    public static void fileMBBoxMapReduce(Path path) {

    }

}
