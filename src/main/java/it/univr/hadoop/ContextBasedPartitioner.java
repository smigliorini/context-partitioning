package it.univr.hadoop;

import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.conf.PartitionTechnique;
import it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridMapper;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import it.univr.veronacard.VeronaCardWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;


public class ContextBasedPartitioner {
    static final Logger LOGGER = LogManager.getLogger(ContextBasedPartitioner.class);

    public static void main (String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        OperationConf configuration = new OperationConf(new GenericOptionsParser(args));

        if(!configuration.validInputOutputFiles()) {
            LOGGER.error("Invalid input files");
            System.exit(1);
        }

        long t1 = System.currentTimeMillis();
        long t2 = System.currentTimeMillis();
        long resultSize = makePartition(configuration, VeronaCardCSVInputFormat.class);
        System.out.println("Total time: "+(t2-t1)+" millis");
        System.out.println("Result size: "+resultSize);
    }

    private static long makePartition(OperationConf config, Class< ? extends FileInputFormat> inputFormatClass) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "CBMR");
        job.setJarByClass(ContextBasedPartitioner.class);
        //set Split size configuration

        config.hContextBasedConf.ifPresentOrElse(customConf -> {
            FileInputFormat.setMinInputSplitSize(job, customConf.getSplitSize(config.technique));
            FileInputFormat.setMaxInputSplitSize(job, customConf.getSplitSize(config.technique));
        }, () -> {
            //mapreduce.input.fileinputformat.split.minsize
        });

        //input
        //TODO: give the input format as parameter, to make it more abstracted
        config.mbrContextData = new VeronaCardWritable();

        //MBBox map reduce
        Pair<ContextData, ContextData> contextData = MBBoxMapReduce.runMBBoxMapReduce(config, inputFormatClass,
                false);
        if(contextData.getLeft() != null) {
            //input setup
            Path[] inputPaths = new Path[config.getFileInputPaths().size()];
            config.getFileInputPaths().toArray(inputPaths);
            FileInputFormat.setInputPaths(job, inputPaths);
            job.setInputFormatClass(inputFormatClass);

            //Mapper setup
            Class<?> mapOutputKeyClass;
            Class<?> mapOutputValueClass;
            Class<? extends Mapper> mapperClass;
            Class<? extends Reducer> reducerClass;
            if(config.technique == PartitionTechnique.ML_GRID) {
                mapperClass = MultiLevelGridMapper.class;
            } else if(config.technique == PartitionTechnique.MD_GRID) {

            } else {

            }

            //job.setMapOutputKeyClass();
            //job.setMapOutputValueClass();
            //job.setMapperClass();
            //Reducer setup
            //job.setReducerClass();
            //output
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, config.getOutputPath());

            //job
            job.waitForCompletion(true);
            Counters counters = job.getCounters();
            Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
            return outputRecordCounter.getValue();
        }
        return 0;
    }
}
