package it.univr.hadoop;

import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import it.univr.veronacard.VeronaCardWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;


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
        long resultSize = makePartition(configuration);
        System.out.println("Total time: "+(t2-t1)+" millis");
        System.out.println("Result size: "+resultSize);
    }

    private static long makePartition(OperationConf config) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "CBMR");
        job.setJarByClass(ContextBasedPartitioner.class);
        //set Split size configuration
        config.hContextBasedConf.ifPresent(customConf -> {
            FileInputFormat.setMinInputSplitSize(job, customConf.getSplitSize(config.technique));
            FileInputFormat.setMaxInputSplitSize(job, customConf.getSplitSize(config.technique));
        });

        //input
        //TODO: give the input format as parameter, to make it more abstracted
        config.mbrContextData = new VeronaCardWritable();
        job.setInputFormatClass(VeronaCardCSVInputFormat.class);
        Path[] inputPaths = new Path[config.getFileInputPaths().size()];
        config.getFileInputPaths().toArray(inputPaths);
        VeronaCardCSVInputFormat.setInputPaths(job, inputPaths);

        //MBBox map reduce
        MBBoxMapReduce.runMBBoxMapReduce(config, VeronaCardCSVInputFormat.class,false);



        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, config.getOutputPath());

        //job
        job.waitForCompletion(true);
        Counters counters = job.getCounters();
        Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
        return outputRecordCounter.getValue();
    }
}
