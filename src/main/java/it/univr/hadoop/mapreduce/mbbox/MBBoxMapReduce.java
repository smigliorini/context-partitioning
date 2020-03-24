package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.conf.OperationConf;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import it.univr.veronacard.VeronaCardWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

import java.io.IOException;

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

        Job job = Job.getInstance(configuration, "MBBoxMapReduce");
        //TO REMOVE AFTER TEST
        configuration.mbrContextData = new VeronaCardWritable();
        configuration.hContextBasedConf.ifPresent(customConf -> {
            FileInputFormat.setMinInputSplitSize(job, customConf.getSplitSize(configuration.technique));
            FileInputFormat.setMaxInputSplitSize(job, customConf.getSplitSize(configuration.technique));
        });
        job.setJarByClass(MBBoxMapReduce.class);
        ///
        fileMBBoxMapReduce(inputPaths, configuration, job);
    }

    public void fileMBBoxMapReduce(Path inputFile, OperationConf configuration) throws IOException, InterruptedException, ClassNotFoundException {
        //fileMBBoxMapReduce(new Path[]{inputFile}, configuration);
    }

    public static void fileMBBoxMapReduce(Path[] inputFiles, OperationConf config, Job job) throws IOException, ClassNotFoundException, InterruptedException {
        if(job == null)
            job = Job.getInstance(config, "MBBoxMapReduce");

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
        //TODO TEST
        System.out.println(config.getMbrContextData().toString());
    }

}
