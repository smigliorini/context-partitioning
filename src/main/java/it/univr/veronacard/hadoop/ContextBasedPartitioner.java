package it.univr.veronacard.hadoop;

import it.univr.veronacard.hadoop.conf.OperationConf;
import it.univr.veronacard.hadoop.input.VeronaCardCSVInputFormat;
import org.apache.hadoop.mapreduce.*;
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
        long resultSize = doPartition(configuration);
        System.out.println("Total time: "+(t2-t1)+" millis");
        System.out.println("Result size: "+resultSize);
    }

    private static long doPartition(OperationConf config) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(config, "CBMR");
        job.setJarByClass(ContextBasedPartitioner.class);
        //job.setInputFormatClass(VeronaCardCSVInputFormat.class);
        job.waitForCompletion(true);
        Counters counters = job.getCounters();
        Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
        return outputRecordCounter.getValue();
    }
}
