package it.univr.veronacard;

import it.univr.hadoop.ContextBasedPartitioner;

import java.io.IOException;

public class VeronaMapReducePartitioner {

    public static void main (String... args ) throws InterruptedException, IOException, ClassNotFoundException {
        long t1 = System.currentTimeMillis();
        long resultSize = ContextBasedPartitioner.makePartitions(args, VeronaCardCSVInputFormat.class);
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: "+(t2-t1)+" millis");
        System.out.println("Result size: "+resultSize);
    }
}
