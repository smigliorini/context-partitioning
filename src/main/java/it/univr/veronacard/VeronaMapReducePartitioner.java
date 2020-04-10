package it.univr.veronacard;

import it.univr.hadoop.ContextBasedPartitioner;

import java.io.IOException;

public class VeronaMapReducePartitioner {

    public static void main (String... args ) throws InterruptedException, IOException, ClassNotFoundException {
        long t1 = System.currentTimeMillis();
        ContextBasedPartitioner partitioner = new ContextBasedPartitioner(args, VeronaCardCSVInputFormat.class);
        partitioner.runPartitioner();
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: "+(t2-t1)+" millis");
    }
}
