package it.univr.veronacard;

import it.univr.hadoop.ContextBasedPartitioner;

import java.io.IOException;

public class VeronaMapReducePartitioner {

    public static void main (String... args ) throws Exception {

        // todo: remove!
        // System.setProperty( "hadoop.home.dir", "/usr/local/hadoop/hadoop-3.2.1/" );

        long t1 = System.currentTimeMillis();
        final ContextBasedPartitioner partitioner =
          new ContextBasedPartitioner(args, VeronaCardCSVInputFormat.class);
        partitioner.runPartitioner();
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: "+(t2-t1)+" millis");
    }
}
