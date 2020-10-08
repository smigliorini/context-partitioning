package it.univr.restaurant;

import it.univr.convert.RestaurantJsonToCsv;
import it.univr.hadoop.ContextBasedPartitioner;

public class RestaurantMapReducePartitioner {

    public static void main (String... args ) throws Exception {
        // todo: remove!
        //System.setProperty( "hadoop.home.dir", "/usr/local/hadoop/" );

        //RestaurantJsonToCsv.runJsonToCsv();

        long t1 = System.currentTimeMillis();
        final ContextBasedPartitioner partitioner =
          new ContextBasedPartitioner(args, RestaurantCSVInputFormat.class);
        partitioner.runPartitioner();
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: "+(t2-t1)+" millis");
    }
}
