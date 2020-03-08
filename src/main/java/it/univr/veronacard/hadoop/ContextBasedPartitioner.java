package it.univr.veronacard.hadoop;

import it.univr.veronacard.hadoop.conf.OperationConf;
import org.apache.hadoop.util.c;

import java.io.IOException;

public class ContextBasedPartitioner {

    public static void main (String args[]) throws IOException {
        OperationConf configuration = new OperationConf(new GenericOptionsParser(args));
    }
}
