package it.univr.veronacard.hadoop;

import it.univr.veronacard.hadoop.conf.OperationConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class ContextBasedPartitioner {

    public static void main (String args[]) throws IOException {

        OperationConfiguration configuration = new OperationConfiguration(new GenericOptionsParser(args));

    }
}
