package it.univr.veronacard.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class OperationConfiguration extends Configuration {



    public OperationConfiguration(GenericOptionsParser genericOptionsParser) {
        super(genericOptionsParser.getConfiguration());
    }

}
