package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.ContextBasedMapper;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Stream;

import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.lang.String.format;

public class MultiDimMapper <V extends ContextData> extends ContextBasedMapper<LongWritable, ContextData, Text, V> {

    static final Logger LOGGER = LogManager.getLogger( MultiDimMapper.class );

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {
        super.setup( context );
    }

    @Override
    protected void map( LongWritable key, ContextData contextData, Context context ) throws IOException, InterruptedException {
        StringBuilder keyBuilder = new StringBuilder( "part-" );
        for( String property : contextData.getContextFields( partition ) ) {
            long value = propertyOperationPartition( property, contextData, context.getConfiguration() );
            keyBuilder.append( format( keyFormat, value ) );
            keyBuilder.append( "-" );
        }
        keyBuilder.deleteCharAt( keyBuilder.length()-1 );
        context.write( new Text(keyBuilder.toString() ), (V) contextData );
    }


}
