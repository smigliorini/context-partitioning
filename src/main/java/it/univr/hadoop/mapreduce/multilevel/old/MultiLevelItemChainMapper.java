package it.univr.hadoop.mapreduce.multilevel.old;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.MultiBaseMapper;
import it.univr.hadoop.writable.TextPairWritable;
import it.univr.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import static java.lang.String.format;


public class MultiLevelItemChainMapper extends MultiBaseMapper<TextPairWritable, ContextData, TextPairWritable,
        ContextData> {

    private static final Logger LOGGER = LogManager.getLogger( MultiLevelItemChainMapper.class );

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {
        super.setup( context );
    }

    @Override
    protected void map( TextPairWritable key, ContextData value, Context context )
            throws IOException, InterruptedException {

        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(key.getFirst().toString());
        Integer currentPropertyPos = Integer.parseInt( key.getSecond().toString() );
        String[] contextFields = value.getContextFields( partition );
        long keyValue = propertyOperationPartition( contextFields[currentPropertyPos],
                value, context.getConfiguration() );
        keyBuilder.append("-");
        keyBuilder.append( format(keyFormat, keyValue) );
        Pair<String, Integer> outputKey;
        if(contextFields.length > currentPropertyPos +1) {
            outputKey = Pair.of( keyBuilder.toString(), currentPropertyPos + 1 );
            context.write( new TextPairWritable(outputKey.getLeft(), outputKey.getRight().toString()), value );
            LOGGER.warn( format( "OUTPUT CHAIN MAP is %s for val: %s", outputKey.getLeft(), value.toString() ) );
        } else {
            //LAST MAPPER
            context.write( new TextPairWritable( keyBuilder.toString(), "" ), value );
            LOGGER.warn( format( "OUTPUT CHAIN MAP is %s for val: %s", keyBuilder.toString(), value.toString() ) );
        }

    }

}
