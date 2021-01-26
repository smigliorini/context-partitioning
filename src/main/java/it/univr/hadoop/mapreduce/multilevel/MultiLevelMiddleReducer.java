package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import it.univr.hadoop.util.ContextBasedUtil;
import it.univr.hadoop.writable.TextPairWritable;
import it.univr.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MultiLevelMiddleReducer<VIN extends ContextData, VOUT extends ContextData> extends MultiBaseReducer<Text,
        VIN, VOUT> {

    private final Logger LOGGER = LogManager.getLogger(MultiLevelMiddleReducer.class);
    String propertyName;
    Optional<String> nextProperty;
    HashMap<Pair<String, String>, Pair<Double, Double>> minMax;

    public static final String MINMAX_FILE_NAME = "PropertyMinMax";

    @Override
    protected void setup( Context context ) {
        super.setup( context );
        this.propertyName = OperationConf.getMultiLevelMapperProperty( context.getConfiguration() );
        minMax = new HashMap<>();
        nextProperty = Optional.empty();
    }

    @Override
    protected void reduce( Text key, Iterable<VIN> values, Context context ) throws IOException, InterruptedException {
        StreamSupport.stream( values.spliterator(), false ).forEach(data -> {
            try {
                foreachOperation( key ,data, context.getConfiguration() );
                //write intermediate data to next mapper
                multipleOutputs.write( key, data, key.toString() );
            } catch( IOException | InterruptedException e ) {
                e.printStackTrace();
                LOGGER.error( e.getMessage() );
            }
        });
    }


    @Override
    protected void foreachOperation( Text key, VIN data, Configuration configuration ) {
        //calculate max and min for the next property, this operation save a new map reduce task (MBB)
        //if( nextProperty.toString().isEmpty() ) { // nextProperty.isEmpty()
        if( !nextProperty.isPresent() ) {
            List<String> strings = Arrays.asList( data.getContextFields( partition ) );
            int i = strings.indexOf( propertyName );
            nextProperty = Optional.of( strings.get( i + 1 ) );
        }
        Pair<String, String> hashKey = Pair.of( key.toString(), nextProperty.get() );
        Pair<Double, Double> doubleDoublePair = minMax.get( hashKey );
        Double value = ContextBasedUtil.getDouble( nextProperty.get(), data );
        if( doubleDoublePair == null ) {
            minMax.put( hashKey, Pair.of( value, value ) );
        } else {
            doubleDoublePair.getLeft();
            Double min = Math.min( doubleDoublePair.getLeft(), value );
            Double max = Math.max( doubleDoublePair.getRight(), value );
            minMax.put( hashKey, Pair.of( min, max ) );
        }
        // todo remove
        //System.out.println("key: " + key.toString() + " propertyName: " + propertyName + " nextProperty: " + nextProperty.get());
        //System.out.println("value: " + value);
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException {
        //Once finished write minimum maximum values with the associated split key.
        if( !nextProperty.toString().isEmpty() ) { // nextProperty.isEmpty()
            for( Pair<String,String> key : minMax.keySet() ) {
                Pair<Double, Double> doubleDoublePair = minMax.get( key );
                TextPairWritable valuePair = new TextPairWritable( doubleDoublePair.getLeft().toString(),
                        doubleDoublePair.getRight().toString() );
                TextPairWritable keyPair = new TextPairWritable( key.getLeft(), key.getRight() );
                multipleOutputs.write( keyPair, valuePair, MINMAX_FILE_NAME );
            }
        }
        super.cleanup(context);
    }
}
