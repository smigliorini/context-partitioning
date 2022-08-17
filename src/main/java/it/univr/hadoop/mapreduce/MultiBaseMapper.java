package it.univr.hadoop.mapreduce;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.util.Pair;
import it.univr.util.ReflectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.lang.String.format;

public abstract class MultiBaseMapper<KEYIN, VALUEIN,
  KEYOUT extends WritableComparable, VOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VOUT> {

  protected static final String KEY_STRING_FORMAT = "%%0%sd";
  private static final Logger BASE_LOGGER = LogManager.getLogger( MultiBaseMapper.class );
  protected String keyFormat;

  protected HashMap<String, Pair<Double, Double>> propertyMinMaxMap;
  protected int numCellPerSide;
  protected Integer[] partition;

  @Override
  protected void setup( Context context ) throws IOException, InterruptedException {
    super.setup( context );
    // todo fast fix
    int splitNumberFiles = 32;
    //int splitNumberFiles = OperationConf.getSplitNumberFiles( context.getConfiguration() );
    Long contextSetDim = OperationConf.getContextSetDim( context.getConfiguration() );
    partition = OperationConf.getPartitionFields( context.getConfiguration() );
    propertyMinMaxMap = new HashMap<>( contextSetDim.intValue() );
    final double powerIndex = 1.0 / contextSetDim;
    numCellPerSide = (int) ceil( pow( splitNumberFiles, powerIndex ) );
    keyFormat = String.format( KEY_STRING_FORMAT, numCellPerSide == 0 ? 1 : numCellPerSide );
    BASE_LOGGER.debug( format( "Splits are %d", splitNumberFiles ) );
    BASE_LOGGER.debug( format( "Power Index %f", powerIndex ) );
    BASE_LOGGER.debug( format( "Num cell per side is: %d", numCellPerSide ) );
  }

  @Override
  protected void map( KEYIN key, VALUEIN value, Context context ) throws IOException, InterruptedException {
  }

  /**
   * Calculate and retrieve the cell position of the property data inside the
   * grid.
   *
   * @param property
   * @param contextData
   * @param configuration
   * @return
   * @throws IOException
   */
  protected long propertyOperationPartition
  ( String property,
    ContextData contextData,
    Configuration configuration )
    throws IOException {

    Pair<Double, Double> minMax = getMinMax( property, configuration );
    Double value = readPropertyValue( property, contextData );
    return getCellPartition( minMax, value );
  }

  protected long propertyOperationPartition
    ( String property,
      ContextData contextData,
      Configuration configuration,
      List<Double> parts )
    throws IOException {

    // Pair<Double, Double> minMax = getMinMax(property, configuration);
    Double value = readPropertyValue( property, contextData );
    return getCellPartition( value, parts );
  }

  protected Pair<Double, Double> getMinMax( String key, Configuration configuration ) throws IOException {
    Pair<Double, Double> minMax = propertyMinMaxMap.get( key );
    if( minMax == null ) {
      minMax = OperationConf.getMinMax( key, configuration );
      propertyMinMaxMap.put( key, minMax );
    }
    return minMax;
  }

  protected Double readPropertyValue( String propertyName, ContextData contextData ) {
    Object propertyValue = ReflectionUtil.readMethod( propertyName, contextData );
    Double value;
    if( propertyValue instanceof WritableComparable )
      value = Double.valueOf( WritablePrimitiveMapper
                                .getBeanFromWritable( (WritableComparable) propertyValue ).toString() );
    else
      value = Double.valueOf( propertyValue.toString() );
    return value;
  }

  protected int getCellPartition( Pair<Double, Double> minMax, Double value ) {
    Double min = minMax.getLeft();
    Double max = minMax.getRight();
    Double width = ( max - min ) / numCellPerSide;
    if( value.equals( max ) ) {
      return numCellPerSide - 1;
    } else {
      return (int) ( ( value - min ) / width );
    }
  }

  protected int getCellPartition( Double value, List<Double> parts ) {
        /* Double min = minMax.getLeft();
        Double max = minMax.getRight();
        Double width = (max - min) / numCellPerSide;
        if (value.equals(max)) {
            return numCellPerSide - 1;
        } else {
            return (int) ((value - min) / width);
        }//*/

    for( int i = 0; i < parts.size() - 1; i++ ) {
      if( value >= parts.get( i ) && value < parts.get( i+1 )){
          return i;
      }
    }
    // if the value is equal to the upper bound of the last partition, assign
    // it to the last partition which index is equal to parts.size() - 1, namely
    // the index of the last possible start.
    if( value.equals( parts.get( parts.size() - 1 ))){
      return parts.size() - 2;
    }
    throw new IllegalStateException( "No valid cell found!" );
  }
}
