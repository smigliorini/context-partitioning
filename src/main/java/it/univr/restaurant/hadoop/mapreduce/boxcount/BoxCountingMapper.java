package it.univr.restaurant.hadoop.mapreduce.boxcount;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.ContextBasedMapper;
import it.univr.hadoop.mapreduce.multidim.MultiDimMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class BoxCountingMapper<V extends ContextData> extends ContextBasedMapper<LongWritable, ContextData, Text, V> {

  static final Logger LOGGER = LogManager.getLogger( MultiDimMapper.class );
  static Map<Integer,List<Double>> parts;
  static int numDimensions;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    final Configuration conf = context.getConfiguration();

    parts = new HashMap<>();
    numDimensions = Integer.parseInt( conf.get( "dimensions" ) );

    for( int i = 0; i < numDimensions; i++ ){

      final String s = conf.get( format( "part-%s", i ) );
      final int start = s.indexOf( "(" );
      final int end = s.indexOf( ")" );

      if( start == -1 || end == -1 ){
        throw new IllegalArgumentException( format( "Illegal grid specification: %s", s ));
      }

      final String value = s.substring( start+1, end );
      // TODO: modifica (check negative number and NumberFormatException)
      // error not negative number: s = QuadTree (-73,961800000000000--73,908899999999990--73,856000000000000)
      final String tmp1 = value.replace(',', '.');
      String tmp;
      if( tmp1.contains("--")) {
        tmp = tmp1.replace("--", ",-");
      } else {
        tmp = tmp1.replace("-", ",");
      }
      // todo remove
      //System.out.println("tmp1: " + tmp + " tmp: " + tmp);

      //final StringTokenizer tk = new StringTokenizer( value, "-" );
      final StringTokenizer tk = new StringTokenizer( tmp, "," );
      final List<Double> divs = new ArrayList<>();
      Double prev = null;
      while( tk.hasMoreTokens() ){
        final String token = tk.nextToken();
        final Double current = Double.parseDouble( token );
        if( prev == null || !current.equals( prev ) ) {
          divs.add( current );
          prev = current;
        }
      }

      parts.put( i, divs );
    }
  }

  @Override
  protected void map(LongWritable key, ContextData contextData, Context context) throws IOException, InterruptedException {
    final String[] fields = contextData.getContextFields();

    if( fields.length != numDimensions ){
      throw new IllegalStateException
        ( format( "Mismatch between the number of dimensions (%d != %d)",
                  fields.length,
                  numDimensions ));
    }

    final StringBuilder keyBuilder = new StringBuilder("part-");
    for( int i  = 0; i < numDimensions; i++ ){
      final String property = fields[i];

      long value = propertyOperationPartition(property, contextData, context.getConfiguration(), parts.get( i ) );
      keyBuilder.append(format(keyFormat, value));
      keyBuilder.append("-");
    }

    keyBuilder.deleteCharAt(keyBuilder.length()-1);
    context.write(new Text(keyBuilder.toString()), (V) contextData);//*/
  }


}
