package it.univr.partitioning;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static it.univr.partitioning.FileUtils.readLines;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class StatsUtils {

  private StatsUtils() {
    // nothing here
  }
  
  // === Methods ===============================================================
  
  /**
   * MISSING_COMMENT
   *
   * @param directory
   * @param partPrefix
   * @param dimensions
   * @return
   */

  public static Map<String, Long> countFileRows
  ( File directory,
    String partPrefix,
    int dimensions,
    boolean countSplits ) {

    if( directory == null ) {
      throw new NullPointerException();
    }

    if( !directory.isDirectory() ) {
      throw new IllegalArgumentException( format( "\"%s\" is not a directory", directory ) );
    }

    final Map<String, Long> result = new HashMap<>();

    final File[] files = directory.listFiles();
    for( File f : files ) {
      final String nf;
      if( !countSplits ) {
        nf = normalizeFileName( f.getName(), partPrefix, dimensions );
      } else {
        nf = f.getName();
      }
      final Long prevValue = result.get( nf ) != null ? result.get( nf ) : 0L;
      final long value = prevValue + f.length();
      result.put( nf, value );
    }

    return result;
  }
  

  /**
   * MISSING_COMMENT
   *
   * @param name
   * @param dimensions
   * @return
   */

  public static String normalizeFileName
  ( String name,
    String partPrefix,
    int dimensions ){

    if( name == null ) {
      throw new NullPointerException();
    }
    if( partPrefix == null ) {
      throw new NullPointerException();
    }

    name = name.replace( partPrefix, "" );

    final StringTokenizer tk = new StringTokenizer( name, "-" );
    final StringBuilder sb = new StringBuilder();

    int i = 0;
    while( tk.hasMoreTokens() && i < dimensions ) {
      sb.append( tk.nextToken() );
      i++;
      if( i < dimensions ){
        sb.append( "-" );
      }
    }
    return sb.toString();
  }

  
  /**
   * %RSD (relative standard deviation) is a statistical measurement that
   * describes the spread of data with respect to the mean and the result is
   * expressed as a percentage.
   *
   * @return
   */

  public static double rsd( List<Double> values ) {
    if( values == null ) {
      throw new NullPointerException();
    }

    double sum = 0.0;
    for( Double v : values ) {
      sum += v;
    }
    double avg = sum / values.size();

    double stdev = 0.0;
    for( double num : values ) {
      stdev += pow( num - avg, 2 );
    }
    stdev = sqrt( stdev / values.size() );

    if( avg != 0 ) {
      return stdev / avg;
    } else {
      return 0.0;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param minValue
   * @param maxValue
   * @param value
   * @return
   */

  public static double normalize( double minValue, double maxValue, double value ) {
    return ( value - minValue ) / ( maxValue - minValue );

  }
}
