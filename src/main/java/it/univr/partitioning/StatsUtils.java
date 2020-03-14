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
   * @param partDirectory
   * @param outputFile
   * @param numDimensions
   * @param separator
   * @param partPrefix
   * @param countSplits
   * @throws IOException
   */

  public static void buildStatFile
    ( File partDirectory,
      File outputFile,
      int numDimensions,
      String separator,
      String partPrefix,
      boolean countSplits )
    throws IOException {

    if( partDirectory == null ) {
      throw new NullPointerException();
    }
    if( outputFile == null ) {
      throw new NullPointerException();
    }
    if( separator == null ) {
      throw new NullPointerException();
    }
    if( partPrefix == null ) {
      throw new NullPointerException();
    }

    final Boundaries b = computeGlobalBoundaries( partDirectory, separator );

    final Map<String, Long> rows = countFileRows( partDirectory, partPrefix, numDimensions, countSplits );
    final Map<String, Double[]> rsd = computeRsd
      ( partDirectory, separator, partPrefix, b, numDimensions, countSplits );

    try( BufferedWriter bw = new BufferedWriter( new FileWriter( outputFile ) ) ) {
      bw.write( String.format( "Split%s"
                               + "NumRows%s"
                               + "RSD x%s"
                               + "RSD y%s"
                               + "RSD t%s"
                               + "RSD age%s"
                               + "%n",
                               separator, separator,
                               separator, separator,
                               separator, separator ) );

      final List<String> keys = new ArrayList<>( rows.keySet() );
      Collections.sort( keys );

      for( String k : keys ) {
        bw.write( String.format( "%s%s"
                                 + "%s%s"
                                 + "%s%s"
                                 + "%s%s"
                                 + "%s%s"
                                 + "%s%s"
                                 + "%n",
                                 k, separator,
                                 rows.get( k ), separator,
                                 rsd.get( k )[0], separator,
                                 rsd.get( k )[1], separator,
                                 rsd.get( k )[2], separator,
                                 rsd.get( k )[3], separator ) );
      }

      final Set<String>[] lks = new Set[numDimensions];
      for( int i = 0; i < numDimensions; i++ ){
        lks[i] = new HashSet<>();
      }

      for( String k : keys ){
        k = k.replace( partPrefix, "" );
        final StringTokenizer tk = new StringTokenizer( k, "-" );
        int i = 0;
        while( tk.hasMoreTokens() && i < numDimensions ){
          lks[i].add( tk.nextToken() );
          i++;
        }
      }

      for( int i = 0; i < numDimensions; i++ ){
        System.out.printf( "Parts for level: %d: ", i );
        final Iterator<String> it = lks[i].iterator();
        while( it.hasNext() ){
          System.out.printf( "%s ", it.next() );
        }
        System.out.printf( "%n" );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param directory
   * @param sepearator
   * @return
   */

  public static Boundaries computeGlobalBoundaries( File directory, String sepearator ) {
    if( directory == null ) {
      throw new NullPointerException();
    }
    if( sepearator == null ) {
      throw new NullPointerException();
    }


    if( !directory.isDirectory() ) {
      throw new IllegalArgumentException( format( "\"%s\" is not a directory", directory ) );
    }

    final Boundaries boundaries = new Boundaries();

    for( File f : directory.listFiles() ) {
      final List<String> lines = readLines( f, false );
      for( String l : lines ) {
        final StringTokenizer tk = new StringTokenizer( l, sepearator );
        int i = 0;

        while( tk.hasMoreTokens() ) {
          switch( i ) {
            case 0: // vc serial
              tk.nextToken();
              i++;
              break;
            case 1:
              final Double x = parseDouble( tk.nextToken() );
              boundaries.updateMinX( x );
              boundaries.updateMaxX( x );
              i++;
              break;
            case 2:
              final Double y = parseDouble( tk.nextToken() );
              boundaries.updateMinY( y );
              boundaries.updateMaxY( y );
              i++;
              break;
            case 3:
              final Long t = parseLong( tk.nextToken() );
              boundaries.updateMinT( t );
              boundaries.updateMaxT( t );
              i++;
              break;
            case 4: // poi name
              tk.nextToken();
              i++;
              break;
            case 5:
              final Integer a = parseInt( tk.nextToken() );
              boundaries.updateMinAge( a );
              boundaries.updateMaxAge( a );
              i++;
              break;
          }
        }
      }
    }
    return boundaries;
  }


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
   * @param directory
   * @param separator
   * @param partPrefix
   * @param b
   * @param numDimensions
   * @param countSplits
   * @return
   */

  public static Map<String, Double[]> computeRsd
  ( File directory,
    String separator,
    String partPrefix,
    Boundaries b,
    int numDimensions,
    boolean countSplits ) {

    if( directory == null ) {
      throw new NullPointerException();
    }
    if( separator == null ) {
      throw new NullPointerException();
    }
    if( b == null ) {
      throw new NullPointerException();
    }

    if( !directory.isDirectory() ) {
      throw new IllegalArgumentException( format( "\"%s\" is not a directory", directory ) );
    }

    final Map<String, Double[]> result = new HashMap<>();

    final File[] files = directory.listFiles();

    final HashMap<String, List<Double>> xValueMap = new HashMap<>();
    final HashMap<String, List<Double>> yValueMap = new HashMap<>();
    final HashMap<String, List<Double>> tValueMap = new HashMap<>();
    final HashMap<String, List<Double>> aValueMap = new HashMap<>();

    for( File f : files ) {
      final List<String> lines = readLines( f, false );

      final String mf;
      if( !countSplits ) {
        mf = normalizeFileName( f.getName(), partPrefix, numDimensions );
      }  else {
        mf = f.getName();
      }

      List<Double> xValues = xValueMap.get( mf );
      if( xValues == null ){
        xValues = new ArrayList<>();
      }
      List<Double> yValues = xValueMap.get( mf );
      if( yValues == null ){
        yValues = new ArrayList<>();
      }
      List<Double> tValues = xValueMap.get( mf );
      if( tValues == null ){
        tValues = new ArrayList<>();
      }
      List<Double> aValues = xValueMap.get( mf );
      if( aValues == null ){
        aValues = new ArrayList<>();
      }

      for( String l : lines ) {
        final StringTokenizer tk = new StringTokenizer( l, separator );
        int i = 0;

        while( tk.hasMoreTokens() ) {
          switch( i ) {
            case 0: // vc serial
              tk.nextToken();
              i++;
              break;
            case 1:
              final double valueX = parseDouble( tk.nextToken() );
              xValues.add( normalize( b.getMinX(), b.getMaxX(), valueX ) );
              i++;
              break;
            case 2:
              final double valueY = parseDouble( tk.nextToken() );
              yValues.add( normalize( b.getMinY(), b.getMaxY(), valueY ) );
              i++;
              break;
            case 3:
              final double valueT = new Double( parseLong( tk.nextToken() ));
              tValues.add( normalize( b.getMinT(), b.getMaxT(), valueT ));
              i++;
              break;
            case 4: // poi name
              tk.nextToken();
              i++;
              break;
            case 5:
              final double valueA = new Double( parseInt( tk.nextToken() ) );
              aValues.add( normalize( b.getMinAge(), b.getMaxAge(), valueA ));
              i++;
              break;
          }
        }
      }
      xValueMap.put( mf, xValues );
      yValueMap.put( mf, yValues );
      tValueMap.put( mf, tValues );
      aValueMap.put( mf, aValues );
    }

    for( String f : xValueMap.keySet() ){
      final double xRsd = rsd( xValueMap.get( f ) );
      final double yRsd = rsd( yValueMap.get( f ) );
      final double tRsd = rsd( tValueMap.get( f ) );
      final double aRsd = rsd( aValueMap.get( f ) );

      result.put( f, new Double[]{xRsd, yRsd, tRsd, aRsd} );
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

  private static String normalizeFileName
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
    while( tk.hasMoreTokens() && i < dimensions ){
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

  private static double rsd( List<Double> values ) {
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

  private static double normalize( double minValue, double maxValue, double value ) {
    return ( value - minValue ) / ( maxValue - minValue );

  }
}
