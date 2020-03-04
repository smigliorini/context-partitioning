package it.univr.veronacard.partitioning;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class DataUtils {

  private static final int globalMinAge = 10;
  private static final int globalMaxAge = 90;

  private DataUtils() {
    // nothing here
  }

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param outDir
   * @param outFileName
   * @param separator
   */

  public static void buildFractalInput
  ( List<String> lines,
    String outDir,
    String outFileName,
    String separator )
    throws FileNotFoundException {

    if( lines == null ) {
      throw new NullPointerException();
    }
    if( outDir == null ) {
      throw new NullPointerException();
    }
    if( outFileName == null ) {
      throw new NullPointerException();
    }

    final String filepath = outDir + outFileName;
    final PrintWriter outWriter = new PrintWriter( filepath );

    for( String l : lines ) {
      final StringTokenizer tk = new StringTokenizer( l, separator );
      int i = 0;
      String x = null, y = null, t = null, age = null;

      while( tk.hasMoreTokens() ) {
        final String token = tk.nextToken();

        if( i == 0 ) { // vc serial
          i++;

        } else if( i == 1 ) { // x
          x = token;
          i++;

        } else if( i == 2 ) { // y
          y = token;
          i++;

        } else if( i == 3 ) { // time
          t = token;
          i++;

        } else if( i == 4 ) { // poi name
          i++;

        } else if( i == 5 ) {
          age = token;
          i++;
        }
      }
      outWriter.write( String.format( "%s%s%s%s%s%s%s%s" +
                                      "%s%s%s%s%s%s%s%n",
                                      x, separator,
                                      y, separator,
                                      t, separator,
                                      age, separator,
                                      x, separator,
                                      y, separator,
                                      t, separator,
                                      age, separator ) );
    }

    outWriter.close();
  }


  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param outDir
   * @param outFileName
   */

  public static void transformLines
  ( List<String> lines,
    String outDir,
    String outFileName,
    String separator )
    throws FileNotFoundException {

    if( lines == null ) {
      throw new NullPointerException();
    }
    if( outDir == null ) {
      throw new NullPointerException();
    }
    if( outFileName == null ) {
      throw new NullPointerException();
    }

    final String filepath = outDir + outFileName;
    final PrintWriter outWriter = new PrintWriter( filepath );

    final WKTReader reader = new WKTReader();
    final SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );

    for( String l : lines ) {
      final StringTokenizer tk = new StringTokenizer( l, separator );
      final StringBuilder b = new StringBuilder();
      String poiName = null;

      int i = 0;
      while( tk.hasMoreTokens() ) {
        final String token = tk.nextToken();

        if( i == 0 ) {
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 1 ) {
          try {
            final Point p = (Point) reader.read( token );
            b.append( p.getX() );
            b.append( separator );
            b.append( p.getY() );
            b.append( separator );

          } catch( ParseException e ) {
            // append two empty coordinates
            b.append( separator );
            b.append( separator );
          }
          i++;

        } else if( i == 2 ) {
          try {
            final Date d = df.parse( token );
            b.append( d.getTime() );
            b.append( separator );

          } catch( java.text.ParseException e ) {
            // append an empty timestamp
            b.append( separator );
          }
          i++;

        } else if( i == 3 ) {
          poiName = token;
          b.append( poiName );
          b.append( separator );
          i++;
        }
      }
      if( poiName != null ) {
        b.append( generateAge( poiName ) );
      } else {
        b.append( "" );
      }

      b.append( String.format( "\n" ) );
      outWriter.write( b.toString() );
    }
    outWriter.close();
  }


  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param separator
   * @return
   */

  public static Boundaries computeBoundaries ( List<String> lines, String separator ) {

    if( lines == null ) {
      throw new NullPointerException();
    }

    final Boundaries b = new Boundaries();

    for( String l : lines ) {
      final StringTokenizer tk = new StringTokenizer( l, separator );
      int i = 0;

      while( tk.hasMoreTokens() ) {
        final String token = tk.nextToken();

        if( i == 0 ) {
          // vcSerial
          i++;

        } else if( i == 1 ) {
          // x
          final double x = parseDouble( token );
          b.updateMinX( x );
          b.updateMaxX( x );
          i++;

        } else if( i == 2 ) {
          // y
          final double y = parseDouble( token );
          b.updateMinY( y );
          b.updateMaxY( y );
          i++;

        } else if( i == 3 ) {
          // time
          final long time = parseLong( token );
          b.updateMinT( time );
          b.updateMaxT( time );
          i++;

        } else if( i == 4 ) {
          // poi name
          i++;

        } else if( i == 5 ) {
          // age
          final int age = Integer.parseInt( token );
          b.updateMinAge( age );
          b.updateMaxAge( age );
        }
      }
    }
    return b;
  }

  /**
   * MISSING_COMMENT
   *
   * @param poiName
   * @return
   */

  private static int generateAge( String poiName ) {
    if( poiName == null ) {
      throw new NullPointerException();
    }

    switch( poiName ) {
      case "Casa Giulietta":
      case "Arena":
      case "Teatro Romano":
      case "Santa Anastasia":
      case "Giardino Giusti":
      case "Castelvecchio":
      case "Tomba Giulietta":
      case "Palazzo della Ragione":
      case "San Zeno":
        return nextAge( 20, 40 );
      case "San Fermo":
      case "Museo Conte":
      case "Museo Storia":
      case "Sighseeing":
      case "Museo Radio":
      case "Museo Miniscalchi":
      case "AMO":
      case "Torre Lamberti":
      case "Verona Tour":
      case "Museo Lapidario":
      case "Museo Africano":
      case "Centro Fotografia":
        case "Duomo":
          return nextAge( 50, 70 );

      /*case "Casa Giulietta":
        return nextAge( 14, 20 ); // 14, 40
      case "San Fermo":
        return nextAge( 30, 40 ); // 30, 70
      case "San Zeno":
        return nextAge( 50, 60 ); // 40, 80
      case "Museo Conte":
        return nextAge( 70, 80 ); // 60, 70
      case "Museo Storia":
        return nextAge( 50, 60 ); // 50, 70
      case "Arena":
        return nextAge( 25, 35 ); // 10, 80
      case "Palazzo della Ragione":
        return nextAge( 60, 70 ); // 40, 80
      case "Sighseeing":
        return nextAge( 80, 85 ); // 50, 70
      case "Museo Radio":
        return nextAge( 50, 60 ); // 50, 60
      case "Teatro Romano":
        return nextAge( 25, 35 ); // 15, 50
      case "Museo Miniscalchi":
        return nextAge( 55, 60 ); // 35, 60
      case "AMO":
        return nextAge( 65, 70 ); // 50, 80
      case "Torre Lamberti":
        return nextAge( 25, 30 ); // 20, 40
      case "Santa Anastasia":
        return nextAge( 50, 60 ); // 30, 60
      case "Verona Tour":
        return nextAge( 15, 20 ); // 15, 35
      case "Giardino Giusti":
        return nextAge( 15, 25 ); // 30, 60
      case "Museo Lapidario":
        return nextAge( 40, 45 ); // 30, 60
      case "Castelvecchio":
        return nextAge( 20, 30 ); // 20, 50
      case "Museo Africano":
        return nextAge( 45, 50 ); // 35, 50
      case "Tomba Giulietta":
        return nextAge( 15, 25 ); // 25, 60
      case "Centro Fotografia":
        return nextAge( 65, 70 ); // 50, 70
      case "Duomo":
        return nextAge( 60, 70 ); // 15, 70//*/

      default:
        final Random r = new Random();
        return r.nextInt( 60 ) + 10;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param minAge
   * @param maxAge
   * @return
   */

  private static int nextAge( int minAge, int maxAge ) {
    final double std = ( maxAge - minAge ) / 2;
    final double mean = minAge + std;

    int age = 0;
    do {
      final Random r = new Random();
      final double value = r.nextGaussian() * std + mean;
      age = (int) Math.round( value );
    } while( age < globalMinAge || age > globalMaxAge );

    return age;
  }


  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param separator
   * @return
   */

  public static List<Record> parseRecords( List<String> lines, String separator ){
    if( lines == null ){
      throw new NullPointerException();
    }
    if( separator == null ){
      throw new NullPointerException();
    }

    final List<Record> records = new ArrayList<Record>();
    for( String l : lines ){
      records.add( parseRecord( l, separator ) );
    }
    return records;
  }

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param separator
   * @return
   */

  public static Record parseRecord( String line, String separator ) {
    if( line == null ) {
      throw new NullPointerException();
    }
    if( separator == null ) {
      throw new NullPointerException();
    }

    final Record r = new Record();

    final StringTokenizer tk = new StringTokenizer( line, separator );
    int i = 0;

    while( tk.hasMoreTokens() ) {
      final String token = tk.nextToken();
      switch( i ) {
        case 0:
          r.setVcSerial( token );
          i++;
          break;
        case 1:
          try {
            r.setX( parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setX( null );
          }
          i++;
          break;
        case 2:
          try {
            r.setY( parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setY( null );
          }
          i++;
          break;
        case 3:
          try {
            r.setTime( Long.parseLong( token ) );
          } catch( NumberFormatException e ) {
            r.setTime( null );
          }
          i++;
          break;
        case 4:
          r.setPoiName( token );
          i++;
          break;
        case 5:
          try {
            r.setAge( Integer.parseInt( token ) );
          } catch( NumberFormatException e ) {
            r.setAge( null );
          }
          i++;
          break;
      }
    }

    return r;
  }
}
