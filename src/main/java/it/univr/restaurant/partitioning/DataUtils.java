package it.univr.restaurant.partitioning;

import it.univr.restaurant.RestaurantRecord;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

import static it.univr.partitioning.FileUtils.readLines;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class DataUtils {

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
      String coordX = null, coordY = null, zipcode = null,
              time = null, score = null, restaurantId = null;

      int i = 0;
      final String[] tokens = l.split( separator );
      for ( String token : tokens ) {

        if( i == 0 ) {
          // building
          i++;
        } else if( i == 1 ) {
          // coordX
          coordX = token;
          i++;
        } else if( i == 2 ) {
          // coordY
          coordY = token;
          i++;
        } else if( i == 3 ) {
          // street
          i++;
        } else if( i == 4 ) {
          // zipcode
          zipcode = token;
          i++;
        } else if( i == 5 ) {
          // borough
          i++;
        } else if( i == 6 ) {
          // cuisine
          i++;
        } else if( i == 7 ) {
          // date
          time = token;
          i++;
        } else if( i == 8 ) {
          // grade
          i++;
        } else if( i == 9 ) {
          // score
          score = token;
          i++;
        } else if( i == 10 ) {
          // name
          i++;
        } else if( i == 11 ) {
          // restaurantId
          restaurantId = token;
          i++;
        }
      }
      outWriter.write( String.format( "%s%s%s%s%s%s%s%s%s%s%s%s" +
                      "%s%s%s%s%s%s%s%%s%s%s%s%s%n",
              coordX, separator,
              coordY, separator,
              zipcode, separator,
              time, separator,
              score, separator,
              restaurantId, separator,
              coordX, separator,
              coordY, separator,
              zipcode, separator,
              time, separator,
              score, separator,
              restaurantId, separator
      ) );
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

    // todo: check
    for( String l : lines ) {

      final StringBuilder b = new StringBuilder();
      String building = null, street = null, borough = null, cuisine = null, name = null;

      int i = 0;
      final String[] tokens = l.split( separator );
      for( String token : tokens ) {
        if( i == 0 ) { // building
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 1 ) { // coordX && coordY
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

        } else if( i == 2 ) { // street
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 3 ) { // zipcode
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 4 ) { // borough
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 5 ) { // cuisine
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 6 ) { // $date
          try {
            final Date d = df.parse( token );
            b.append( d.getTime() );
            b.append( separator );

          } catch( java.text.ParseException e ) {
            // append an empty timestamp
            b.append( separator );
          }
          i++;

        } else if( i == 7 ) { // grade
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 8 ) { // score
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 9 ) { // name
          b.append( token );
          b.append( separator );
          i++;

        } else if( i == 10 ) { // restaurantId
          b.append( token );
          b.append( separator );
          i++;

        }
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

  public static RestaurantBoundaries computeBoundaries( List<String> lines, String separator ) {

    if( lines == null ) {
      throw new NullPointerException();
    }
    if( separator == null ) {
      throw new NullPointerException();
    }
    
    final RestaurantBoundaries b = new RestaurantBoundaries();

    for( String l : lines ) {
      int i = 0;
      final String[] tokens = l.split( separator );
      for( String token : tokens ) {

        if( i == 0 ) { // building
          i++;

        } else if( i == 1 ) { // coordX
          final double x = parseDouble( token );
          b.updateMinX( x );
          b.updateMaxX( x );
          i++;

        } else if( i == 2 ) { // coordY
          final double y = parseDouble( token );
          b.updateMinY( y );
          b.updateMaxY( y );
          i++;

        } else if( i == 3 ) { // street
          i++;

        } else if( i == 4 ) { // zipcode
          final int zipcode = Integer.parseInt( token );
          b.updateMinZipcode( zipcode );
          b.updateMaxZipcode( zipcode );
          i++;

        } else if( i == 5 ) { // borough
          i++;

        } else if( i == 6 ) { // cuisine
          i++;

        } else if( i == 7 ) { // $date
          final long t = parseLong( token );
          b.updateMinT( t );
          b.updateMaxT( t );
          i++;

        } else if( i == 8 ) { // grade
          i++;

        } else if( i == 9 ) { // score
          final int score = Integer.parseInt( token );
          b.updateMinScore( score );
          b.updateMaxScore( score );
          i++;

        } else if( i == 10 ) { // name
          i++;

        } else if( i == 11 ) { // restaurantId
          final int restaurantId = Integer.parseInt( token );
          b.updateMinId( restaurantId );
          b.updateMaxId( restaurantId );
          i++;

        }
      }
    }
    return b;
  }
  
  
  /**
   * MISSING_COMMENT
   *
   * @param directory
   * @param separator
   * @return
   */
  
  public static RestaurantBoundaries computeGlobalBoundaries( File directory, String separator ) {
    
    if( directory == null ) {
      throw new NullPointerException();
    }
    if( separator == null ) {
      throw new NullPointerException();
    }
    if( !directory.isDirectory() ) {
      throw new IllegalArgumentException( format( "\"%s\" is not a directory", directory ) );
    }
    
    final RestaurantBoundaries boundaries = new RestaurantBoundaries();
    
    for( File f : directory.listFiles() ) {
      final List<String> lines = readLines( f, false );
      
      for( String l : lines ) {
        int i = 0;
        final String[] tokens = l.split( separator );
        for( String token : tokens ) {
          switch( i ) {
            case 1: // coordX
              final Double x = parseDouble( token );
              boundaries.updateMinX( x );
              boundaries.updateMaxX( x );
              i++;
              break;
            case 2: // coordY
              final Double y = parseDouble( token );
              boundaries.updateMinY( y );
              boundaries.updateMaxY( y );
              i++;
              break;
            case 4: // zipcode
              // TODO: NumberFormatException
              //final Integer z = parseInt( token );
              Integer z = null;
              try {
                z = parseInt( token );
              } catch( NumberFormatException e ) {
                z = Math.toIntExact( Math.round( parseDouble( token ) ) );
              }
              boundaries.updateMinZipcode( z );
              boundaries.updateMaxZipcode( z );
              i++;
              break;
            case 7: // $date
              // TODO: NumberFormatException
              //final Long d = parseLong( token );
              Long d = null;
              try {
                d = parseLong( token );
              } catch( NumberFormatException e ) {
                d = Math.round( parseDouble( token ) );
              }
              boundaries.updateMinT( d );
              boundaries.updateMaxT( d );
              i++;
              break;
            case 9: // score
              // TODO: NumberFormatException
              //final Integer s = parseInt( token );
              Integer s = null;
              try {
                s = parseInt( token );
              } catch( NumberFormatException e ) {
                s = Math.toIntExact( Math.round( parseDouble( token ) ) );
              }
              boundaries.updateMinScore( s );
              boundaries.updateMaxScore( s );
              i++;
              break;
            case 11: // restaurantId
              // TODO: NumberFormatException
              //final Integer id = parseInt( token );
              Integer id = null;
              try {
                id = parseInt( token );
              } catch( NumberFormatException e ) {
                id = Math.toIntExact( Math.round( parseDouble( token ) ) );
              }
              boundaries.updateMinId( id );
              boundaries.updateMaxId( id );
              i++;
              break;
            case 0: // building
            case 3: // street
            case 5: // borough
            case 6: // cuisine
            case 8: // grade
            case 10: // name
              i++;
              break;
          }
        }
      }//*/
    }
    return boundaries;
  }
  
  
  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param separator
   * @return
   */

  public static List<RestaurantRecord> parseRecords( List<String> lines, String separator ){
    if( lines == null ){
      throw new NullPointerException();
    }
    if( separator == null ){
      throw new NullPointerException();
    }

    final List<RestaurantRecord> records = new ArrayList<>();
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

  public static RestaurantRecord parseRecord( String line, String separator ) {
    if( line == null ) {
      throw new NullPointerException();
    }
    if( separator == null ) {
      throw new NullPointerException();
    }

    final RestaurantRecord r = new RestaurantRecord();

    int i = 0;
    final String[] tokens = line.split( separator );
    for( String token : tokens ) {
      switch ( i ) {
        case 0: // building
          if( !token.isEmpty() ) {
            r.setBuilding( token );
          } else {
            r.setBuilding( null );
          }
          i++;
          break;
        case 1: // coordX
          try {
            r.setCoordX( parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setCoordX( null );
          }
          i++;
          break;
        case 2: // coordY
          try {
            r.setCoordY( parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setCoordY( null );
          }
          i++;
          break;
        case 3: // street
          if( !token.isEmpty() ) {
            r.setStreet( token );
          } else {
            r.setStreet( null );
          }
          i++;
          break;
        case 4: // zipcode
          try {
            //r.setZipcode( Integer.parseInt( token ) );
            r.setZipcode( Double.parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setZipcode( null );
          }
          i++;
          break;
        case 5: // borough
          if( !token.isEmpty() ) {
            r.setBorough( token );
          } else {
            r.setBorough( null );
          }
          i++;
          break;
        case 6: // cuisine
          if( !token.isEmpty() ) {
            r.setCuisine( token );
          } else {
            r.setCuisine( null );
          }
          i++;
          break;
        case 7: // $date
          try {
            r.setTime( Double.parseDouble( token ) );
          } catch ( NumberFormatException e ) {
            r.setTime( null );
          }
          i++;
          break;
        case 8: // grade
          if( !token.isEmpty() ) {
            r.setGrade( token );
          } else {
            r.setGrade( null );
          }
          i++;
          break;
        case 9: // score
          try {
            r.setScore( Double.parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setScore( null );
          }
          i++;
          break;
        case 10: // name
          if( !token.isEmpty() ) {
            r.setName( token );
          } else {
            r.setName( null );
          }
          i++;
          break;
        case 11: // restaurantId
          try {
            //r.setRestaurantId( Integer.parseInt( token ) );
            r.setRestaurantId( Double.parseDouble( token ) );
          } catch( NumberFormatException e ) {
            r.setRestaurantId( null );
          }
          i++;
          break;
      }
    }

    return r;
  }
}
