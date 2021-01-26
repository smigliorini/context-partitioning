package it.univr.restaurant.partitioning;

import it.univr.restaurant.RestaurantRecord;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.List;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;

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
      String coordX = null, coordY = null, $date = null, score = null;
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
          i++;
        } else if( i == 5 ) {
          // borough
          i++;
        } else if( i == 6 ) {
          // cuisine
          i++;
        } else if( i == 7 ) {
          // time
          $date = token;
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
          i++;
        }
      }
      outWriter.write( String.format( "%s%s%s%s%s%s%s%s" +
                      "%s%s%s%s%s%s%s%n",
              coordX, separator,
              coordY, separator,
              $date, separator,
              score, separator,
              coordX, separator,
              coordY, separator,
              $date, separator,
              score, separator ) );
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
  // TODO: check
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
    /*
    final String filepath = outDir + outFileName;
    final PrintWriter outWriter = new PrintWriter( filepath );

    final WKTReader reader = new WKTReader();
    final SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );

    for( String l : lines ) {
      final StringBuilder b = new StringBuilder();
      String building = null, street = null, zipcode = null, borough = null, cuisine = null,
              grade = null, name = null, restaurantId = null;
      int i = 0;
      final String[] tokens = l.split( separator );
      for ( String token : tokens ) {

        if ( i == 0 ) { // building
          b.append( token );
          b.append( separator );
          i++;

        } else if ( i == 1 ) { // coordX && coordY
          try {
            final Point p = (Point) reader.read( token );
            b.append( p.getX() );
            b.append( separator );
            b.append( p.getY() );
            b.append( separator );

          } catch ( ParseException e ) {
            // append two empty coordinates
            b.append( separator );
            b.append( separator );
          }
          i++;

        } else if ( i == 2 ) {
          try {
            final Date d = df.parse( token );
            b.append( d.getTime() );
            b.append( separator );

          } catch ( java.text.ParseException e ) {
            // append an empty timestamp
            b.append( separator );
          }
          i++;

        } else if ( i == 3 ) {
          b.append( token );
          b.append( separator );
          i++;
        }
      }

      b.append( String.format( "\n" ) );
      outWriter.write( b.toString() );
    }
    outWriter.close();
     */
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
      int i = 0;
      final String[] tokens = l.split( separator );
      for ( String token : tokens ) {
        /*
        if ( i == 0 ) { // coordX
          final double coordX = parseDouble( token );
          b.updateMinX( coordX );
          b.updateMaxX( coordX );
          i++;
        } else if ( i == 1 ) { // coordY
          final double coordY = parseDouble( token );
          b.updateMinY( coordY );
          b.updateMaxY( coordY );
          i++;
        } else if ( i == 2 ) { // $date
          final long $date = parseLong( token );
          b.updateMinT( $date );
          b.updateMaxT( $date );
          i++;
        } else if ( i == 3 ) { // score
          final int score = Integer.parseInt( token );
          b.updateMinScore( score );
          b.updateMaxScore( score );
          i++;
        }//*/

        if ( i == 0 ) { // building
          i++;

        } else if ( i == 1 ) { // coordX
          final double coordX = parseDouble( token );
          b.updateMinX( coordX );
          b.updateMaxX( coordX );
          i++;

        } else if ( i == 2 ) { // coordY
          final double coordY = parseDouble( token );
          b.updateMinY( coordY );
          b.updateMaxY( coordY );
          i++;

        } else if ( i == 3 ) { // street
          i++;

        } else if ( i == 4 ) { // zipcode
          i++;

        } else if ( i == 5 ) { // borough
          i++;

        } else if ( i == 6 ) { // cuisine
          i++;

        } else if ( i == 7 ) { // $date
          final long $date = parseLong( token );
          b.updateMinT( $date );
          b.updateMaxT( $date );
          i++;

        } else if ( i == 8 ) { // grade
          i++;

        } else if ( i == 9 ) { // score
          final int score = Integer.parseInt( token );
          b.updateMinScore( score );
          b.updateMaxScore( score );
          i++;

        } else if ( i == 10 ) { // name
          i++;

        } else if ( i == 11 ) { // restaurantId
          i++;

        }//*/
      }
    }
    return b;
  }

  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param separator
   * @return
   */

  public static List<RestaurantRecord> parseRecords(List<String> lines, String separator ){
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

  public static RestaurantRecord parseRecord(String line, String separator ) {
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
