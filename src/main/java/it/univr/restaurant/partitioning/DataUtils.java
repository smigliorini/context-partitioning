package it.univr.restaurant.partitioning;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import it.univr.restaurant.partitioning.Boundaries;
import it.univr.restaurant.RestaurantRecord;

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

  private DataUtils() {
    // nothing here
  }

  // === Methods ===============================================================

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

    final StringTokenizer tk = new StringTokenizer( line, separator );
    int i = 0;

    // new version of the input file!
    while( tk.hasMoreTokens() ) {
      final String token = tk.nextToken();
      switch (i) {
        case 0: // coordX
          try {
            r.setCoordX(parseDouble(token));
          } catch (NumberFormatException e) {
            r.setCoordX(null);
          }
          i++;
          break;
        case 1: // coordY
          try {
            r.setCoordY(parseDouble(token));
          } catch (NumberFormatException e) {
            r.setCoordY(null);
          }
          i++;
          break;
        case 2: // $date
          try {
            r.set$date(Long.parseLong(token));
          } catch (NumberFormatException e) {
            r.set$date(null);
          }
          i++;
          break;
        case 3: // grade
          try {
            r.setGrade(token);
          } catch (NumberFormatException e) {
            r.setGrade(null);
          }
          i++;
          break;
        case 4: // score
          try {
            r.setScore(Integer.parseInt(token));
          } catch (NumberFormatException e) {
            r.setScore(null);
          }
          i++;
          break;
      }
    }
    return r;
  }
}
