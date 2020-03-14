package it.univr.partitioning;

import it.univr.veronacard.VeronaCard;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class QueryUtils {

  private QueryUtils() {
    // nothing here
  }

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param params
   * @param indexDirectory
   * @param separator
   * @param partPrefix
   * @return
   */

  public static Set<String> rangeContextQuery
  ( QueryParams params,
    File indexDirectory,
    String separator,
    String partPrefix ) {

    if( params == null ) {
      throw new NullPointerException();
    }
    if( indexDirectory == null ) {
      throw new NullPointerException();
    }
    if( partPrefix == null ){
      throw new NullPointerException();
    }

    if( !indexDirectory.exists() || !indexDirectory.isDirectory() ) {
      throw new IllegalArgumentException
        ( format( "\"%s\" is not a valid directory.", indexDirectory.getName() ) );
    }

    final Set<String> splits = new HashSet<>();

    for( File f : indexDirectory.listFiles() ) {
      if( f.getName().startsWith( partPrefix )){
      final List<String> lines = FileUtils.readLines( f, false );

      final Iterator<String> it = lines.iterator();
      boolean found = false;

      while( it.hasNext() && !found ) {
        final String l = it.next();

        final VeronaCard r = DataUtils.parseRecord(l, separator);
        if (r.getX() == null || r.getY() == null || r.getTime() == null || r.getAge() == null) {
          System.out.printf("Parse null values!!!");
        }

        boolean xfound = false;
        if (params.getMinX() != null &&
                params.getMaxX() != null &&
                r.getX() >= params.getMinX() &&
                r.getX() <= params.getMaxX()) {
          xfound = true;
        } else if (params.getMinX() == null &&
                params.getMaxX() == null) {
          xfound = true;
        } else {
          xfound = false;
        }

        boolean yfound = false;
        if (params.getMinY() != null &&
                params.getMaxY() != null &&
                r.getY() >= params.getMinY() &&
                r.getY() <= params.getMaxY()) {
          yfound = true;
        } else if (params.getMinY() == null &&
                params.getMaxY() == null) {
          yfound = true;
        } else {
          xfound = false;
        }

        boolean tfound = false;
        if (params.getMinT() != null &&
                params.getMaxT() != null &&
                r.getTime() >= params.getMinT() &&
                r.getTime() <= params.getMaxT()) {
          tfound = true;
        } else if (params.getMinT() == null &&
                params.getMaxT() == null) {
          tfound = true;
        } else {
          xfound = false;
        }

        boolean afound = false;
        if (params.getMinAge() != null &&
                params.getMaxAge() != null &&
                r.getAge() >= params.getMinAge() &&
                r.getAge() <= params.getMaxAge()) {
          afound = true;
        } else if (params.getMinAge() == null &&
                params.getMaxAge() == null) {
          afound = true;
        } else {
          xfound = false;
        }

        found = xfound && yfound && tfound && afound;
        if (found) {
          splits.add(f.getName());
        }
      }
      }
    }

    return splits;
  }
}
