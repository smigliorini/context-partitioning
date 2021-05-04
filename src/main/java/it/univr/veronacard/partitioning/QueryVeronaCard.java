package it.univr.veronacard.partitioning;

import it.univr.partitioning.FileUtils;
import it.univr.veronacard.VeronaCardRecord;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class QueryVeronaCard 
    extends QueryParamsVeronaCard {
  
  public QueryVeronaCard() { super(); }
  
  public QueryVeronaCard( QueryParamsVeronaCard paramsVeronaCard ) { super( paramsVeronaCard ); }
  
  public QueryVeronaCard( QueryVeronaCard queryVeronaCard ) {
    super( queryVeronaCard );
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
  ( QueryParamsVeronaCard params,
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
      if( f.getName().startsWith( partPrefix ) ) {
      final List<String> lines = FileUtils.readLines( f, false );

      final Iterator<String> it = lines.iterator();
      boolean found = false;

      while( it.hasNext() && !found ) {
        final String l = it.next();

        final VeronaCardRecord r = DataUtils.parseRecord( l, separator );
        if (r.getX() == null || r.getY() == null || r.getTime() == null || r.getAge() == null) {
          System.out.printf("Parse null values!!!");
        }

        boolean xfound = false;
        if( params.getMinX() != null &&
                params.getMaxX() != null &&
                r.getX() >= params.getMinX() &&
                r.getX() <= params.getMaxX() ) {
          xfound = true;
        } else if( params.getMinX() == null &&
                params.getMaxX() == null ) {
          xfound = true;
        } else {
          xfound = false;
        }

        boolean yfound = false;
        if( params.getMinY() != null &&
                params.getMaxY() != null &&
                r.getY() >= params.getMinY() &&
                r.getY() <= params.getMaxY() ) {
          yfound = true;
        } else if( params.getMinY() == null &&
                params.getMaxY() == null ) {
          yfound = true;
        } else {
          xfound = false;
        }

        boolean tfound = false;
        if( params.getMinT() != null &&
                params.getMaxT() != null &&
                r.getTime() >= params.getMinT() &&
                r.getTime() <= params.getMaxT() ) {
          tfound = true;
        } else if( params.getMinT() == null &&
                params.getMaxT() == null ) {
          tfound = true;
        } else {
          xfound = false;
        }

        boolean afound = false;
        if( params.getMinAge() != null &&
                params.getMaxAge() != null &&
                r.getAge() >= params.getMinAge() &&
                r.getAge() <= params.getMaxAge() ) {
          afound = true;
        } else if( params.getMinAge() == null &&
                params.getMaxAge() == null ) {
          afound = true;
        } else {
          xfound = false;
        }

        found = xfound && yfound && tfound && afound;
        if( found ) {
          splits.add( f.getName() );
        }
      }
      }
    }

    return splits;
  }
  
  
  public String print( String minTime, String maxTime ) {
    final SimpleDateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:ss" );
    
    //final String minTString = minT != null ? f.format( new Date( params.getMinT() ) ) : "null";
    //final String maxTString = maxT != null ? f.format( new Date( params.getMaxT() ) ) : "null";
    final String minTString = minTime != null ? f.format( new Date( minT ) ) : "null";
    final String maxTString = maxTime != null ? f.format( new Date( maxT ) ) : "null";
    
    /*
    System.out.printf
        ( "Range query "
                + "t=[%s,%s], "
                + "x=[%.5f,%.5f], "
                + "y=[%.5f,%.5f], "
                + "age=[%d,%d]%n",
            minTString,
            maxTString,
            //params.getMinX(), params.getMaxX(),
            //params.getMinY(), params.getMaxY(),
            //params.getMinAge(), params.getMaxAge() );
            minX, maxX,
            minY, maxY,
            minAge, maxAge );//*/
    final StringBuilder sb = new StringBuilder();
    sb.append( "Range query " );
    sb.append( format( "t=[%s,%s], ", minTString, maxTString ) );
    sb.append( format( "x=[%.5f,%.5f], ", minX, maxX ) );
    sb.append( format( "y=[%.5f,%.5f], ", minY, maxY ) );
    sb.append( format( "age=[%d,%d]%n", minAge, maxAge ) );
    return sb.toString();
  }
}
