package it.univr.restaurant.partitioning;

import it.univr.partitioning.FileUtils;
import it.univr.restaurant.RestaurantRecord;

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
public class QueryRestaurant
    extends QueryParamsRestaurant {
  
  public QueryRestaurant() { super(); }
  
  public QueryRestaurant( QueryParamsRestaurant paramsRestaurant ) { super( paramsRestaurant ); }
  
  public QueryRestaurant( QueryRestaurant queryRestaurant ) {
    super( queryRestaurant );
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
  ( QueryParamsRestaurant params,
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
          //found = checkSplit( l, separator );
          final RestaurantRecord r = DataUtils.parseRecord( l, separator );
          if( r.getCoordX() == null || r.getCoordY() == null ||
              r.getZipcode() == null || r.getTime() == null ||
              r.getScore() == null || r.getRestaurantId() == null ) {
            System.out.printf("Parse null values!!!");
          }
  
          boolean xfound = false;
          if( params.getMinX() != null &&
              params.getMaxX() != null &&
              r.getCoordX() >= params.getMinX() &&
              r.getCoordX() <= params.getMaxX() ) {
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
              r.getCoordY() >= params.getMinY() &&
              r.getCoordY() <= params.getMaxY() ) {
            yfound = true;
          } else if( params.getMinY() == null &&
              params.getMaxY() == null ) {
            yfound = true;
          } else {
            xfound = false;
          }
  
          boolean zfound = false;
          if( params.getMinZ() != null &&
              params.getMaxZ() != null &&
              r.getTime() >= params.getMinZ() &&
              r.getTime() <= params.getMaxZ() ) {
            zfound = true;
          } else if( params.getMinZ() == null &&
              params.getMaxZ() == null ) {
            zfound = true;
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
  
          boolean sfound = false;
          if( params.getMinS() != null &&
              params.getMaxS() != null &&
              r.getScore() >= params.getMinS() &&
              r.getScore() <= params.getMaxS() ) {
            sfound = true;
          } else if( params.getMinS() == null &&
              params.getMaxS() == null ) {
            sfound = true;
          } else {
            xfound = false;
          }
  
          boolean idfound = false;
          if( params.getMinId() != null &&
              params.getMaxId() != null &&
              r.getRestaurantId() >= params.getMinId() &&
              r.getRestaurantId() <= params.getMaxId() ) {
            idfound = true;
          } else if( params.getMinId() == null &&
              params.getMaxId() == null ) {
            idfound = true;
          } else {
            xfound = false;
          }
  
          found = xfound && yfound && zfound && tfound && sfound && idfound;
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
  
    final String minTString = minTime != null ? f.format( new Date( minT ) ) : "null";
    final String maxTString = maxTime != null ? f.format( new Date( maxT ) ) : "null";
  
    /*
    System.out.printf
        ( "Range query "
                + "t=[%s,%s], "
                + "x=[%.5f,%.5f], "
                + "y=[%.5f,%.5f], "
                + "z=[%d,%d], "
                + "s=[%d,%d], "
                + "id=[%d,%d]%n",
          
            minTString,
            maxTString,
            minX, maxX,
            minY, maxY,
            minZ, maxZ,
            minS, maxS,
            minId, maxId );//*/
    final StringBuilder sb = new StringBuilder();
    sb.append( "Range query " );
    sb.append( format( "t=[%s,%s], ", minTString, maxTString ) );
    sb.append( format( "x=[%.5f,%.5f], ", minX, maxX ) );
    sb.append( format( "y=[%.5f,%.5f], ", minY, maxY ) );
    sb.append( format( "z=[%d,%d], ", minZ, maxZ ) );
    sb.append( format( "s=[%d,%d], ", minS, maxS ) );
    sb.append( format( "id=[%d,%d]%n", minId, maxId ) );
    return sb.toString();
  }
}
