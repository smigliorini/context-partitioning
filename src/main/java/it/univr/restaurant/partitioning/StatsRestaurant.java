package it.univr.restaurant.partitioning;

import it.univr.partitioning.StatsUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static it.univr.partitioning.FileUtils.readLines;
import static it.univr.partitioning.StatsUtils.*;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;


public class StatsRestaurant {
  
  private StatsRestaurant() {
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
    
    //final Boundaries b = computeGlobalBoundaries( partDirectory, separator );
    final RestaurantBoundaries boundaries =  DataUtils.computeGlobalBoundaries( partDirectory, separator );
    
    
    final Map<String, Long> rows = countFileRows( partDirectory, partPrefix, numDimensions, countSplits );
    final Map<String, Double[]> rsd = computeRsd( partDirectory, separator, partPrefix,
            boundaries, numDimensions, countSplits );
    
    try( BufferedWriter bw = new BufferedWriter( new FileWriter( outputFile ) ) ) {
      
      bw.write( String.format( "Split%s"
              + "NumRows%s"
              + "RSD coordX%s"
              + "RSD coordY%s"
              + "RSD zipcode%s"
              + "RSD $date%s"
              + "RSD score%s"
              + "RSD ID%s"
              + "%n",
          separator, separator,
          separator, separator,
          separator, separator,
          separator, separator,
          separator, separator,
          separator, separator
      ) );
      
      final List<String> keys = new ArrayList<>( rows.keySet() );
      Collections.sort( keys );
      
      for( String k : keys ) {
        bw.write( String.format( "%s%s"
                + "%s%s"
                + "%s%s"
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
            rsd.get( k )[3], separator,
            rsd.get( k )[4], separator,
            rsd.get( k )[5], separator
        ) );
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
   * @param separator
   * @param partPrefix
   * @param b
   * @param numDimensions
   * @param countSplits
   * @return
   */
  
  private static Map<String, Double[]> computeRsd
  ( File directory,
    String separator,
    String partPrefix,
    RestaurantBoundaries b,
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
    final HashMap<String, List<Double>> zValueMap = new HashMap<>();
    final HashMap<String, List<Double>> tValueMap = new HashMap<>();
    final HashMap<String, List<Double>> sValueMap = new HashMap<>();
    final HashMap<String, List<Double>> idValueMap = new HashMap<>();
    
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
      List<Double> zValues = xValueMap.get( mf );
      if( zValues == null ){
        zValues = new ArrayList<>();
      }
      List<Double> tValues = xValueMap.get( mf );
      if( tValues == null ){
        tValues = new ArrayList<>();
      }
      List<Double> sValues = xValueMap.get( mf );
      if( sValues == null ){
        sValues = new ArrayList<>();
      }
      List<Double> idValues = xValueMap.get( mf );
      if( idValues == null ){
        idValues = new ArrayList<>();
      }
      
      for( String l : lines ) {
        
        int i = 0;
        final String[] tokens = l.split( separator );
        for ( String token : tokens ) {
          switch( i ) {
            case 1: // coordX
              final double valueX = parseDouble( token );
              xValues.add( normalize( b.getMinX(), b.getMaxX(), valueX ) );
              i++;
              break;
            case 2: // coordY
              final double valueY = parseDouble( token );
              yValues.add( normalize( b.getMinY(), b.getMaxY(), valueY ) );
              i++;
              break;
            case 4: // zipcode
              // TODO: NumberFormatException
              //final double valueZ = (double) parseInt( token );
              double valueZ;
              try {
                valueZ = (double) parseInt( token );
              } catch( NumberFormatException e ) {
                valueZ = (double) parseDouble( token );
              }
              zValues.add( normalize( b.getMinZipcode(), b.getMaxZipcode(), valueZ ) );
              i++;
              break;
            case 7: // $date
              //final double valueT = (double) parseLong( token );
              double valueT;
              try {
                valueT = (double) parseLong( token );
              } catch( NumberFormatException e ) {
                valueT = (double) parseDouble( token );
              }
              tValues.add( normalize( b.getMinT(), b.getMaxT(), valueT ));
              i++;
              break;
            case 9: // score
              //final double valueS = (double) parseInt( token );
              double valueS;
              try {
                valueS = (double) parseInt( token );
              } catch( NumberFormatException e ) {
                valueS = (double) parseDouble( token );
              }
              sValues.add( normalize( b.getMinScore(), b.getMaxScore(), valueS ));
              i++;
              break;
            case 11: // restaurantId
              //final double valueId = (double) parseInt( token );
              double valueId;
              try {
                valueId = (double) parseInt( token );
              } catch( NumberFormatException e ) {
                valueId = (double) parseDouble( token );
              }
              idValues.add( normalize( b.getMinId(), b.getMaxId(), valueId ));
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
      }
      xValueMap.put( mf, xValues );
      yValueMap.put( mf, yValues );
      zValueMap.put( mf, zValues );
      tValueMap.put( mf, tValues );
      sValueMap.put( mf, sValues );
      idValueMap.put( mf, idValues );
    }
    
    for( String f : xValueMap.keySet() ){
      final double xRsd = rsd( xValueMap.get( f ) );
      final double yRsd = rsd( yValueMap.get( f ) );
      final double zRsd = rsd( zValueMap.get( f ) );
      final double tRsd = rsd( tValueMap.get( f ) );
      final double sRsd = rsd( sValueMap.get( f ) );
      final double idRsd = rsd( idValueMap.get( f ) );
      
      result.put( f, new Double[]{xRsd, yRsd, zRsd, tRsd, sRsd, idRsd} );
    }
    
    return result;
  }
  
}
