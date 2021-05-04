package it.univr.veronacard.partitioning;

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

public class StatsVeronaCard {
  
  private StatsVeronaCard() {
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
    final VeronaCardBoundaries boundaries = DataUtils.computeGlobalBoundaries( partDirectory, separator );
    
    final Map<String, Long> rows = countFileRows( partDirectory, partPrefix, numDimensions, countSplits );
    final Map<String, Double[]> rsd = computeRsd( partDirectory, separator, partPrefix,
            boundaries, numDimensions, countSplits );
    
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
    VeronaCardBoundaries b,
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
}
