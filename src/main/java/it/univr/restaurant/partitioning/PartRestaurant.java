package it.univr.restaurant.partitioning;

import it.univr.partitioning.FileUtils;
import it.univr.restaurant.RestaurantRecord;

import java.io.*;
import java.util.*;

import static java.lang.Double.*;
import static java.lang.Double.max;
import static java.lang.String.format;

public class PartRestaurant {
  
  private PartRestaurant() {
    // nothing here
  }
  
  // === Methods ===============================================================
  
  /**
   * MISSING_COMMENT
   *
   * @param input
   * @param outputDir
   */
  // todo: remove duplicate
  public static void generateContextBasedParts
  ( File input,
    File outputDir,
    String separator,
    String partPrefix,
    int splitSize,
    double[] timeSplits,
    double[] xSplits,
    double[] ySplits,
    double[] zSplits,
    double[] idSplits,
    double[] sSplits )
      throws IOException {
    
    if( input == null ) {
      throw new NullPointerException();
    }
    if( outputDir == null ) {
      throw new NullPointerException();
    }
    
    if( !input.exists() || !input.isFile() ) {
      throw new IllegalArgumentException
          ( format( "The input \"%s\" is not a file.", input.getAbsolutePath() ) );
    }
    
    if( !outputDir.exists() || !outputDir.isDirectory() ) {
      outputDir.mkdirs();
    }
    
    final List<String> lines = FileUtils.readLines( input, false );
    final List<RestaurantRecord> records = DataUtils.parseRecords( lines, separator );
    final String partTemplate = "%s%s-%s-%s-%s-%s-%s";
    final Map<String,List<RestaurantRecord>> result = new HashMap<>();
  
    for( RestaurantRecord r : records ) {
      int t = -1;
      int i = 1;
      while( i < timeSplits.length && t == - 1 ){
        if( r.getTime() >= timeSplits[i-1] && r.getTime() <= timeSplits[i] ){
          t = i-1;
        }
        i++;
      }
      if( t == -1 ){
        throw new IllegalStateException();
      }
    
      int x = -1;
      i = 1;
      while( i < xSplits.length && x == - 1 ){
        if( r.getCoordX() >= xSplits[i-1] && r.getCoordX() <= xSplits[i] ){
          x = i-1;
        }
        i++;
      }
      if( x == -1 ){
        throw new IllegalStateException();
      }
    
      int y = -1;
      i = 1;
      while( i < ySplits.length && y == - 1 ){
        if( r.getCoordY() >= ySplits[i-1] && r.getCoordY() <= ySplits[i] ){
          y = i-1;
        }
        i++;
      }
      if( y == -1 ){
        throw new IllegalStateException();
      }
    
      int z = -1;
      i = 1;
      while( i < zSplits.length && z == - 1 ){
        if( r.getZipcode() >= zSplits[i-1] && r.getZipcode() <= zSplits[i] ){
          z = i-1;
        }
        i++;
      }
      if( z == -1 ){
        throw new IllegalStateException();
      }
    
      int s = -1;
      i = 1;
      while( i < sSplits.length && s == - 1 ){
        if( r.getScore() >= sSplits[i-1] && r.getScore() <= sSplits[i] ){
          s = i-1;
        }
        i++;
      }
      if( s == -1 ){
        throw new IllegalStateException();
      }
    
      int id = -1;
      i = 1;
      while( i < idSplits.length && id == - 1 ){
        if( r.getRestaurantId() >= idSplits[i-1] && r.getRestaurantId() <= idSplits[i] ){
          id = i-1;
        }
        i++;
      }
      if( id == -1 ){
        throw new IllegalStateException();
      }
    
      final String key = format( partTemplate, partPrefix, t, x, y, z, s, id );
      List<RestaurantRecord> list = result.get( key );
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add( r );
      result.put( key, list );
    }
  
    // --- write the final index -----------------------------------------------
  
    for( String k : result.keySet() ) {
      try(  BufferedWriter bw = new BufferedWriter( new FileWriter( new File( outputDir, k ) ) ) ) {
        for( RestaurantRecord r : result.get(k )) {
          bw.write(format("%s%n", r.toString( separator )));
        }
      }
    }
    
    // --- check split dimensions ----------------------------------------------

    /*final File[] partitions = outputDir.listFiles();
    for( File p : partitions ) {
      if( p.length() > splitSize ) {
        // number of splits
        int numSplits = (int)
                ( ( p.length() / splitSize ) +
                        ( ( p.length() % splitSize > 0 ) ? 1 : 0 ) );
        int m = numSplits;
        int d = 0;
        while( m >  0 ){
          m = numSplits / 10;
          d += 1;
        }
        final String splitTemplate = format( "%%s-%%0%sd", d );
        try( BufferedReader br = new BufferedReader( new FileReader( p ) ) ) {
          long size = 0L;
          int i = 0;
          BufferedWriter bw = new BufferedWriter
                  ( new FileWriter
                          ( new File
                                  ( outputDir, format( splitTemplate, p.getName(), i ) ) ) );
          String line;
          while( ( line = br.readLine() ) != null ){
            final String value = String.format( "%s%n", line );
            if( size + value.getBytes().length > splitSize ){
              bw.close();
              bw = new BufferedWriter
                      ( new FileWriter
                              ( new File
                                      ( outputDir, format( splitTemplate, p.getName(), ++i ) ) ) );
              size = 0L;
            }
            bw.write( value );
            size += value.getBytes().length;
          }
          bw.close();
        }
        p.delete();
      }
    }//*/
  }
  
  public static void writeContextBasedParts
  ( List<String> lines,
    File outputDir,
    String separator,
    String partPrefix,
    double[] timeSplits,
    double[] xSplits,
    double[] ySplits,
    double[] zSplits,
    double[] idSplits,
    double[] sSplits )
      throws IOException {
  
    if( lines == null ) {
      throw new NullPointerException();
    }
    if( partPrefix == null ) {
      throw new NullPointerException();
    }
    
    final List<RestaurantRecord> records = DataUtils.parseRecords( lines, separator );
    final String partTemplate = "%s%s-%s-%s-%s-%s-%s";
    final Map<String,List<RestaurantRecord>> result = new HashMap<>();
  
    for( RestaurantRecord r : records ) {
      int t = -1;
      int i = 1;
      while( i < timeSplits.length && t == - 1 ){
        if( r.getTime() >= timeSplits[i-1] && r.getTime() <= timeSplits[i] ){
          t = i-1;
        }
        i++;
      }
      if( t == -1 ){
        throw new IllegalStateException();
      }
    
      int x = -1;
      i = 1;
      while( i < xSplits.length && x == - 1 ){
        if( r.getCoordX() >= xSplits[i-1] && r.getCoordX() <= xSplits[i] ){
          x = i-1;
        }
        i++;
      }
      if( x == -1 ){
        throw new IllegalStateException();
      }
    
      int y = -1;
      i = 1;
      while( i < ySplits.length && y == - 1 ){
        if( r.getCoordY() >= ySplits[i-1] && r.getCoordY() <= ySplits[i] ){
          y = i-1;
        }
        i++;
      }
      if( y == -1 ){
        throw new IllegalStateException();
      }
    
      int z = -1;
      i = 1;
      while( i < zSplits.length && z == - 1 ){
        if( r.getZipcode() >= zSplits[i-1] && r.getZipcode() <= zSplits[i] ){
          z = i-1;
        }
        i++;
      }
      if( z == -1 ){
        throw new IllegalStateException();
      }
    
      int s = -1;
      i = 1;
      while( i < sSplits.length && s == - 1 ){
        if( r.getScore() >= sSplits[i-1] && r.getScore() <= sSplits[i] ){
          s = i-1;
        }
        i++;
      }
      if( s == -1 ){
        throw new IllegalStateException();
      }
    
      int id = -1;
      i = 1;
      while( i < idSplits.length && id == - 1 ){
        if( r.getRestaurantId() >= idSplits[i-1] && r.getRestaurantId() <= idSplits[i] ){
          id = i-1;
        }
        i++;
      }
      if( id == -1 ){
        throw new IllegalStateException();
      }
    
      final String key = format( partTemplate, partPrefix, t, x, y, z, s, id );
      List<RestaurantRecord> list = result.get( key );
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add( r );
      result.put( key, list );
    }
  
    // --- write the final index -----------------------------------------------
  
    for( String k : result.keySet() ) {
      try(  BufferedWriter bw = new BufferedWriter( new FileWriter( new File( outputDir, k ) ) ) ) {
        for( RestaurantRecord r : result.get(k )) {
          bw.write(format("%s%n", r.toString( separator )));
        }
      }
    }
  }
  
  public static Set<String> getMultiDimGridPartSet
  ( File input,
    File outFile,
    int numCellPerSide,
    String partPrefix,
    RestaurantBoundaries b,
    String separator )
      throws IOException {
  
    if( partPrefix == null ) {
      throw new NullPointerException();
    }
    if( b == null ) {
      throw new NullPointerException();
    }
    
    final double widthSidePartX = ( b.getMaxX() - b.getMinX() ) / numCellPerSide;
    final double widthSidePartY = ( b.getMaxY() - b.getMinY() ) / numCellPerSide;
    final double widthSidePartZ = ( (double) ( b.getMaxZipcode() - b.getMinZipcode() ) ) / numCellPerSide;
    final double widthSidePartT = ( (double) ( b.getMaxT() - b.getMinT() ) ) / numCellPerSide;
    final double widthSidePartS = ( (double) ( b.getMaxScore() - b.getMinScore() ) ) / numCellPerSide;
    final double widthSidePartId = ( (double) ( b.getMaxId() - b.getMinId() ) ) / numCellPerSide;
  
    final String xFormat = String.format( "%%0%sd", numCellPerSide == 0 ? 1 : numCellPerSide );
    final String yFormat = String.format( "%%0%sd", numCellPerSide == 0 ? 1 : numCellPerSide );
    final String zFormat = String.format( "%%0%sd", numCellPerSide == 0 ? 1 : numCellPerSide );
    final String tFormat = String.format( "%%0%sd", numCellPerSide == 0 ? 1 : numCellPerSide );
    final String sFormat = String.format( "%%0%sd", numCellPerSide == 0 ? 1 : numCellPerSide );
    final String idFormat = String.format( "%%0%sd", numCellPerSide == 0 ? 1 : numCellPerSide );
  
    final String partTemplate = partPrefix +
        xFormat + "-" + yFormat + "-" + zFormat + "-" +
        tFormat + "-" + sFormat + "-" + idFormat;
  
    final Set<String> keySet = new HashSet<String>();
    
    
    try( BufferedReader br = new BufferedReader( new FileReader( input ) ) ) {
      try( BufferedWriter wr = new BufferedWriter( new FileWriter( outFile ) ) ) {
      
        String line;
        while( ( line = br.readLine() ) != null ) {
          final RestaurantRecord r = DataUtils.parseRecord( line, separator );
        
          // the cell index is given by the integer part except for the max boundary
          final int xPart;
          if( r.getCoordX() == b.getMaxX() ) {
            xPart = numCellPerSide - 1;
          } else {
            xPart = (int) ( ( r.getCoordX() - b.getMinX() ) / widthSidePartX );
          }
        
          // the cell index is given by the integer part except for the max boundary
          final int yPart;
          if( r.getCoordY() == b.getMaxY() ) {
            yPart = numCellPerSide - 1;
          } else {
            yPart = (int) ( ( r.getCoordY() - b.getMinY() ) / widthSidePartY );
          }
        
          // the cell index is given by the integer part except for the max boundary
          final int zPart;
        
          if( r.getZipcode() == b.getMaxZipcode() ) {
            zPart = numCellPerSide - 1;
          } else {
            zPart = (int) ( ( r.getZipcode() - b.getMinZipcode() ) / widthSidePartZ );
          }
        
          // the cell index is given by the integer part except for the max boundary
          final int tPart;
          if( r.getTime() == b.getMaxT() ) {
            tPart = numCellPerSide - 1;
          } else {
            tPart = (int) ( ( r.getTime() - b.getMinT() ) / widthSidePartT );
          }
        
          // the cell index is given by the integer part except for the max boundary
          final int sPart;
        
          if( r.getScore() == b.getMaxScore() ) {
            sPart = numCellPerSide - 1;
          } else {
            sPart = (int) ( ( r.getScore() - b.getMinScore() ) / widthSidePartS );
          }
        
          // the cell index is given by the integer part except for the max boundary
          final int idPart;
        
          if( r.getRestaurantId() == b.getMaxId() ) {
            idPart = numCellPerSide - 1;
          } else {
            idPart = (int) ( ( r.getRestaurantId() - b.getMinId() ) / widthSidePartId );
          }
        
          final String key = format(
              partTemplate, xPart, yPart, zPart, tPart, sPart, idPart );
          keySet.add( key );
        
          wr.write( format( "%s,%s%n", key, line ) );
        }
      }
    }
    return keySet;
  }
  
  
  public static void writeMultiLevelGridParts
  ( List<String> lines,
    File outputDir,
    int numCellPerSide,
    String partPrefix,
    RestaurantBoundaries b,
    String separator )
      throws IOException {
  
    if( lines == null ) {
      throw new NullPointerException();
    }
    if( outputDir == null ) {
      throw new NullPointerException();
    }
    if( b == null ) {
      throw new NullPointerException();
    }
    
    final String xFormat = String.format( "%%0%sd", numCellPerSide == 0? 1: numCellPerSide);
    final String yFormat = String.format( "%%0%sd", numCellPerSide == 0? 1: numCellPerSide);
    final String zFormat = String.format( "%%0%sd", numCellPerSide == 0? 1: numCellPerSide);
    final String tFormat = String.format( "%%0%sd", numCellPerSide == 0? 1: numCellPerSide);
    final String sFormat = String.format( "%%0%sd", numCellPerSide == 0? 1: numCellPerSide);
    final String idFormat = String.format( "%%0%sd", numCellPerSide == 0? 1: numCellPerSide);
    
    
    final List<RestaurantRecord> records = DataUtils.parseRecords( lines, separator );
  
    // first level => $date
    final double widthSidePartT = ( (double) ( b.getMaxT() - b.getMinT() ) ) / numCellPerSide;
    final String tPartTemplate = partPrefix + tFormat;
    final Map<String,List<RestaurantRecord>> tParts = new HashMap<>();
  
    for( RestaurantRecord r : records ){
      if( r.getTime() != null ) {
        // the cell index is given by the integer part except for the max boundary
        final int tPart;
        if( r.getTime() == b.getMaxT() ){
          tPart = numCellPerSide - 1;
        } else {
          tPart = (int) ( ( r.getTime() - b.getMinT() ) / widthSidePartT );
        }
        List<RestaurantRecord> elements = tParts.get( format( tPartTemplate, tPart ) );
        if( elements == null ){
          elements = new ArrayList<>();
        }
        elements.add(r);
        tParts.put( format( tPartTemplate, tPart ), elements );
      }
    }
  
    // second level => coordX
    final String xPartTemplate =  "%s-" + xFormat;
    final Map<String,List<RestaurantRecord>> xParts = new HashMap<>();
  
    for( String k : tParts.keySet() ) {
      double minX = MAX_VALUE;
      double maxX = MIN_VALUE;
    
      // find the x-boundary of the t-split
      for( RestaurantRecord r : tParts.get(k) ) {
        minX = min( minX, r.getCoordX() );
        maxX = max( maxX, r.getCoordX() );
      }//*/
    
      //double maxX = boundaries.getMaxX();
      //double minX = boundaries.getMinX();
    
      final double widthSidePartX = ( maxX - minX ) / numCellPerSide;
    
      for( RestaurantRecord r : tParts.get(k) ) {
        // for each temporal split
        if( r.getCoordX() != null ) {
          // the cell index is given by the integer part except for the max boundary
          final int xPart;
          //if( r.getX().equals( boundaries.getMaxX() )){
          if( r.getCoordX() == maxX ){
            xPart = numCellPerSide - 1;
          } else {
            xPart = (int) ( ( r.getCoordX() - minX ) / widthSidePartX );
          }
        
          List<RestaurantRecord> elements = xParts.get( format( xPartTemplate, k, xPart ) );
          if( elements == null ) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          xParts.put( format( xPartTemplate, k, xPart ), elements );
        }
      }
    }
  
    // third level => coordY
    final String yPartTemplate =  "%s-" + yFormat;
    final Map<String,List<RestaurantRecord>> yParts = new HashMap<>();
  
    for( String k : xParts.keySet() ) {
      double minY = MAX_VALUE;
      double maxY = MIN_VALUE;
    
      // find the x-boundary of the t-split
      for( RestaurantRecord r : xParts.get(k) ) {
        minY = min( minY, r.getCoordY() );
        maxY = max( maxY, r.getCoordY() );
      }//*/
    
      //final double minY = boundaries.getMinY();
      //final double maxY = boundaries.getMaxY();
    
      final double widthSidePartY = ( maxY - minY ) / numCellPerSide;
    
      for( RestaurantRecord r : xParts.get( k )) {
        // for each temporal split
        if( r.getCoordY() != null ) {
          // int yPart = (int) ( ( r.getY() - minY) / widthSidePartY );
          // the cell index is given by the integer part except for the max boundary
          final int yPart;
          //if( r.getY().equals( boundaries.getMaxY() ) ) {
          if( r.getCoordY() == maxY ) {
            yPart = numCellPerSide - 1;
          } else {
            yPart = (int) ( ( r.getCoordY() - minY ) / widthSidePartY );
          }
        
          List<RestaurantRecord> elements = yParts.get( format( yPartTemplate, k, yPart ) );
          if( elements == null ) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          yParts.put( format( yPartTemplate, k, yPart ), elements );
        }
      }
    }
  
    // fourth level => score
    final String sPartTemplate =  "%s-" + sFormat;
    final Map<String,List<RestaurantRecord>> sParts = new HashMap<>();
  
    for( String k : yParts.keySet() ) {
      double minS = MAX_VALUE;
      double maxS = MIN_VALUE;
    
      // find the x-boundary of the t-split
      for( RestaurantRecord r : yParts.get(k) ) {
        minS = min( minS, r.getScore() );
        maxS = max( maxS, r.getScore() );
      }//*/
    
      //final double minA = boundaries.getMinAge();
      //final double maxA = boundaries.getMaxAge();
    
      final double widthSidePartS = ( maxS - minS ) / numCellPerSide;
    
      for( RestaurantRecord r : yParts.get(k) ) {
        // for each temporal split
        if( r.getScore() != null ) {
          //int sPart = (int) ( ( r.getScore() - minS ) / widthSidePartS );
          // the cell index is given by the integer part except for the max boundary
          final int sPart;
          //if( r.getAge().equals( boundaries.getMaxAge() )){
          if( r.getScore() == maxS ){
            sPart = numCellPerSide - 1;
          } else {
            sPart = (int) ( ( r.getScore() - minS ) / widthSidePartS );
          }
        
          List<RestaurantRecord> elements = sParts.get( format( sPartTemplate, k, sPart ) );
          if( elements == null ) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          sParts.put( format( sPartTemplate, k, sPart ), elements );
        }
      }
    }
  
    // fifth level => zipcode
    final String zPartTemplate =  "%s-" + zFormat;
    final Map<String,List<RestaurantRecord>> zParts = new HashMap<>();
  
    for( String k : sParts.keySet() ) {
      double minZ = MAX_VALUE;
      double maxZ = MIN_VALUE;
    
      // find the x-boundary of the t-split
      for( RestaurantRecord r : sParts.get(k) ) {
        minZ = min( minZ, r.getZipcode() );
        maxZ = max( maxZ, r.getZipcode() );
      }//*/
    
      //final double minZ = boundaries.getMinZ();
      //final double maxZ = boundaries.getMaxZ();
    
      final double widthSidePartZ = ( maxZ - minZ ) / numCellPerSide;
    
      for( RestaurantRecord r : sParts.get( k ) ) {
        // for each temporal split
        if( r.getZipcode() != null ) {
          //int zPart = (int) ( (r.getZipcode() - minZ ) / widthSidePartY);
          // the cell index is given by the integer part except for the max boundary
          final int zPart;
          //if( r.getZipcode().equals( boundaries.getMaxZ() ) ){
          if( r.getZipcode() == maxZ ){
            zPart = numCellPerSide - 1;
          } else {
            zPart = (int) ( ( r.getZipcode() - minZ ) / widthSidePartZ );
          }
        
          List<RestaurantRecord> elements = zParts.get( format( zPartTemplate, k, zPart ) );
          if( elements == null ) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          zParts.put( format( zPartTemplate, k, zPart ), elements );
        }
      }
    }
  
    // sixth level => restaurantId
    final String idPartTemplate =  "%s-" + idFormat;
    final Map<String,List<RestaurantRecord>> idParts = new HashMap<>();
  
    for( String k : zParts.keySet() ) {
      double minId = MAX_VALUE;
      double maxId = MIN_VALUE;
    
      // find the x-boundary of the t-split
      for( RestaurantRecord r : zParts.get(k) ) {
        minId = min( minId, r.getRestaurantId() );
        maxId = max( maxId, r.getRestaurantId() );
      }//*/
    
      //final double minId = boundaries.getMinId();
      //final double maxId = boundaries.getMaxId();
    
      final double widthSidePartId = ( maxId - minId ) / numCellPerSide;
    
      for( RestaurantRecord r : zParts.get(k) ) {
        // for each temporal split
        if( r.getRestaurantId() != null ) {
          //int idPart = (int) ( ( r.getRestaurantId() - minId ) / widthSidePartId );
          // the cell index is given by the integer part except for the max boundary
          final int idPart;
          //if( r.getRestaurantId().equals( boundaries.getMaxId() ) ) {
          if( r.getRestaurantId() == maxId ) {
            idPart = numCellPerSide - 1;
          } else {
            idPart = (int) ( ( r.getRestaurantId() - minId ) / widthSidePartId );
          }
        
          List<RestaurantRecord> elements = idParts.get( format( idPartTemplate, k, idPart ) );
          if( elements == null ) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          idParts.put( format( idPartTemplate, k, idPart ), elements );
        }
      }
    }
  
    // --- write the final index -----------------------------------------------
  
    for( String k : idParts.keySet() ){
      try( BufferedWriter bw = new BufferedWriter( new FileWriter( new File( outputDir, k ) ) ) ) {
        for( RestaurantRecord r : idParts.get(k) ) {
          bw.write( format( "%s%n", r.toString( separator ) ) );
        }
      }
    }
  }
}
