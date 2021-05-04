package it.univr.restaurant.partitioning;

import it.univr.partitioning.BoundariesData;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

public class RestaurantBoundaries
    extends RestaurantBoundariesRecord
    implements BoundariesData {
  
  final double[] timeSplits = new double[] {
  };
  
  final double[] xSplits = new double[] {
  };
  
  final double[] ySplits = new double[] {
  };
  
  final double[] zSplits = new double[] {
  };
  
  final double[] sSplits = new double[] {
  };
  
  final double[] idSplits = new double[] {
  };
  
  public static String SPLITERATOR = ",";
  
  public RestaurantBoundaries() { super(); }
  
  public RestaurantBoundaries( RestaurantBoundariesRecord boundariesRecord ) {
    super( boundariesRecord );
  }
  
  public RestaurantBoundaries( RestaurantBoundaries restaurantBoundaries ) { super( restaurantBoundaries ); }
  
  
  @Override
  public void computeContextBasedParts( List<String> lines, File outputDir,
                                        String separator, String partPrefix ) throws IOException {
    PartRestaurant.writeContextBasedParts( lines, outputDir, separator, partPrefix,
        timeSplits, xSplits, ySplits, zSplits, idSplits, sSplits );
  }
  
  @Override
  public Set<String> computeMultiDimGridParts( BoundariesData boundariesData, File input, File outputFile,
                                               int numCellPerSide, String partPrefix ) throws IOException {
    return PartRestaurant.getMultiDimGridPartSet( input, outputFile, numCellPerSide, partPrefix,
        (RestaurantBoundaries) boundariesData, SPLITERATOR );
  }
  
  @Override
  public void computeMultiLevelGridParts( BoundariesData boundariesData, List<String> lines, File outputDir,
                                          int numCellPerSide, String partPrefix ) throws IOException {
    PartRestaurant.writeMultiLevelGridParts( lines, outputDir, numCellPerSide, partPrefix,
        (RestaurantBoundaries) boundariesData, SPLITERATOR );
  }
  
  public RestaurantBoundaries parseBoundaries( List<String> lines ) {
    RestaurantBoundariesRecord boundariesRecord = DataUtils.computeBoundaries( lines, SPLITERATOR );
    return new RestaurantBoundaries( boundariesRecord );
  }
  
  public RestaurantBoundaries parseGlobalBoundaries( File directory ) {
    RestaurantBoundariesRecord boundariesRecord = DataUtils.computeGlobalBoundaries( directory, SPLITERATOR );
    return new RestaurantBoundaries( boundariesRecord );
  }
  
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append( format( "CoordX boundaries (%.4f,%.4f).%n", minX, maxX ) );
    sb.append( format( "CoordY boundaries (%.4f,%.4f).%n", minY, maxY ) );
    sb.append( format( "Zipcode boundaries (%d,%d).%n", minZip, maxZip ) );
    sb.append( format( "Date boundaries (%d,%d).%n", minT, maxT ) );
    sb.append( format( "Score boundaries (%d,%d).%n", minScore, maxScore ) );
    sb.append( format( "Id boundaries (%d,%d).%n", minId, maxId ) );
    return sb.toString();
  }
}
