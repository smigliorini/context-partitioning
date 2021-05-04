package it.univr.veronacard.partitioning;

import it.univr.partitioning.BoundariesData;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

public class VeronaCardBoundaries
    extends VeronaCardBoundariesRecord
    implements BoundariesData {
  
  final double[] timeSplits = new double[] {
      1388575020000.000000,
      1422394200000.000000,
      1456213380000.000000,
      1490032560000.000000 };
  
  final double[] xSplits = new double[]{
      10.979600,
      10.993500,
      10.996975,
      10.998713,
      11.000450,
      11.007400
  };
  
  final double[] ySplits = new double[]{
      45.433300,
      45.436800,
      45.440300,
      45,442050,
      45,442925,
      45,443800,
      45,447300
  };
  
  final double[] aSplits = new double[]{
      10.000000,
      30.000000,
      46.015625,
      90.000000
  };
  
  public static String SPLITERATOR = ",";
  
  public VeronaCardBoundaries() {
    super();
  }
  
  public VeronaCardBoundaries( VeronaCardBoundariesRecord recordBoundaries ) {
    super( recordBoundaries );
  }
  
  public VeronaCardBoundaries( VeronaCardBoundaries veronaCardBoundaries ) {
    super( veronaCardBoundaries );
  }
  
  
  @Override
  public void computeContextBasedParts( List<String> lines, File outputDir,
                                        String separator, String partPrefix ) throws IOException {
    PartVeronaCard.writeContextBasedParts( lines, outputDir, separator, partPrefix,
        timeSplits, xSplits, ySplits, aSplits );
  }
  
  @Override
  public Set<String> computeMultiDimGridParts( BoundariesData boundariesData, File input, File outputFile,
                                               int numCellPerSide, String partPrefix ) throws IOException {
    return PartVeronaCard.getMultiDimGridPartSet( input, outputFile, numCellPerSide, partPrefix,
        (VeronaCardBoundaries) boundariesData, SPLITERATOR );
  }
  
  @Override
  public void computeMultiLevelGridParts( BoundariesData boundariesData, List<String> lines, File outputDir,
                                          int numCellPerSide, String partPrefix ) throws IOException {
    PartVeronaCard.writeMultiLevelGridParts( lines, outputDir, numCellPerSide, partPrefix,
        (VeronaCardBoundaries) boundariesData, SPLITERATOR );
  }
  
  
  public VeronaCardBoundaries parseBoundaries( List<String> lines ) {
    VeronaCardBoundariesRecord veronaCardBoundaries = DataUtils.computeBoundaries( lines, SPLITERATOR );
    return new VeronaCardBoundaries( veronaCardBoundaries );
  }
  
  
  public VeronaCardBoundaries parseGlobalBoundaries( File directory ) {
    VeronaCardBoundariesRecord veronaCardBoundaries = DataUtils.computeGlobalBoundaries( directory, SPLITERATOR );
    return new VeronaCardBoundaries( veronaCardBoundaries );
  }
  
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append( format( "X boundaries (%.4f,%.4f).%n", minX, maxX ) );
    sb.append( format( "Y boundaries (%.4f,%.4f).%n", minY, maxY ) );
    sb.append( format( "Time boundaries (%d,%d).%n", minT, maxT ) );
    sb.append( format( "Age boundaries (%d,%d).%n", minAge, maxAge ) );
    return sb.toString();
  }
}
