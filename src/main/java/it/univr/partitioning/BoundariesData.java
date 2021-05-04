package it.univr.partitioning;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface BoundariesData {
  
  void computeContextBasedParts( List<String> lines, File outputDir,
                                 String separator, String partPrefix ) throws IOException;
  
  Set<String> computeMultiDimGridParts( BoundariesData boundariesData, File input,
                                        File outputFile, int numCellPerSide, String partPrefix ) throws IOException;
  
  void computeMultiLevelGridParts( BoundariesData boundariesData, List<String> lines,
                                   File outputDir, int numCellPerSide, String partPrefix ) throws IOException;
  
  BoundariesData parseBoundaries( List<String> lines );
  
  BoundariesData parseGlobalBoundaries( File directory );
  
}
