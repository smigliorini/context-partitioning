package it.univr.operations;

import java.util.ArrayList;
import java.util.List;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class GlobalIndex {

  // === Properties ============================================================

  private List<Partition> partitionList;

  // === Methods ===============================================================

  public GlobalIndex() {
    this.partitionList = new ArrayList<>();
  }

  public List<Partition> getPartitionList() {
    return partitionList;
  }

  public void setPartitionList( List<Partition> partitionList ) {
    this.partitionList = partitionList;
  }

  public void addPartition( Partition partition ){
    if( partitionList == null ){
      partitionList = new ArrayList<>();
    }
    partitionList.add( partition );
  }
}
