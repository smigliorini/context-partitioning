package it.univr.operations;

import it.univr.descriptors.NRectangle;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Partition {

  // === Properties ============================================================

  private int number;
  private NRectangle boundaries;
  private String filename;

  // === Methods ===============================================================

  public Partition() {
    this.number = -1;
    this.boundaries = null;
    this.filename = null;
  }

  public int getNumber() {
    return number;
  }

  public void setNumber( int number ) {
    this.number = number;
  }

  public NRectangle getBoundaries() {
    return boundaries;
  }

  public void setBoundaries( NRectangle boundaries ) {
    this.boundaries = boundaries;
  }

  public void setBoundaries( String boundaries ){
    this.boundaries  = new NRectangle( boundaries );
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }
}
