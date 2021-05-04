package it.univr.veronacard.partitioning;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VeronaCardBoundariesRecord {

  protected double minX;
  protected double maxX;
  protected double minY;
  protected double maxY;
  protected long minT;
  protected long maxT;
  protected int minAge;
  protected int maxAge;
  
  public static String SPLITERATOR = ",";
  
  public VeronaCardBoundariesRecord() {
    minX = Double.MAX_VALUE;
    maxX = Double.MIN_VALUE;
    minY = Double.MAX_VALUE;
    maxY = Double.MIN_VALUE;
    minT = Long.MAX_VALUE;
    maxT = Long.MIN_VALUE;
    minAge = Integer.MAX_VALUE;
    maxAge = Integer.MIN_VALUE;
  }
  
  public VeronaCardBoundariesRecord
  ( Double minX, Double maxX, Double minY, Double maxY,
    Long minT, Long maxT, Integer minAge, Integer maxAge ) {
    
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.minT = minT;
    this.maxT = maxT;
    this.minAge = minAge;
    this.maxAge = maxAge;
  }
  
  public VeronaCardBoundariesRecord( VeronaCardBoundariesRecord boundaries ) {
    this( boundaries.getMinX(),
        boundaries.getMaxX(),
        boundaries.getMinY(),
        boundaries.getMaxY(),
        boundaries.getMinT(),
        boundaries.getMaxT(),
        boundaries.getMinAge(),
        boundaries.getMaxAge()
    );
  }

  public void updateMinX( double x ){
    minX = Math.min( minX, x );
  }

  public void updateMaxX( double x ){
    maxX = Math.max( maxX, x );
  }

  public void updateMinY( double y ){
    minY = Math.min( minY, y );
  }

  public void updateMaxY( double y ){
    maxY = Math.max( maxY, y );
  }

  public void updateMinT( long t ){
    minT = Math.min( minT, t );
  }

  public void updateMaxT( long t ){
    maxT = Math.max( maxT, t );
  }

  public void updateMinAge( int a ){
    minAge = Math.min( minAge, a );
  }

  public void updateMaxAge( int a ){
    maxAge = Math.max( maxAge, a );
  }

  public double getMinX() { return minX; }

  public double getMaxX() {
    return maxX;
  }

  public double getMinY() {
    return minY;
  }

  public double getMaxY() {
    return maxY;
  }

  public long getMinT() {
    return minT;
  }

  public long getMaxT() {
    return maxT;
  }

  public int getMinAge() {
    return minAge;
  }

  public int getMaxAge() {
    return maxAge;
  }
  
  @Override
  public String toString() { return toString( SPLITERATOR ); }
  
  public String toString( String separator) {
    final StringBuilder sb = new StringBuilder();
    sb.append( minX );
    sb.append( separator );
    sb.append( maxX );
    sb.append( separator );
    sb.append( minY );
    sb.append( separator );
    sb.append( maxY );
    sb.append( separator );
    sb.append( minT );
    sb.append( separator );
    sb.append( maxT );
    sb.append( separator );
    sb.append( minAge );
    sb.append( separator );
    sb.append( maxAge );
    return sb.toString();
  }
}
