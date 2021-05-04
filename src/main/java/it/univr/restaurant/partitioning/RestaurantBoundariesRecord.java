package it.univr.restaurant.partitioning;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class RestaurantBoundariesRecord {

  protected double minX;
  protected double maxX;
  protected double minY;
  protected double maxY;
  protected int minZip;
  protected int maxZip;
  protected long minT;
  protected long maxT;
  protected int minScore;
  protected int maxScore;
  protected int minId;
  protected int maxId;
  
  public static String SPLITERATOR = ",";
  
  public RestaurantBoundariesRecord() {
    minX = Double.MAX_VALUE;
    maxX = Double.MIN_VALUE;
    minY = Double.MAX_VALUE;
    maxY = Double.MIN_VALUE;
    minZip = Integer.MAX_VALUE;
    maxZip = Integer.MIN_VALUE;
    minT = Long.MAX_VALUE;
    maxT = Long.MIN_VALUE;
    minScore = Integer.MAX_VALUE;
    maxScore = Integer.MIN_VALUE;
    minId = Integer.MAX_VALUE;
    maxId = Integer.MIN_VALUE;
  }
  
  public RestaurantBoundariesRecord
  ( Double minX, Double maxX, Double minY, Double maxY,
    Integer minZip, Integer maxZip, Long minT, Long maxT,
    Integer minScore, Integer maxScore, Integer minId, Integer maxId ) {
    
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.minZip = minZip;
    this.maxZip = maxZip;
    this.minT = minT;
    this.maxT = maxT;
    this.minScore = minScore;
    this.maxScore = maxScore;
    this.minId = minId;
    this.maxId = maxId;
  }
  
  public RestaurantBoundariesRecord( RestaurantBoundariesRecord boundariesRecord ) {
    this( boundariesRecord.getMinX(),
        boundariesRecord.getMaxX(),
        boundariesRecord.getMinY(),
        boundariesRecord.getMaxY(),
        boundariesRecord.getMinZipcode(),
        boundariesRecord.getMaxZipcode(),
        boundariesRecord.getMinT(),
        boundariesRecord.getMaxT(),
        boundariesRecord.getMinScore(),
        boundariesRecord.getMaxScore(),
        boundariesRecord.getMinId(),
        boundariesRecord.getMaxId()
    );
  }
  
  public void updateMinX( double x ){
    minX = Math.min( minX, x );
  }

  public void updateMaxX( double x ){
    maxX = Math.max( maxX, x );
  }

  public void updateMinY( double y ){ minY = Math.min( minY, y ); }

  public void updateMaxY( double y ){
    maxY = Math.max( maxY, y );
  }

  public void updateMinZipcode( int z ) { minZip = Math.min( minZip, z ); }

  public void updateMaxZipcode( int z ) { maxZip = Math.max( maxZip, z ); }

  public void updateMinT( long t ){ minT = Math.min( minT, t ); }

  public void updateMaxT( long t ){
    maxT = Math.max( maxT, t );
  }

  public void updateMinScore( int s ){ minScore = Math.min( minScore, s ); }

  public void updateMaxScore( int s ){
    maxScore = Math.max( maxScore, s );
  }

  public void updateMinId( int id ){
    minId = Math.min( minId, id );
  }

  public void updateMaxId( int id ){
    maxId = Math.max( maxId, id );
  }

  public double getMinX() {
    return minX;
  }

  public double getMaxX() { return maxX; }

  public double getMinY() {
    return minY;
  }

  public double getMaxY() {
    return maxY;
  }

  public int getMinZipcode() { return minZip; }

  public int getMaxZipcode() { return maxZip; }

  public long getMinT() { return minT; }

  public long getMaxT() {
    return maxT;
  }

  public int getMinScore() {
    return minScore;
  }

  public int getMaxScore() {
    return maxScore;
  }

  public int getMinId() { return minId; }

  public int getMaxId() { return maxId; }
  
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
    sb.append( minZip );
    sb.append( separator );
    sb.append( maxZip );
    sb.append( separator );
    sb.append( minT );
    sb.append( separator );
    sb.append( maxT );
    sb.append( separator );
    sb.append( minScore );
    sb.append( separator );
    sb.append( maxScore );
    sb.append( separator );
    sb.append( minId );
    sb.append( separator );
    sb.append( maxId );
    return sb.toString();
  }
}
