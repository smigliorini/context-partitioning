package it.univr.veronacard.partitioning;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Boundaries {

  private double minX;
  private double maxX;
  private double minY;
  private double maxY;
  private long minT;
  private long maxT;
  private int minAge;
  private int maxAge;

  public Boundaries() {
    minX = Double.MAX_VALUE;
    maxX = Double.MIN_VALUE;
    minY = Double.MAX_VALUE;
    maxY = Double.MIN_VALUE;
    minT = Long.MAX_VALUE;
    maxT = Long.MIN_VALUE;
    minAge = Integer.MAX_VALUE;
    maxAge = Integer.MIN_VALUE;
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

  public double getMinX() {
    return minX;
  }

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
}
