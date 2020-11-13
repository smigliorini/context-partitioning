package it.univr.restaurant.partitioning;

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
  private int minScore;
  private int maxScore;

  public Boundaries() {
    minX = Double.MAX_VALUE;
    maxX = Double.MIN_VALUE;
    minY = Double.MAX_VALUE;
    maxY = Double.MIN_VALUE;
    minT = Long.MAX_VALUE;
    maxT = Long.MIN_VALUE;
    minScore = Integer.MAX_VALUE;
    maxScore = Integer.MIN_VALUE;
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

  public void updateMinT( long t ){ minT = Math.min( minT, t ); }

  public void updateMaxT( long t ){
    maxT = Math.max( maxT, t );
  }

  public void updateMinScore( int s ){ minScore = Math.min( minScore, s ); }

  public void updateMaxScore( int s ){
    maxScore = Math.max(  maxScore, s );
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
}
