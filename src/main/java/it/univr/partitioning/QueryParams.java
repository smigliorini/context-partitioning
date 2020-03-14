package it.univr.partitioning;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class QueryParams{
  private Double minX;
  private Double maxX;
  private Double minY;
  private Double maxY;
  private Long minT;
  private Long maxT;
  private Integer minAge;
  private Integer maxAge;

  public QueryParams() {
    minX = null;
    maxX = null;
    minY = null;
    maxY = null;
    minT = null;
    maxT = null;
    minAge = null;
    maxAge = null;
  }

  public QueryParams
    ( Double minX,
      Double maxX,
      Double minY,
      Double maxY,
      Long minT,
      Long maxT,
      Integer minAge,
      Integer maxAge ) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.minT = minT;
    this.maxT = maxT;
    this.minAge = minAge;
    this.maxAge = maxAge;
  }

  public Double getMinX() {
    return minX;
  }

  public void setMinX( Double minX ) {
    this.minX = minX;
  }

  public Double getMaxX() {
    return maxX;
  }

  public void setMaxX( Double maxX ) {
    this.maxX = maxX;
  }

  public Double getMinY() {
    return minY;
  }

  public void setMinY( Double minY ) {
    this.minY = minY;
  }

  public Double getMaxY() {
    return maxY;
  }

  public void setMaxY( Double maxY ) {
    this.maxY = maxY;
  }

  public Long getMinT() {
    return minT;
  }

  public void setMinT( Long minT ) {
    this.minT = minT;
  }

  public Long getMaxT() {
    return maxT;
  }

  public void setMaxT( Long maxT ) {
    this.maxT = maxT;
  }

  public Integer getMinAge() {
    return minAge;
  }

  public void setMinAge( Integer minAge ) {
    this.minAge = minAge;
  }

  public Integer getMaxAge() {
    return maxAge;
  }

  public void setMaxAge( Integer maxAge ) {
    this.maxAge = maxAge;
  }
}