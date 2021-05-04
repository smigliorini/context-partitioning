package it.univr.restaurant.partitioning;


/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class QueryParamsRestaurant {
  
  protected Double minX;
  protected Double maxX;
  protected Double minY;
  protected Double maxY;
  protected Integer minZ;
  protected Integer maxZ;
  protected Long minT;
  protected Long maxT;
  protected Integer minS;
  protected Integer maxS;
  protected Integer minId;
  protected Integer maxId;

  public QueryParamsRestaurant() {
    minX = null;
    maxX = null;
    minY = null;
    maxY = null;
    minZ = null;
    maxZ = null;
    minT = null;
    maxT = null;
    minS = null;
    maxS = null;
    minId = null;
    maxId = null;
  }

  public QueryParamsRestaurant
    ( Double minX, Double maxX,
      Double minY, Double maxY,
      Integer minZ, Integer maxZ,
      Long minT, Long maxT,
      Integer minS, Integer maxS,
      Integer minId, Integer maxId ) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.minZ = minZ;
    this.maxZ = maxZ;
    this.minT = minT;
    this.maxT = maxT;
    this.minS = minS;
    this.maxS = maxS;
    this.minId = minId;
    this.maxId = maxId;
  }
  
  public QueryParamsRestaurant( QueryParamsRestaurant paramsRestaurant ) {
    this( paramsRestaurant.getMinX(),
        paramsRestaurant.getMaxX(),
        paramsRestaurant.getMinY(),
        paramsRestaurant.getMaxY(),
        paramsRestaurant.getMinZ(),
        paramsRestaurant.getMaxZ(),
        paramsRestaurant.getMinT(),
        paramsRestaurant.getMaxT(),
        paramsRestaurant.getMinS(),
        paramsRestaurant.getMaxS(),
        paramsRestaurant.getMinId(),
        paramsRestaurant.getMaxId()
    );
  }
  
  public Double getMinX() { return minX; }
  
  public void setMinX( Double minX ) { this.minX = minX; }

  public Double getMaxX() { return maxX; }
  
  public void setMaxX( Double maxX ) { this.maxX = maxX; }

  public Double getMinY() { return minY; }
  
  public void setMinY( Double minY ) { this.minY = minY; }

  public Double getMaxY() { return maxY; }
  
  public void setMaxY( Double maxY ) { this.maxY = maxY; }

  public Integer getMinZ() { return minZ; }
  
  public void setMinZ( Integer minZ ) { this.minZ = minZ; }

  public Integer getMaxZ() { return maxZ; }
  
  public void setMaxZ( Integer maxZ ) { this.maxZ = maxZ; }

  public Long getMinT() { return minT; }
  
  public void setMinT( Long minT ) { this.minT = minT; }

  public Long getMaxT() { return maxT; }
  
  public void setMaxT( Long maxT ) { this.maxT = maxT; }

  public Integer getMinS() { return minS; }
  
  public void setMinS( Integer minS ) { this.minS = minS; }

  public Integer getMaxS() { return maxS; }
  
  public void setMaxS( Integer maxS ) { this.maxS = maxS; }

  public Integer getMinId() { return minId; }
  
  public void setMinId( Integer minId ) { this.minId = minId; }

  public Integer getMaxId() { return maxId; }
  
  public void setMaxId( Integer maxId ) { this.maxId = maxId; }
}