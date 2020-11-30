package it.univr.restaurant.descriptors;

import java.util.ArrayList;
import java.util.StringTokenizer;

import static java.lang.Math.min;

public class NRectangle {

  // === Properties ============================================================

  protected ArrayList<Double> coordMin;
  protected ArrayList<Double> coordMax;
  protected ArrayList<Double> size;
  protected int dim;
  protected boolean isValid;

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param value
   */

  public NRectangle( String value ) {
    StringTokenizer st = new StringTokenizer( value, "," );
    ArrayList<Double> al = new ArrayList<Double>();

    // TODO: input file dependent -> OneGrid 632
    while( st.hasMoreTokens() ) {
      final String token = st.nextToken();
      al.add( Double.parseDouble( token ) );
    }

    if( al.size() % 2 != 0 ) {
      this.isValid = false;
      return;
    }

    this.dim = al.size() / 2;
    this.coordMin = new ArrayList<Double>();
    this.coordMax = new ArrayList<Double>();
    this.size = new ArrayList<Double>();
    Double diff;

    for( int i = 0; i < this.dim; i++ ) {
      this.coordMin.add( al.get( i ) );
      this.coordMax.add( al.get( i + this.dim ) );
      diff = al.get( i + this.dim ) - al.get( i );

      if( diff < 0 ) {
        this.isValid = false;
        return;
      }
      this.size.add( diff );
    }

    this.isValid = true;
  }

  public int getDim() {
    return dim;
  }

  public Double getCoordMin( int i ) {
    if( i > dim ) return null;
    else return coordMin.get( i );
  }

  public void setCoordMin( int i, double d ) {
    if( i < dim ) coordMin.set( i, d );
  }

  public Double getCoordMax( int i ) {
    if( i > dim ) return null;
    else return coordMax.get( i );
  }

  public void setCoordMax( int i, double d ) {
    if( i < dim ) coordMax.set( i, d );
  }

  public Double getSize( int i ) {
    if( i > dim ) return null;
    else return size.get( i );
  }

  /**
   * MISSING_COMMENT
   *
   * @param xmin
   * @param ymin
   * @param xmax
   * @param ymax
   * @return
   */

  public Boolean isInside2D
  ( double xmin, double ymin,
    double xmax, double ymax ) {

    if( coordMin.get( 0 ) < xmin || coordMin.get( 1 ) < ymin ||
        coordMax.get( 0 ) > xmax || coordMax.get( 1 ) > ymax )
      return false;
    else
      return true;
  }

  /**
   * MISSING_COMMENT
   *
   * @param xmin
   * @param ymin
   * @param zmin
   * @param xmax
   * @param ymax
   * @param zmax
   * @return
   */
  public Boolean isInside3D
  ( double xmin, double ymin, double zmin,
    double xmax, double ymax, double zmax ) {

    if( coordMin.get( 0 ) < xmin || coordMin.get( 1 ) < ymin || coordMin.get( 2 ) < zmin ||
        coordMax.get( 0 ) > xmax || coordMax.get( 1 ) > ymax || coordMax.get( 2 ) > zmax )
      return false;
    else
      return true;
  }

  /**
   * MISSING_COMMENT
   *
   * @param mins
   * @param maxs
   * @param numDims
   * @return
   */

  public Boolean isInsideMultiDims
  ( Double[] mins, Double[] maxs, int numDims ) {
    if( mins.length != numDims || maxs.length != numDims ) {
      throw new IllegalArgumentException();
    }

    if( coordMin.size() < numDims || coordMax.size() < numDims ) {
      throw new IllegalArgumentException();
    }

    boolean inside = true;
    for( int i = 0; i < min( numDims, coordMin.size() ) && inside; i++ ) {
      if( coordMin.get( i ) < mins[i] || coordMax.get( i ) > maxs[i] ) {
        inside = false;
      }
    }

    return inside;
  }

  /**
   * MISSING_COMMENT
   *
   * @param rangeQuery
   * @return
   */

  public Boolean isInsideMultiDims( NRectangle rangeQuery ) {
    if( rangeQuery == null ) {
      throw new NullPointerException();
    }

    final Double[] coordMaxs = new Double[rangeQuery.coordMax.size()];
    rangeQuery.coordMax.toArray( coordMaxs );

    final Double[] coordMins = new Double[rangeQuery.coordMin.size()];
    rangeQuery.coordMin.toArray( coordMins );

    return isInsideMultiDims( coordMins, coordMaxs, rangeQuery.dim );
  }

  /**
   * MISSING_COMMENT
   *
   * @param rangeQuery
   * @return
   */

  public Boolean intersectsMultiDims( NRectangle rangeQuery ) {
    if( rangeQuery == null ) {
      throw new NullPointerException();
    }

    for( int i = 0; i < rangeQuery.dim; i++ ) {
      if( !intersects
        ( this.getCoordMin( i ),
          this.getCoordMax( i ),
          rangeQuery.getCoordMin( i ),
          rangeQuery.getCoordMax( i ) ) ) {
        return false;
      }
    }
    return true;
  }

  /**
   * MISSING_COMMENT
   *
   * @param xMin
   * @param xMax
   * @param rangeMin
   * @param rangeMax
   * @return
   */

  private Boolean intersects
  ( Double xMin, Double xMax,
    Double rangeMin, Double rangeMax ) {

    if( xMin <= rangeMin && xMax >= rangeMin ) {
      return true;
    } else if( xMin > rangeMin && xMin <= rangeMax ) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public String print() {
    if( dim == 2 )
      return coordMin.get( 0 ).toString() + "," + coordMin.get( 1 ).toString() + "," +
             coordMax.get( 0 ).toString() + "," + coordMax.get( 1 ).toString();
    else {
      String s = new String();
      for( int i = 0; i < dim; i++ )
        s += coordMin.get( i ).toString() + ",";
      for( int i = 0; i < dim; i++ )
        s += coordMax.get( i ).toString() + ( i == ( dim - 1 ) ? "" : "," );
      return s;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public String printWKT2D() {
    if( dim == 2 )
      return "POLYGON((" + coordMin.get( 0 ).toString() + " " + coordMin.get( 1 ).toString() + ", " +
             coordMax.get( 0 ).toString() + " " + coordMin.get( 1 ).toString() + ", " +
             coordMax.get( 0 ).toString() + " " + coordMax.get( 1 ).toString() + ", " +
             coordMin.get( 0 ).toString() + " " + coordMax.get( 1 ).toString() + ", " +
             coordMin.get( 0 ).toString() + " " + coordMin.get( 1 ).toString() + "))";
    else
      return "";
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public double getNVol() {
    double a = 1.0;
    for( int i = 0; i < dim; i++ )
      a *= ( this.coordMax.get( i ) - this.coordMin.get( i ) );
    return a;
  }
}