package it.univr.veronacard;

import java.util.Objects;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VeronaCard {

  protected String vcSerial;
  protected Double x;
  protected Double y;
  protected Long time;
  protected String poiName;
  protected Integer age;

  public VeronaCard() {
    vcSerial = null;
    x = null;
    y = null;
    time = null;
    poiName = null;
    age = null;
  }

  public String getVcSerial() {
    return vcSerial;
  }

  public void setVcSerial( String vcSerial ) {
    this.vcSerial = vcSerial;
  }

  public Double getX() {
    return x;
  }

  public void setX( Double x ) {
    this.x = x;
  }

  public Double getY() {
    return y;
  }

  public void setY( Double y ) {
    this.y = y;
  }

  public Long getTime() {
    return time;
  }

  public void setTime( Long time ) {
    this.time = time;
  }

  public String getPoiName() {
    return poiName;
  }

  public void setPoiName( String poiName ) {
    this.poiName = poiName;
  }

  public Integer getAge() {
    return age;
  }

  public void setAge( Integer age ) {
    this.age = age;
  }

  public String toString( String separator ){
    final StringBuilder sb = new StringBuilder();
    sb.append( vcSerial );
    sb.append( separator );
    sb.append( x );
    sb.append( separator );
    sb.append( y );
    sb.append( separator );
    sb.append( time );
    sb.append( separator );
    sb.append( poiName );
    sb.append( separator );
    sb.append( age );
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof VeronaCard)) return false;
    VeronaCard that = (VeronaCard) o;
    return Objects.equals(vcSerial, that.vcSerial) &&
            Objects.equals(x, that.x) &&
            Objects.equals(y, that.y) &&
            Objects.equals(time, that.time);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vcSerial, x, y, time);
  }
}
