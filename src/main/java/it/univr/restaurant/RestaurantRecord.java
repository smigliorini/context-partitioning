package it.univr.restaurant;

import java.util.Objects;

public class  RestaurantRecord {

  public static final String SPLITERATOR = ",";

  protected String building;
  protected Double coordX;
  protected Double coordY;
  protected String street;
  protected String zipcode;
  protected String borough;
  protected String cuisine;
  //protected Long $date;
  protected Double $date;
  protected String grade;
  //protected Integer score;
  protected Double score;
  protected String name;
  protected String restaurantId;

  public RestaurantRecord() {
    building = null;
    coordX = null;
    coordY = null;
    street = null;
    zipcode = null;
    borough = null;
    cuisine = null;
    $date = null;
    grade = null;
    score = null;
    name = null;
    restaurantId = null;
  }

  public RestaurantRecord(String building, Double coordX, Double coordY, String street, String zipcode, String borough,
                          String cuisine, Double $date, String grade, Double score, String name, String restaurantId) {
    this.building = building;
    this.coordX = coordX;
    this.coordY = coordY;
    this.street = street;
    this.zipcode = zipcode;
    this.borough = borough;
    this.cuisine = cuisine;
    this.$date = $date;
    this.grade = grade;
    this.score = score;
    this.name = name;
    this.restaurantId = restaurantId;
  }

  public RestaurantRecord(RestaurantRecord restaurantRecord) {
    this(restaurantRecord.getBuilding(),
            restaurantRecord.getCoordX(),
            restaurantRecord.getCoordY(),
            restaurantRecord.getStreet(),
            restaurantRecord.getZipcode(),
            restaurantRecord.getBorough(),
            restaurantRecord.getCuisine(),
            restaurantRecord.get$date(),
            restaurantRecord.getGrade(),
            restaurantRecord.getScore(),
            restaurantRecord.getName(),
            restaurantRecord.getRestaurantId()
    );
  }

  public String getBuilding() {
    return building;
  }
  public void setBuilding(String building) { this.building = building; }

  public Double getCoordX() { return coordX; }
  public void setCoordX(Double coordX) { this.coordX = coordX; }

  public Double getCoordY() { return coordY; }
  public void setCoordY(Double coordY) { this.coordY = coordY; }

  public String getStreet() {
    return street;
  }
  public void setStreet(String street) {
    this.street = street;
  }

  public String getZipcode() {
    return zipcode;
  }
  public void setZipcode(String zipcode) {
    this.zipcode = zipcode;
  }

  public String getBorough() {
    return borough;
  }
  public void setBorough(String borough) {
    this.borough = borough;
  }

  public String getCuisine() {
    return cuisine;
  }
  public void setCuisine(String cuisine) {
    this.cuisine = cuisine;
  }

  public Double get$date() {
    return $date;
  }
  public void set$date(Double $date) {
    this.$date = $date;
  }

  public String getGrade() {
    return grade;
  }
  public void setGrade(String grade) {
    this.grade = grade;
  }

  public Double getScore() { return score; }
  public void setScore(Double score) { this.score = score; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getRestaurantId() { return restaurantId; }
  public void setRestaurantId(String restaurantId) { this.restaurantId = restaurantId; }

  public String toString( String separator ){
    final StringBuilder sb = new StringBuilder();
    //sb.append( building );
    //sb.append( separator );
    sb.append( coordX );
    sb.append( separator );
    sb.append( coordY );
    sb.append( separator );
    //sb.append( street );
    //sb.append( separator );
    //sb.append( zipcode );
    //sb.append( separator );
    //sb.append( borough );
    //sb.append( separator );
    //sb.append( cuisine );
    //sb.append( separator );
    sb.append( $date );
    sb.append( separator );
    //sb.append( grade );
    //sb.append( separator );
    sb.append( score );
    //sb.append( separator );
    //sb.append( name );
    //sb.append( separator );
    //sb.append( restaurantId );

    // fix for producing the same format of the input
    sb.append( separator ); // ?
    //sb.append( building );
    //sb.append( separator );
    sb.append( coordX );
    sb.append( separator );
    sb.append( coordY );
    sb.append( separator );
    //sb.append( street );
    //sb.append( separator );
    //sb.append( zipcode );
    //sb.append( separator );
    //sb.append( borough );
    //sb.append( separator );
    //sb.append( cuisine );
    //sb.append( separator );
    sb.append( $date );
    sb.append( separator );
    //sb.append( grade );
    //sb.append( separator );
    sb.append( score );
    //sb.append( separator );
    //sb.append( name );
    //sb.append( separator );
    //sb.append( restaurantId );

    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RestaurantRecord)) return false;
    RestaurantRecord that = (RestaurantRecord) o;
    return Objects.equals(coordX, that.coordX) &&
            Objects.equals(coordY, that.coordY) &&
            Objects.equals($date, that.$date) &&
            Objects.equals(grade, that.grade) &&
            Objects.equals(score, that.score) &&
            Objects.equals(restaurantId, that.restaurantId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(coordX, coordY, $date, grade, score);
  }

  @Override
  public String toString() {
    return toString( SPLITERATOR );
  }
}
