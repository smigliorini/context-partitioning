package it.univr.restaurant;

import it.univr.hadoop.ContextData;
import it.univr.restaurant.partitioning.DataUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class RestaurantWritable
  extends RestaurantRecord
  implements ContextData {

  static final Logger LOGGER = LogManager.getLogger(RestaurantWritable.class);
  public static String SPLITERATOR = ",";
  private static final String[] fileFields = {
          "building", "coordX", "coordY", "street", "zipcode",
          "borough", "cuisine", "$date", "grade", "score", "name", "restaurantId"
  };
  private static final String[] partLevels = { "coordX", "coordY", "$date", "score" };

  public RestaurantWritable() {
    super();
  }

  public RestaurantWritable( RestaurantRecord restaurantRecord ) {
    super( restaurantRecord );
  }

  public RestaurantWritable( RestaurantWritable restaurantWritable ) {
    super( restaurantWritable );
  }

  private static final int[] args = {1,2,7,9}; // coordX, coordY, $date, score//*/
  /*private static final int[] args = {7,1,2,9}; // $date, coordX, coordY, score//*/

  @Override
  public String[] getContextFields() {
    //return new String[]{"coordX", "coordY", "$date", "score"};
    return getContextFields( args );
  }

  public String[] getContextFields( int[] ordinal ) {
    final List<String> fields = Arrays.asList(getFields( ordinal ));
    final Set<String> contextFields = new LinkedHashSet<>();

    for( String level : partLevels ) {
      if( fields.contains( level ) ) {
        contextFields.add( level );
      }
    }

    if( contextFields.size() == 0 ) {
      LOGGER.error( format( "Context fields is empty index, choose: %s", Arrays.toString(partLevels) ) );
    }

    return contextFields.toArray( new String[contextFields.size()] );
  }

  public String[] getFields() {

    return getFields( args );
  }

  public String[] getFields( int[] ordinal ) {
    final Set<String> fieldSet = new LinkedHashSet<>();
    final int start = 0, end = fileFields.length;

    for( int o : ordinal ) {
      if( o < start || o > end ) {
        LOGGER.warn( format( "Field index %d not exist.", o ) );
      }
      fieldSet.add( fileFields[o] );
    }
    return fieldSet.toArray( new String[fieldSet.size()] );
  }

  @Override
  public int compareTo( ContextData o ) {
    RestaurantRecord restaurantRecord = ( RestaurantRecord ) o;
    if ( this.coordX < restaurantRecord.coordX )
      return 1;
    if ( this.coordX > restaurantRecord.coordX )
      return -1;
    if ( this.coordY < restaurantRecord.coordY )
      return 1;
    if ( this.coordY > restaurantRecord.coordY )
      return -1;
    if ( this.$date < restaurantRecord.$date )
      return -1;
    if ( this.$date > restaurantRecord.$date )
      return 1;
    if ( this.grade.compareTo(restaurantRecord.grade) < 0 )
      return 1;
    if ( this.grade.compareTo(restaurantRecord.grade) > 0 )
      return -1;
    if ( this.score < restaurantRecord.score )
      return 1;
    if ( this.score > restaurantRecord.score )
      return -1;
    if ( this.restaurantId.compareTo(restaurantRecord.restaurantId) < 0 )
      return 1;
    if ( this.restaurantId.compareTo(restaurantRecord.restaurantId) > 0 )
      return -1;
    return 0;
  }

  @Override
  public void write( DataOutput out ) throws IOException {
    /*
    out.writeDouble( coordX );
    out.writeDouble( coordY );
    out.writeDouble( $date );
    out.writeDouble( score );//*/

    for( String fieldName : getFields() ) {

      switch( fieldName ) {
        case "building":
          // building can be empty string
          try {
            out.writeUTF( building.toString() );
          } catch( NullPointerException e ) {
            out.writeUTF( "null" );
          }
        case "coordX":
          out.writeDouble( coordX );
          break;
        case "coordY":
          out.writeDouble( coordY );
          break;
        case "street:":
          out.writeUTF( street );
          break;
        case "zipcode":
          out.writeUTF( zipcode );
          break;
        case "borough":
          out.writeUTF( borough );
          break;
        case "cuisine":
          out.writeUTF( cuisine );
          break;
        case "$date":
          out.writeDouble( $date );
          break;
        case "grade":
          out.writeUTF( grade );
        case "score":
          try {
            out.writeDouble( score );
          } catch ( NullPointerException e ) {
            System.out.println("score: " + score);
            out.writeDouble( score );
          }
          break;
        case "name":
          out.writeUTF( name );
          break;
        case "restaurantId":
          out.writeUTF( restaurantId );
          break;
      }
    }
  }

  @Override
  public void readFields( DataInput in ) throws IOException {
    /*
    coordX = in.readDouble();
    coordY = in.readDouble();
    // $date = in.readLong();
    $date = in.readDouble();
    //score = in.readInt();
    score = in.readDouble();//*/

    for( String fieldName : getFields() ) {

      switch( fieldName ) {
        case "building":
          building = in.readUTF();
          break;
        case "coordX":
          coordX = in.readDouble();
          break;
        case "coordY":
          coordY = in.readDouble();
          break;
        case "street:":
          street = in.readUTF();
          break;
        case "zipcode":
          zipcode = in.readUTF();
          break;
        case "borough":
          borough = in.readUTF();
          break;
        case "cuisine":
          cuisine = in.readUTF();
          break;
        case "$date":
          $date = in.readDouble();
          break;
        case "grade":
          grade = in.readUTF();
          break;
        case "score":
          score = in.readDouble();
          break;
        case "name":
          name = in.readUTF();
          break;
        case "restaurantId":
          restaurantId = in.readUTF();
          break;
      }
    }
  }

  @Override
  public boolean equals( Object o ) {
    //TODO related to hashcode
    return super.equals( o );
  }

  @Override
  public int hashCode() {
    //TODO Read documentation Hadoop about hash calculation
    return super.hashCode();
  }

  public RestaurantWritable parseRecord( String line ) {
    RestaurantRecord restaurantRecord = DataUtils.parseRecord( line, SPLITERATOR );
    return new RestaurantWritable( restaurantRecord );
  }

  @Override
  public String toString() { return toString( SPLITERATOR ); }

  public String toString( String separator ) {
    final StringBuilder b = new StringBuilder();

    for( int i=0; i < 2; i++ ) { // fix for producing the same format of the input
      for( String fieldName : getFields() ) {

        switch( fieldName ) {
          case "building":
            b.append( building );
            b.append( separator );
            break;
          case "coordX":
            b.append( coordX );
            b.append( separator );
            break;
          case "coordY":
            b.append( coordY );
            b.append( separator );
            break;
          case "street":
            b.append( street );
            b.append( separator );
            break;
          case "zipcode":
            b.append( zipcode );
            b.append( separator );
            break;
          case "borough":
            b.append( borough );
            b.append( separator );
            break;
          case "cuisine":
            b.append( cuisine );
            b.append( separator );
            break;
          case "$date":
            b.append( $date );
            b.append( separator );
            break;
          case "grade":
            b.append( grade );
            b.append( separator );
            break;
          case "score":
            b.append( score );
            b.append( separator );
            break;
          case "name":
            b.append( name );
            b.append( separator );
            break;
          case "restaurantId":
            b.append( restaurantId );
            b.append( separator );
            break;
        }
      }
    }

    b.deleteCharAt( b.length()-1 );
    return b.toString();
  }
}
