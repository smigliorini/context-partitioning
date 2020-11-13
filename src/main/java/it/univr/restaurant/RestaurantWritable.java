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

  /**
   * List of fields to be printed in the output file
   */
  /*private static final int[] numItems = { 0,1,2,3,4,5,6,8,9,10,11 }; // all items - $date //*/
  private static final int[] numItems = { 1,2,9,0 }; // coordX, coordY, $date, building //*/
  /*private static final int[] numItems = { 1,2,7,9 }; // coordX, coordY, $date, score //*/
  /*private static final int[] numItems = { 1,2,8,11 }; // coordX, coordY, score, grade, restaurantId //*/
  /*private static final int[] numItems = { 1,2,9,8 }; // coordX, coordY, score, grade //*/

  /**
   * Fields of the input file
   */
  private static final String[] attributes = {
          //"coordX", "coordY", "$date", "score"
          "building",
          "coordX",
          "coordY",
          "street",
          "zipcode",
          "borough",
          "cuisine",
          "$date",
          "grade",
          "score",
          "name",
          "restaurantId"
  };

  /**
   * Possible fields to partition
   */
  private static final String[] partItems = { "coordX", "coordY", "$date", "score" };

  static final Logger LOGGER = LogManager.getLogger( RestaurantWritable.class );

  public static String SPLITERATOR = ",";

  public RestaurantWritable() {
    super();
  }

  public RestaurantWritable( RestaurantRecord restaurantRecord ) {
    super( restaurantRecord );
  }

  public RestaurantWritable( RestaurantWritable restaurantWritable ) {
    super( restaurantWritable );
  }

  @Override
  public String[] getContextFields() {
    //return new String[]{"coordX", "coordY", "$date", "score"};
    return getContextFields( numItems );
  }

  public String[] getContextFields( int[] ordinal ) {
    final List<String> writeFields = Arrays.asList( getWriteFields( ordinal ) );
    final Set<String> partFields = new HashSet<>();

    for( String pf : partItems ) {
      if( writeFields.contains(pf) ) {
        partFields.add(pf);
      }
    }

    if( partFields.size() == 0 ) {
      LOGGER.error( format( "Context fields array is empty, choose at least two: %s", Arrays.toString(partItems) ) );
      System.exit(-1);
    }

    return partFields.toArray( new String[partFields.size()] );
  }

  public String[] getWriteFields() {
    return getWriteFields( numItems );
  }

  public String[] getWriteFields( int[] items ) {
    List<String> fields = new ArrayList<>();

    for( int fieldNumber : items ) {
      if( fieldNumber >= 0 && fieldNumber < attributes.length  )
        fields.add( attributes[fieldNumber] );
      else
        LOGGER.warn( format( "Field index %d is not exist.", fieldNumber ) );
    }

    return fields.toArray( new String[fields.size()] );
  }

  @Override
  public int compareTo( ContextData o ) {
    RestaurantRecord restaurantRecord = ( RestaurantRecord ) o;
    if ( this.$date < restaurantRecord.$date )
      return -1;
    if ( this.$date > restaurantRecord.$date )
      return 1;
    if ( this.coordX < restaurantRecord.coordX )
      return 1;
    if ( this.coordX > restaurantRecord.coordX )
      return -1;
    if ( this.coordY < restaurantRecord.coordY )
     return 1;
    if ( this.coordY > restaurantRecord.coordY )
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

    for( String fieldName : getWriteFields() ) {
      switch( fieldName ) {
        case "building":
          // building can be empty string
          try {
            out.writeUTF( building.toString() );
          } catch( NullPointerException e ) {
            out.writeUTF( "null" );
          }
          break;
        case "coordX":
          out.writeDouble( coordX );
          break;
        case "coordY":
          out.writeDouble( coordY );
          break;
        case "street":
          out.writeUTF( street.toString() );
          break;
        case "zipcode":
          out.writeInt( zipcode );
          break;
        case "borough":
          out.writeUTF( borough.toString() );
          break;
        case "cuisine":
          out.writeUTF( cuisine.toString() );
          break;
        case "$date":
          out.writeDouble( $date );
          break;
        case "grade":
          out.writeUTF( grade.toString() );
          break;
        case "score":
          out.writeDouble( score );
          break;
        case "name":
          out.writeUTF( name.toString() );
          break;
        case "restaurantId":
          out.writeInt( restaurantId );
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
    // score = in.readInt();
    score = in.readDouble();//*/

    for( String fieldName : getWriteFields() ) {
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
        case "street":
          street = in.readUTF();
          break;
        case "zipcode":
          zipcode = in.readInt();
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
          restaurantId = in.readInt();
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

      for( String fieldName : getWriteFields() ) {
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
    b.deleteCharAt( b.length() - 1 );
    return b.toString();
  }
}
