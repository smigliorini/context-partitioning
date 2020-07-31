package it.univr.veronacard;

import it.univr.hadoop.ContextData;
import it.univr.partitioning.DataUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VeronaCardWritable
  extends VeronaCardRecord
  implements ContextData {

  public static String SPLITERATOR = ",";

  public VeronaCardWritable() {
    super();
  }

  public VeronaCardWritable( VeronaCardRecord veronaCardRecord ) {
    super( veronaCardRecord );
  }

  public VeronaCardWritable( VeronaCardWritable veronaCardWritable ) {
    super( veronaCardWritable );
  }

  @Override
  public String[] getContextFields() {
    return new String[]{"x", "y", "time", "age"};
  }

  @Override
  public int compareTo( ContextData o ) {
    //TODO Do we need a sort algorithm?
    VeronaCardRecord veronaCardRecord = (VeronaCardRecord) o;
    if( this.time < veronaCardRecord.time )
      return -1;
    if( this.time > veronaCardRecord.time )
      return 1;
    if( this.x < veronaCardRecord.x )
      return 1;
    if( this.x > veronaCardRecord.x )
      return -1;
    return 0;
  }

  @Override
  public void write( DataOutput out ) throws IOException {
    //new Text(vcSerial).write(out);
    out.writeDouble( x );
    out.writeDouble( y );
    //out.writeLong(time);
    out.writeDouble( time );
    //new Text(poiName).write(out);
    //out.writeInt(age);
    out.writeDouble( age );
  }

  @Override
  public void readFields( DataInput in ) throws IOException {
    //Text serial = new Text();
    //serial.readFields(in);
    //vcSerial = serial.toString();
    x = in.readDouble();
    y = in.readDouble();
    //time = in.readLong();
    time = in.readDouble();
    //Text poi = new Text();
    //poi.readFields(in);
    //poiName = poi.toString();
    //age = in.readInt();
    age = in.readDouble();
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

  public VeronaCardWritable parseRecord( String line ) {
    VeronaCardRecord veronaCardRecord = DataUtils.parseRecord( line, SPLITERATOR );
    return new VeronaCardWritable( veronaCardRecord );
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append( x );
    b.append( "," );
    b.append( y );
    b.append( "," );
    b.append( time );
    b.append( "," );
    b.append( age );
    b.append( "," );
    b.append( x );
    b.append( "," );
    b.append( y );
    b.append( "," );
    b.append( time );
    b.append( "," );
    b.append( age );
    return b.toString();
  }
}
