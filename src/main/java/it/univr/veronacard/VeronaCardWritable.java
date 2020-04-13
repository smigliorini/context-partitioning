package it.univr.veronacard;

import it.univr.hadoop.ContextData;
import it.univr.partitioning.DataUtils;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VeronaCardWritable extends VeronaCardRecord implements ContextData {

    public static String SPLITERATOR = ",";

    public VeronaCardWritable (){
        super();
    }

    public VeronaCardWritable (VeronaCardRecord veronaCardRecord){
        super(veronaCardRecord);
    }

    public VeronaCardWritable(VeronaCardWritable veronaCardWritable) {
        super(veronaCardWritable);
    }

    @Override
    public String[] getContextFields() {
        return new String[]{"time", "x", "y", "age"};
    }

    @Override
    public int compareTo(ContextData o) {
        //TODO Do we need a sort algorithm?
        VeronaCardRecord veronaCardRecord = (VeronaCardRecord) o;
        if(this.time < veronaCardRecord.time)
            return -1;
        if(this.time > veronaCardRecord.time)
            return 1;
        if(this.x < veronaCardRecord.x)
            return 1;
        if(this.x > veronaCardRecord.x)
            return -1;
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(vcSerial).write(out);
        out.writeDouble(x);
        out.writeDouble(y);
        out.writeLong(time);
        new Text(poiName).write(out);
        out.writeInt(age);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Text serial = new Text();
        serial.readFields(in);
        vcSerial = serial.toString();
        x = in.readDouble();
        y = in.readDouble();
        time = in.readLong();
        Text poi = new Text();
        poi.readFields(in);
        poiName = poi.toString();
        age = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        //TODO related to hashcode
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        //TODO Read documentation Hadoop about hash calculation
        return super.hashCode();
    }

    public VeronaCardWritable parseRecord(String line) {
        VeronaCardRecord veronaCardRecord = DataUtils.parseRecord(line, SPLITERATOR);
        return new VeronaCardWritable(veronaCardRecord);
    }


}
