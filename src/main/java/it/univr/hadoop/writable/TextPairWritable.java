package it.univr.hadoop.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPairWritable implements WritableComparable<TextPairWritable> {

    private Text first;
    private Text second;

    public TextPairWritable() {
        set(new Text(), new Text());
    }
    public TextPairWritable(String first, String second) {
        set(new Text(first), new Text(second));
    }
    public TextPairWritable(Text first, Text second) {
        set(first, second);
    }


    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }


    public Text getFirst() {
        return first;
    }
    public Text getSecond() {
        return second;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPairWritable) {
            TextPairWritable tp = (TextPairWritable) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
    @Override
    public String toString() {
        if(second.toString().isBlank())
            return first.toString();
        return first + "\t" + second;
    }


    @Override
    public int compareTo(TextPairWritable tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }
}
