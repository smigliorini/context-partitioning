package it.univr.veronacard.hadoop.input;

import org.apache.hadoop.io.Text;

public interface TextSerialible {

    /**
     * Store current object as string in the given text appending text already there.
     * @param text The text object to append to.
     * @return The same text that was passed as a parameter
     */
    public Text toText(Text text);

    /**
     * Retrieve information from the given text.
     * @param text The text to parse
     */
    public void fromText(Text text);
}
