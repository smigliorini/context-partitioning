package it.univr.veronacard.hadoop.conf;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;

@XmlRootElement(name = "configuration")
public class HContexBasedConf {

    private HashMap<PartitionTechnique, Integer> splitSizes;

    public HContexBasedConf () {
        super();
    }

    public HashMap<PartitionTechnique, Integer> getSplitSizes() {
        return splitSizes;
    }

    public void setSplitSizes(HashMap<PartitionTechnique, Integer> splitSizes) {
        this.splitSizes = splitSizes;
    }

    public Integer getSplitSize(PartitionTechnique partitionTechnique) {
        return splitSizes.get(partitionTechnique);
    }

}
