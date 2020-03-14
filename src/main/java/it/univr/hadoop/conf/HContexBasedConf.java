package it.univr.hadoop.conf;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;

@XmlRootElement(name = "configuration")
public class HContexBasedConf {

    private HashMap<PartitionTechnique, Long> splitSizes;

    public HContexBasedConf () {
        super();
    }

    public HashMap<PartitionTechnique, Long> getSplitSizes() {
        return splitSizes;
    }

    public void setSplitSizes(HashMap<PartitionTechnique, Long> splitSizes) {
        this.splitSizes = splitSizes;
    }

    public Long getSplitSize(PartitionTechnique partitionTechnique) {
        return splitSizes.get(partitionTechnique);
    }

}
