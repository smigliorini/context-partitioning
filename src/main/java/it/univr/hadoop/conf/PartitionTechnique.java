package it.univr.hadoop.conf;


public enum PartitionTechnique {

    ML_GRID("ML_GRID"), MD_GRID("MD_GRID"), BOX_COUNT("BOX_COUNT");

    private String partitionTechnique;

    PartitionTechnique(String technique) {
        this.partitionTechnique = technique;
    }


    public String getPartitionTechnique() {
        return partitionTechnique;
    }

}
