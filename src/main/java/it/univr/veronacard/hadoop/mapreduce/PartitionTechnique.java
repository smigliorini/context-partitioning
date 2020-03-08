package it.univr.veronacard.hadoop.mapreduce;


public enum PartitionTechnique {

    ML_GRID("ML_GRID"), MD_GRID("MD_GRID"),CONTEXT_AWARE("CONTEXT_AWARE");

    private String partitionTechnique;

    PartitionTechnique(String technique) {
        this.partitionTechnique = technique;
    }


    public String getPartitionTechnique() {
        return partitionTechnique;
    }

}
