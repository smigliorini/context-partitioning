package it.univr.veronacard.hadoop.mapreduce;


public enum PartitionTechnique {

    ML_GRID("ML_GRID"), MD_GRID("MD_GRID"),CONTEX_AWARE("CONTEX_AWARE");


    PartitionTechnique(String partitionOpearation) {
    }
}
