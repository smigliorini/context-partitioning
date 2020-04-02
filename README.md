# Context based approach for partitioning Big data

## Configuration
Import the project from Intellij or any IDE compatible with a Java project

The project is a Maven based project, be sure to already have maven installed in your computer.
### Maven
Useful commands  
	- `mvn compile` compile and download all the dependecies  
	- `mvn clean` clean  
	- `mvn package` take the compiled code and package it, as a JAR  

The packaging does already make a Fat JAR with all dependecies of the project, thanks to pom.xml configuration.  
Note: IntelliJ does already have some shortcuts for compiling and cleaning the project with Maven.

### Custom Configuration
Custom Hadoop configuration are defined and loader from conf.xml file, where we find the used split size.

## Running application
### Prerequisites 
	- Hadoop installed
	- HOME_HADOOP coonfigured
	- JAR file exported in HADOOP_CLASSPATH
### Run commands
	 `hadoop mainPathPackage [-Dhadoopconf] [PARTITION_TECHNIQUE] inputFilePath [outputPath]`

PARTITION_TECHNIQUE:  
	- ML_GRID  
	- MD_GRID  
	- BOUX_COUNT    

By default the application use MD_GRID  


outputPath: path to the output directory. The directory is created from the application as Hadoop standard application, hence It has not to be already in the FS. When the output path is not provided the application automaticcally will create a an out directory in the input's directory.


##### Command Examples
Context based run running command:  
	- `hadoop it.univr.veronacard.VeronaMapReducePartitioner -Dmapred.reduce.tasks=110 test/test_partitioning/vc_ticket_space_time_transf.csv`  
Context based running command by passing hadoop standard reduce tasks configuration:  
	- `hadoop it.univr.veronacard.VeronaMapReducePartitioner -Dmapred.reduce.tasks=110 test/test_partitioning/vc_ticket_space_time_transf.csv`  
Minimum Bounding Box running run command:  
	- `hadoop it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce -Dmapred.reduce.tasks=110 test/test_partitioning/vc_ticket_space_time_transf.csv`  



