package it.univr.descriptors;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

//import edu.umn.cs.spatialHadoop.TigerShape;


/**
 * @author Alberto Belussi
 */
public class OneGrid extends Configured {
//implements Tool {

  protected static int minNumGriglie = 12;
  protected static int NumGriglie = 12;
  protected static long GridID = 1000000000;
  public String mbr;
  private int numReducers;
  private Path inputPath;
  private Path outputDir;
  private String inputType;
  private Double[] cellSide = new Double[10];
  private Integer dim;
  private String MoranIndex;

  public OneGrid( String[] args ) throws IOException, InterruptedException {
    if( args.length != 7 ) {
      //                                       0              1      2                                                             3                                  4                 5             6
      System.out.println( "Usage: OneGrid <WKT|CSV|CSVmulti> <dim> <mbr (Rectangle: (xmin,ymin,...)-(xmax,ymax,...) or compute)> <cell_Side (0.0 if mbr=compute)> <MoranIndex (MI|noMI)> <input_path> <output_path>" );
      System.out.println( "args.length = " + args.length );
      for( int i = 0; i < args.length; i++ )
        System.out.println( "args[" + i + "]" + " = " + args[i] );
      //System.out.println("       <mbr> form is ...");
      System.exit( 0 );
    }
    this.numReducers = 1;
    this.inputType = args[0];
    this.dim = new Integer( args[1] );
    this.mbr = new String( args[2] );
    //this.mbr = "Rectangle: (-0.01,-0.01)-(99.99,99.99)";
    this.MoranIndex = new String( args[4] );
    this.inputPath = new Path( args[5] );
    this.outputDir = new Path( args[6] );

    // gestire parametro mbr(compute)
    if( this.mbr.equals( "compute" ) ) {
      Configuration confMBR = new Configuration();
      confMBR.set( "mapreduce.framework.name", "local" );
      confMBR.set( "mapreduce.jobtracker.address", "local" );
      confMBR.set( "fs.defaultFS", "file:///" );
      confMBR.set( "shape", "wkt" );
      Partition p = FileMBR.fileMBR( this.inputPath, new OperationsParams( confMBR ) );

      double deltax, deltay;
      deltax = p.x2 - p.x1;
      deltay = p.y2 - p.y1;
      if( deltay > deltax ) deltax = deltay;
      else deltay = deltax;

      this.mbr = "Rectangle: (" + p.x1 + "," + p.y1 + ")-(" + ( p.x1 + deltax ) + "," + ( p.y1 + deltay ) + ")";
      this.cellSide[0] = p.getWidth() / 2;
      this.cellSide[1] = p.getHeight() / 2;
    } else {
      this.cellSide[0] = new Double( args[3] );
      this.cellSide[1] = this.cellSide[0];
    }

    if( this.inputType.equalsIgnoreCase( "WKT" ) ) {
      // Ricalcola la cellSize in modo da ottenere almeno minNunGriglie (11) griglie!
      // AB: raffino valore cellSide se non ottengo almeno 11 griglie

      Grid g = new Grid( mbr, cellSide[0] );
      if( ( this.cellSide[0] * ( Math.pow( 2, minNumGriglie ) ) ) > g.width ) {
        this.cellSide[0] = g.width / ( Math.pow( 2, minNumGriglie ) );
        this.cellSide[1] = g.height / ( Math.pow( 2, minNumGriglie ) );
      }
    } else {
      // La granularità minima la calcolo anche in base alla dimensione dello spazio
      System.out.println( "MBR: " + this.mbr + " cellSide[0]: " + cellSide[0] + " Dim: " + dim );
      NGrid ng = new NGrid( mbr, cellSide[0], dim );
      if( !ng.isValid )
        System.out.println( "NGrid non generata correttamente!" );
      //if ((this.cellSide*(Math.pow(2,minNumGriglie))) > g.size.get(0))
      int granularity = minNumGriglie;
      if( dim == 3 ) granularity = minNumGriglie - 0;
      if( dim == 4 ) granularity = minNumGriglie - 0;
      if( dim == 5 ) granularity = minNumGriglie - 1;

      for( int i = 0; i < dim; i++ ) {
        System.out.println( "ng.size[" + i + "]" + ng.size.get( i ) );
        this.cellSide[i] = ng.size.get( i ) / ( Math.pow( 2, granularity ) );
      }
    }
    System.out.println( "CellSide IN: " + this.cellSide[0] + " " + this.cellSide[1] + " "
                        + this.cellSide[2] + " " + this.cellSide[3] );
  }

  public static void main( String[] args ) throws Exception {
    System.setProperty( "hadoop.home.dir", "/Library/hadoop/hadoop-2.8.3/" );
    //System.out.println(System.getProperty("user.dir"));
    //int res = ToolRunner.run(new Configuration(), new OneGrid(args), args);
    double start = System.currentTimeMillis();
    System.out.println( "START: " + start );

    int res = run( args, new OneGrid( args ) );
    double end = System.currentTimeMillis();
    System.out.println( "END: " + end );
    System.out.println( "DURATION: " + ( end - start ) );
    System.exit( res );
  }

  /*public static class SIPartitioner
  extends Partitioner<IntWritable, Text> {

	  @Override
	  public int getPartition(IntWritable key, Text value, int numPartition) {
		return 0;

	  }
  }*/

  //@Override
  public static int run( String[] args, OneGrid o ) throws Exception {

    // define new job instead of null using conf
    Configuration conf = new Configuration();
    conf.set( "mapreduce.framework.name", "local" );
    conf.set( "mapreduce.jobtracker.address", "local" );
    conf.set( "fs.defaultFS", "file:///" );

    // Configuration conf = o.getConf();

    // TODO: Codice per settare un parametro da passare al mapper
    // NON la griglia ma bensì l'MBR calcolato con il precedente job.
    //
    // Usare:
    //	GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    //
    //Configuration conf = new Configuration();
    //conf.set("stop.words", "the, a, an, be, but, can");

    // passo il valore mbr per creare la griglia
    conf.set( "mbr", o.mbr );

    // passo lato cella
    for( int i = 0; i < o.dim; i++ )
      conf.setDouble( "cellSide" + i, o.cellSide[i] );

    // passo tipo di calcolo: oneGrid or multipleGrid
    conf.set( "type", "oneGrid" );

    // passo il nome dei file di input
    conf.set( "inputFile", o.inputPath.toString() );
    // passo il tipo di input CSV or WKT
    conf.set( "inputType", o.inputType );
    // passo il numero di dimensioni
    conf.setInt( "dim", o.dim );
    // passo il flag per il calcolo del Moran's Index
    conf.setBoolean( "MoranIndex", o.MoranIndex.equalsIgnoreCase( "MI" ) );

    // AB: togliere il seguente comando se non funziona
    conf.set( "mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t" );

    Job job = Job.getInstance( conf, "oneGrid" );
    job.setJarByClass( OneGrid.class );

    // set job input format
    job.setInputFormatClass( TextInputFormat.class );

    // set map class and the map output key and value classes
    job.setMapOutputKeyClass( LongWritable.class );
    job.setMapOutputValueClass( LongWritable.class );
    job.setMapperClass( Map.class );

    //set partitioner statement
    //job.setPartitionerClass(SIPartitioner.class);

    // set reduce class and the reduce output key and value classes
    job.setReducerClass( Reduce.class );

    // set job output format
    job.setOutputFormatClass( TextOutputFormat.class );
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( Text.class );


    // add the input file as job input (from HDFS) to the variable
    // inputFile
    TextInputFormat.setInputPaths( job, o.inputPath );

    // set the output path for the job results (to HDFS) to the variable
    // outputPath
    TextOutputFormat.setOutputPath( job, o.outputDir );

    // set the number of reducers using variable numberReducers
    job.setNumReduceTasks( o.numReducers );

    // set the jar class
    job.setJarByClass( OneGrid.class );

    return job.waitForCompletion( true ) ? 0 : 1; // this will execute the job
  }

  /*
   * Qui implemento il mapper.
   */
  public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    protected ArrayList<Grid> griglia = new ArrayList<Grid>( NumGriglie );
    protected ArrayList<NGrid> Ngriglia = new ArrayList<NGrid>( NumGriglie );
    protected ArrayList<HashMap<Long, Long>> boxCount = new ArrayList<HashMap<Long, Long>>( NumGriglie );
    protected Double[] cellSide = new Double[10];

    // statistiche
    protected Double areaAVG = 0.0;
    protected Double xAVG = 0.0, yAVG = 0.0;
    protected Double vertAVG = 0.0;
    protected long n = 0L;

    protected void setup( Context context )
      throws IOException, InterruptedException {
      // metodo in cui leggere l'MBR passato come parametro
      Configuration conf = context.getConfiguration();

      String type = conf.get( "type" );
      String mbr = conf.get( "mbr" );
      String inputType = conf.get( "inputType" );
      int dim = conf.getInt( "dim", 2 );
      for( int i = 0; i < dim; i++ )
        cellSide[i] = conf.getDouble( "cellSide" + i, 0.0 );
      System.out.println( "MAP Setup ..." );
      System.out.println( "CellSide MAP: " + this.cellSide[0] + " " + this.cellSide[1] + " "
                          + this.cellSide[2] + " " + this.cellSide[3] );

      if( inputType.equalsIgnoreCase( "WKT" ) ) {
        // CASO WKT
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          // caso griglia unica
          //System.out.println("CASO WKT--> mbr: " + mbr + "\ncs: " + cs);
          // creo la griglia
          griglia.add( new Grid( mbr, cellSide[0] ) );
          System.out.println( "CASO WKT MAP-setUp: griglia-> origine: " + griglia.get( 0 ).x + " " + griglia.get( 0 ).y +
                              " wh=" + griglia.get( 0 ).width + " hg=" + griglia.get( 0 ).height +
                              " cell size: " + griglia.get( 0 ).tileWidth + "," + griglia.get( 0 ).tileHeight +
                              " NumCol: " + griglia.get( 0 ).numColumns + " NumRow: " + griglia.get( 0 ).numRows );
          // creo la struttura per contenere i conteggi di intersezione per cella
          boxCount.add( new HashMap<Long, Long>() );
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          // caso griglie multiple
          // creo le griglie
          // AB 18/4/18: CORRETTO CICLO
          Grid g = new Grid( mbr, cellSide[0] );
          griglia.add( g );
          boxCount.add( new HashMap<Long, Long>() );
          double cs = cellSide[0];
          for( int i = 1; i < ( NumGriglie - dim + 2 ); i++ ) {
            // 23 maggio 2018
            // griglie generate sono le stesse del metodo oneGrid
						/* PRIMA ERA:
						   if (i<(NumGriglie/2))
							cs = w/(100*(NumGriglie/2) - 100*(i-1));
						else if (i<(NumGriglie-1))
							cs = w/(10*NumGriglie - 10*i);
						else
							cs = w/2;
						*/
            /* ORA E' */
            // ----------
            cs = cs * 2;
            // ----------
            g = new Grid( mbr, cs );
            griglia.add( g );
            System.out.println( "griglia [" + i + "] -> origine: " + griglia.get( i ).x + " " + griglia.get( i ).y +
                                " wh=" + griglia.get( i ).width + " " + griglia.get( i ).height +
                                " cell size: " + griglia.get( i ).tileWidth + "," + griglia.get( i ).tileHeight );
            boxCount.add( new HashMap<Long, Long>() );
          }
        }
      } else {
        // CASO CSV
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          // caso griglia unica
          //System.out.println("CASO CSV --> mbr: " + mbr + "\ncs: " + cs);
          // creo la Ngriglia
          NGrid ng = new NGrid( mbr, cellSide[0], dim );
          if( !ng.isValid ) {
            System.out.println( "Ngrid is not valid!" );
            return;
          }

          Ngriglia.add( new NGrid( mbr, cellSide[0], dim ) );
          System.out.println( "CASO CSV MAP-setUp: oneGrid Ngriglia-> origine: " +
                              Ngriglia.get( 0 ).orig.get( 0 ) + " " +
                              Ngriglia.get( 0 ).orig.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).orig.size() > 2 ? Ngriglia.get( 0 ).orig.get( 2 ) : "ND" ) + " " +
                              "size(1)=" + Ngriglia.get( 0 ).size.get( 0 ) + " " +
                              "size(2)=" + Ngriglia.get( 0 ).size.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).size.size() > 2 ? "size(3)=" + Ngriglia.get( 0 ).size.get( 2 ) : "ND" ) + " " +
                              "tileside=" + Ngriglia.get( 0 ).tileSide );
          boxCount.add( new HashMap<Long, Long>() );
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          // caso Ngriglie multiple
          // creo le Ngriglie
          NGrid ng = new NGrid( mbr, cellSide[0], dim );
          Ngriglia.add( ng );
          System.out.println( "CASO CSV MAP-setUp: multipleGrid Ngriglia-> origine: " +
                              Ngriglia.get( 0 ).orig.get( 0 ) + " " +
                              Ngriglia.get( 0 ).orig.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).orig.size() > 2 ? Ngriglia.get( 0 ).orig.get( 2 ) : "ND" ) + " " +
                              "size(1)=" + Ngriglia.get( 0 ).size.get( 0 ) + " " +
                              "size(2)=" + Ngriglia.get( 0 ).size.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).size.size() > 2 ? "size(3)=" + Ngriglia.get( 0 ).size.get( 2 ) : "ND" ) + " " +
                              "tileside=" + Ngriglia.get( 0 ).tileSide );
          boxCount.add( new HashMap<Long, Long>() );
          double cs = cellSide[0];
          for( int i = 1; i < ( NumGriglie - dim + 2 ); i++ ) {
            cs = cs * 2;
            ng = new NGrid( mbr, cs, dim );
            Ngriglia.add( ng );
            boxCount.add( new HashMap<Long, Long>() );
          }
        }
      }
    }

    /**
     * Il mapper si occupa di aggiornare un vettore dove ogni elemento
     * rappresenta una cella della griglia (rettangolo che contiene tutte le
     * geometrie). Se la figura letta si sovrappone alla griglia allora aggiungo
     * uno. Il vettore è passato al reducer per successivi calcoli.
     *
     * @param key     Chiave
     * @param value   Valore
     * @param context contesto
     * @throws IOException          Eccezione IO
     * @throws InterruptedException Eccezione
     */
    public void map( LongWritable key, Text value, Context context )
      throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      String type = conf.get( "type" );
      String inputType = conf.get( "inputType" );
      Geometry shape = null;
      NRectangle nrect = null;

      //System.out.println("Chiave: "+key.toString());

      if( inputType.equalsIgnoreCase( "WKT" ) ) {
        // leggo la geometria
        WKTReader wktread = new WKTReader();
        try {
          //System.out.println("Geometria: "+value.toString());
          shape = wktread.read( value.toString().trim() );
          n++;
          areaAVG = ( areaAVG * (double) ( n - 1 ) + shape.getArea() ) / n;
          xAVG = ( xAVG * (double) ( n - 1 ) + shape.getEnvelopeInternal().getWidth() ) / n;
          yAVG = ( yAVG * (double) ( n - 1 ) + shape.getEnvelopeInternal().getHeight() ) / n;
          vertAVG = ( vertAVG * (double) ( n - 1 ) + shape.getNumPoints() ) / n;
        } catch( ParseException e ) {
          e.printStackTrace();
        }
      } else {
        nrect = new NRectangle( value.toString().trim() );
        if( nrect.isValid ) {
          n++;
          areaAVG = ( areaAVG * (double) ( n - 1 ) + nrect.getNVol() ) / n;
          xAVG = ( xAVG * (double) ( n - 1 ) + nrect.getSize( 0 ) ) / n;
          yAVG = ( yAVG * (double) ( n - 1 ) + nrect.getSize( 1 ) ) / n;
          vertAVG = Math.pow( 2, nrect.getDim() );
        }
      }

      //System.out.println(">> shape: "+shape.toText());
      //shape.fromText(value);


      // Metodo AB:
      // Verifico celle intersecate dalla geometria shape
      // CASO: WKT
      Long[] intcells = null;
      if( inputType.equalsIgnoreCase( "WKT" ) ) {
        // WKT
        Grid gr = null;
        // caso monoGriglia
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          gr = griglia.get( 0 );
          //System.out.println("MAP-core: checking intersections...");
          if( shape != null )
            intcells = gr.overlapPartitions( shape );
          // aggiorno il boxCount
          //System.out.println("MAP-core: intersections found: "+intcells.length);
          HashMap<Long, Long> bc = boxCount.get( 0 );
          if( intcells != null ) {
            for( int i = 0; i < intcells.length; i++ )
              if( bc.get( intcells[i] ) == null )
                bc.put( intcells[i], 1L );
              else
                bc.put( intcells[i], bc.get( intcells[i] ) + 1L );
          }
          if( bc.containsKey( -1L ) ) bc.put( -1L, bc.get( -1L ) + 1L );
          else bc.put( -1L, 1L );
          //System.out.println("MAP-core: end");
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          // caso multiGriglia
          for( int i = 0; i < NumGriglie; i++ ) {
            gr = griglia.get( i );
            if( shape != null )
              intcells = gr.overlapPartitions( shape );
            HashMap<Long, Long> bc = boxCount.get( i );
            if( intcells != null ) {
              for( int j = 0; j < intcells.length; j++ )
                if( bc.get( intcells[j] ) == null )
                  bc.put( intcells[j], 1L );
                else
                  bc.put( intcells[j], bc.get( intcells[j] ) + 1L );
            }
          }
          // da controllare fine
        }
      } else {
        // CSV
        //System.out.println("CASO CSV MAP");
        if( nrect == null || !nrect.isValid ) {
          System.out.println( "CASO CSV MAP rectangle is not valid! "
                              + value.toString() );
          return;
        }
        NGrid ng = null;
				/* DEBUG
				   System.out.println("CASO CSV MAP rectangle: "+nrect.dim
						+" "+nrect.coordMin.get(0)
						+" "+nrect.coordMin.get(1)
						+" "+nrect.coordMin.get(2)
						+" "+nrect.coordMin.get(3));
				*/
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          ng = Ngriglia.get( 0 );
          intcells = ng.overlapPartitions( nrect );
          // aggiorno il boxCount
          //System.out.println("CASO CSV MAP oneGrid: num intersected cells "+intcells.length);
          HashMap<Long, Long> bc = boxCount.get( 0 );
          if( intcells != null ) {
            for( int i = 0; i < intcells.length; i++ )
              if( bc.get( intcells[i] ) == null )
                bc.put( intcells[i], 1L );
              else
                bc.put( intcells[i], bc.get( intcells[i] ) + 1L );
          }
          if( bc.containsKey( -1L ) ) bc.put( -1L, bc.get( -1L ) + 1L );
          else bc.put( -1L, 1L );
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          // caso multiGriglia
          for( int i = 0; i < NumGriglie; i++ ) {
            ng = Ngriglia.get( i );
            intcells = ng.overlapPartitions( nrect );
            //System.out.println("CASO CSV MAP multipleGrid: num intersected cells "+intcells.length);
            HashMap<Long, Long> bc = boxCount.get( i );
            if( intcells != null ) {
              for( int j = 0; j < intcells.length; j++ )
                if( bc.get( intcells[j] ) == null )
                  bc.put( intcells[j], 1L );
                else
                  bc.put( intcells[j], bc.get( intcells[j] ) + 1L );
            }
          }
        }
      }
    }

    protected void cleanup( Context context )
      throws IOException, InterruptedException {
      LongWritable key, value;
      Configuration conf = context.getConfiguration();
      String type = conf.get( "type" );
      System.out.println( "MAP-cleanUP: start" );
      if( type.equalsIgnoreCase( "oneGrid" ) ) {
        HashMap<Long, Long> bc = boxCount.get( 0 );
        for( Long k : bc.keySet() ) {
          key = new LongWritable( k );
          value = new LongWritable( bc.get( k ) );
          context.write( key, value );
        }
      } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
        for( int i = 0; i < NumGriglie; i++ ) {
          HashMap<Long, Long> bc = boxCount.get( i );
          for( Long k : bc.keySet() ) {
            key = new LongWritable( k + GridID * i );
            value = new LongWritable( bc.get( k ) );
            context.write( key, value );
          }
        }
      }

      // salvo areaAVG come un long:
      // sfruttando il metodo doubleToLongBits (key=-2)
      System.out.println( "MAP: area media: "
                          + areaAVG.toString() + " per n(" + n + "): " + ( areaAVG * n ) );
      key = new LongWritable( -2L );
      value = new LongWritable( Double.doubleToLongBits( areaAVG * n ) );
      context.write( key, value );

      // salvo xAVG,yAVG come un long:
      // sfruttando il metodo doubleToLongBits (key=-3 e -4 e -5)
      key = new LongWritable( -3L );
      value = new LongWritable( Double.doubleToLongBits( xAVG * n ) );
      context.write( key, value );
      key = new LongWritable( -4L );
      value = new LongWritable( Double.doubleToLongBits( yAVG * n ) );
      context.write( key, value );
      key = new LongWritable( -5L );
      value = new LongWritable( Double.doubleToLongBits( vertAVG * n ) );
      context.write( key, value );

      // salvo n come un long (key=-6)
      key = new LongWritable( -6L );
      value = new LongWritable( n );
      context.write( key, value );

      System.out.println( "MAP-cleanUP: end" );
    }
  }

  /*
   * Qui implemento il reducer.
   */
  public static class Reduce extends Reducer<LongWritable, LongWritable, Text, Text> {
    //private Text result;
    protected ArrayList<Grid> griglia = new ArrayList<Grid>( NumGriglie );
    protected ArrayList<NGrid> Ngriglia = new ArrayList<NGrid>( NumGriglie );
    protected ArrayList<HashMap<Long, Long>> boxCount = new ArrayList<HashMap<Long, Long>>( NumGriglie );
    protected Double[] cellSide = new Double[10];

    protected HashMap<Double, Double> D0;
    protected HashMap<Double, Double> D2;
    protected HashMap<Double, Double> D3;
    protected HashMap<Double, Double> M0;
    protected HashMap<Double, Double> M2;
    protected HashMap<Double, Double> M1;
    protected HashMap<Double, Double> Ncell;

    protected ArrayList<HashMap<Double, Double>> D0_dim;
    protected ArrayList<HashMap<Double, Double>> D2_dim;
    protected ArrayList<HashMap<Double, Double>> Ncell_dim;

    protected String inputFile;
    protected Long reduceStart, reduceEnd;

    // statistiche
    protected Double areaAVG = 0.0;
    protected Double xAVG = 0.0, yAVG = 0.0;
    protected Double vertAVG = 0.0;
    protected long n = 0L;
    protected boolean MoranIndex;

    protected void setup( Context context ) throws IOException, InterruptedException {
      // metodo in cui leggere l'MBR passato come parametro
      Configuration conf = context.getConfiguration();

      reduceStart = System.currentTimeMillis();
      String type = conf.get( "type" );
      String mbr = conf.get( "mbr" );
      MoranIndex = conf.getBoolean( "MoranIndex", false );
      String inputType = conf.get( "inputType" );
      inputFile = conf.get( "inputFile" );
      int dim = conf.getInt( "dim", 2 );
      for( int i = 0; i < dim; i++ )
        cellSide[i] = conf.getDouble( "cellSide" + i, 0.0 );
      System.out.println( "REDUCE Setup ..." );
      System.out.println( "CellSide REDUCE: " + this.cellSide[0] + " " + this.cellSide[1] + " "
                          + this.cellSide[2] + " " + this.cellSide[3] );


      if( inputType.equalsIgnoreCase( "WKT" ) ) {
        // CASO WKT
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          // caso griglia unica
          //System.out.println("CASO WKT--> mbr: " + mbr + "\ncs: " + cs);
          // creo la griglia
          griglia.add( new Grid( mbr, cellSide[0] ) );
          System.out.println( "CASO WKT REDUCE-setUp: griglia-> origine: " + griglia.get( 0 ).x + " " + griglia.get( 0 ).y +
                              " wh=" + griglia.get( 0 ).width + " " + griglia.get( 0 ).height +
                              " cell size: " + griglia.get( 0 ).tileWidth + "," + griglia.get( 0 ).tileHeight );
          // creo la struttura per contenere i conteggi di intersezione per cella
          boxCount.add( new HashMap<Long, Long>() );
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          // caso griglie multiple
          // creo le griglie
          // AB 18/4/18: CORRETTO CICLO
          Grid g = new Grid( mbr, cellSide[0] );
          griglia.add( g );
          boxCount.add( new HashMap<Long, Long>() );
          double cs = cellSide[0];
          for( int i = 1; i < NumGriglie; i++ ) {
            // 23 maggio 2018
            // griglie generate sono le stesse del metodo oneGrid
						/* PRIMA ERA:
						   if (i<(NumGriglie/2))
							cs = w/(100*(NumGriglie/2) - 100*(i-1));
						else if (i<(NumGriglie-1))
							cs = w/(10*NumGriglie - 10*i);
						else
							cs = w/2;
						*/
            /* ORA E' */
            // ----------
            cs = cs * 2;
            // ----------
            g = new Grid( mbr, cs );
            griglia.add( g );
            System.out.println( "griglia [" + i + "] -> origine: " + griglia.get( i ).x + " " + griglia.get( i ).y +
                                " wh=" + griglia.get( i ).width + " " + griglia.get( i ).height +
                                " cell size: " + griglia.get( i ).tileWidth + "," + griglia.get( i ).tileHeight );
            boxCount.add( new HashMap<Long, Long>() );
          }
        }
      } else {
        // CASO CSV
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          // caso griglia unica
          //System.out.println("CASO CSV--> mbr: " + mbr + "\ncs: " + cs);
          // creo la Ngriglia
          Ngriglia.add( new NGrid( mbr, cellSide[0], dim ) );
          System.out.println( "CASO CSV REDUCE-setUp: oneGrid Ngriglia-> origine: " +
                              Ngriglia.get( 0 ).orig.get( 0 ) + " " +
                              Ngriglia.get( 0 ).orig.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).orig.size() > 2 ? Ngriglia.get( 0 ).orig.get( 2 ) : "ND" ) + " " +
                              "size(1)=" + Ngriglia.get( 0 ).size.get( 0 ) + " " +
                              "size(2)=" + Ngriglia.get( 0 ).size.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).size.size() > 2 ? "size(3)=" + Ngriglia.get( 0 ).size.get( 2 ) : "ND" ) + " " +
                              "tileside=" + Ngriglia.get( 0 ).tileSide );
          boxCount.add( new HashMap<Long, Long>() );
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          // caso Ngriglie multiple
          // creo le Ngriglie
          NGrid ng = new NGrid( mbr, cellSide[0], dim );
          Ngriglia.add( ng );
          System.out.println( "CASO CSV REDUCE-setUp: multipleGrid Ngriglia-> origine: " +
                              Ngriglia.get( 0 ).orig.get( 0 ) + " " +
                              Ngriglia.get( 0 ).orig.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).orig.size() > 2 ? Ngriglia.get( 0 ).orig.get( 2 ) : "ND" ) + " " +
                              "size(1)=" + Ngriglia.get( 0 ).size.get( 0 ) + " " +
                              "size(2)=" + Ngriglia.get( 0 ).size.get( 1 ) + " " +
                              ( Ngriglia.get( 0 ).size.size() > 2 ? "size(3)=" + Ngriglia.get( 0 ).size.get( 2 ) : "ND" ) + " " +
                              "tileside=" + Ngriglia.get( 0 ).tileSide );
          boxCount.add( new HashMap<Long, Long>() );
          double cs = cellSide[0];
          for( int i = 1; i < NumGriglie; i++ ) {
            cs = cs * 2;
            ng = new NGrid( mbr, cs, dim );
            Ngriglia.add( ng );
            boxCount.add( new HashMap<Long, Long>() );
          }
        }
      }
    }

    /**
     * Il reducer (unico per questo job) riceve un vettore di cui dovr� sommare
     * gli elementi corrispondenti. Il risultato intermedio sar� un nuovo
     * vettore contenente per ogni cella il totale di geometrie sovrapposte.
     * Dovrebbe alla fine calcolare la D0 e la D2
     *
     * @param key     Chiave
     * @param values  Valore
     * @param context Contesto
     * @throws IOException          Eccezione IO
     * @throws InterruptedException Eccezione
     */
    public void reduce( LongWritable key, Iterable<LongWritable> values, Context context )
      throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();

      String type = conf.get( "type" );

      long count = 0L;
      if( ( key.get() >= -1L ) || ( key.get() == -6 ) ) {
        // sommo i valori
        for( LongWritable v : values )
          count += v.get();
        if( key.get() == -6 )
          n = count;
      } else {
        double countd = 0.0;
        for( LongWritable v : values )
          countd += Double.longBitsToDouble( v.get() );
        switch( (int) key.get() ) {
          case -5:
            vertAVG = countd;
            break;
          case -4:
            yAVG = countd;
            break;
          case -3:
            xAVG = countd;
            break;
          case -2:
            areaAVG = countd;
            break;
        }
      }

      if( key.get() >= -1L ) {
        Long k, c;
        if( type.equalsIgnoreCase( "oneGrid" ) ) {
          k = new Long( key.get() );
          c = new Long( count );
          HashMap<Long, Long> bc = boxCount.get( 0 );
          bc.put( k, c );
        } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
          k = new Long( key.get() );
          c = new Long( count );
          HashMap<Long, Long> bc = boxCount.get( (int) ( k / GridID ) );
          bc.put( k % GridID, c );
        }
      }
		  /*
		  int tot = 0;
		  for(i = 0; i < somme.length; i++)
			  tot += somme[i];

		  // stampo il risultato
		  for(i = 0; i < somme.length; i++)
		  {	//[k]= v
			  Text k = new Text("["+i+"]=");
			  Text v = new Text(""+somme[i]);
			  context.write(k, v);
		  }
		  context.write(new Text("TOT: "), new Text(""+tot));

		  // Qui ho il vettore con i totali
		  */
    }

    // CLEAN UP METHOD ---------------------------------------------------------------------
    protected void cleanup( Context context )
      throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      String type = conf.get( "type" );
      String inputType = conf.get( "inputType" );
      String mbr = conf.get( "mbr" );
      int dim = conf.getInt( "dim", 2 );
      System.out.println( "REDUCE cleanUP: start..." );
      System.out.println( "CellSide REDUCE: " + this.cellSide[0] + " " + this.cellSide[1] + " "
                          + this.cellSide[2] + " " + this.cellSide[3] );

      final long maxIterations = 5000000L;

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // creo le liste di valori per calcolare la regressione lineare e
        // quindi ottenere dalla slope D0, D2 e D3
        D0 = new HashMap<Double, Double>();
        D2 = new HashMap<Double, Double>();
        D3 = new HashMap<Double, Double>();
        if( MoranIndex ) {
          M0 = new HashMap<Double, Double>();
          M2 = new HashMap<Double, Double>();
          M1 = new HashMap<Double, Double>();
        }
        Ncell = new HashMap<Double, Double>();
      } else {
        D0_dim = new ArrayList<HashMap<Double, Double>>();
        D2_dim = new ArrayList<HashMap<Double, Double>>();
        Ncell_dim = new ArrayList<HashMap<Double, Double>>();
        for( int i = 0; i < dim; i++ ) {
          D0_dim.add( new HashMap<Double, Double>() );
          D2_dim.add( new HashMap<Double, Double>() );
          Ncell_dim.add( new HashMap<Double, Double>() );
        }
      }

      // Prima griglia uguale per i due metodi oneGrid e multipleGrid
      Grid gr = null;
      NGrid ng = null;
      if( inputType.equalsIgnoreCase( "WKT" ) )
        gr = griglia.get( 0 );
      else
        ng = Ngriglia.get( 0 );

      HashMap<Long, Long> bc = boxCount.get( 0 );
      long card = bc.get( -1L );
      bc.remove( -1L );

      // DEBUG
		  /* FOR debugging you need to set the OutputType to LongWritable,LongWritable instead of
		   * Text,Text
		   *
		  for (Long k: bc.keySet()) {
			  key = new LongWritable(k);
			  value = new LongWritable(bc.get(k));
			  context.write(key,value);
		  }
		  */

      // First value in D0, D2 e D3

      double N;
      if( inputType.equalsIgnoreCase( "WKT" ) )
        N = gr.numTiles;
      else
        N = ng.numTiles;

		  /* Nov 2019: le dimensioni della cella sono già corrette come impostate inizialmente
		   * quindi il codice seguente viene commentato
		  double[] cellSide = new double[10];
		  if (inputType.equalsIgnoreCase("WKT")) {
			  cellSide[0] = gr.tileWidth;
			  cellSide[1] = gr.tileHeight;
		  } else {
			  for(int i=0; i<dim; i++)
				  cellSide[i] = ng.size.get(i);
		  }
		  */

      // Variabili per il calcolo nel caso WKT e CSV
      double S0 = 0.0, S1 = 0.0, S2 = 0.0, S3 = 0.0;
      double W = 0;
      double NUM0 = 0, NUM2 = 0;
      //NUM3 = 0,
      double DEN0 = 0, DEN2 = 0;
      //DEN3 = 0;
      double NUM1 = 0, DEN1 = 0;
      double x_j, x0_i, x0_j, x2_i, x2_j; //, x3_i, x3_j;
      double x1_i;
      long row, col, kad;

      double MI_val0 = 0.0;
      double MI_val1 = 0.0;
      double MI_val2 = 0.0;
      double maxCount = 0.0, minCount = 0.0;
      // ----- fine Variabili per il calcolo nel caso WKT e CSV
      // ----- Variabili per il calcolo nel caso CSVmulti
      ArrayList<HashMap<Long, Long>> bc_dim = new ArrayList<HashMap<Long, Long>>();
      for( int i = 0; i < dim; i++ )
        bc_dim.add( new HashMap<Long, Long>() );
      double[] S0_dim = new double[10];
      double[] S2_dim = new double[10];

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        S0 = bc.size();
        //System.out.println("Griglia 1 ---> S0["+cellSide[0]+","+cellSide[1]+"...]= "+S0);
        S1 = 0.0;
        S2 = 0.0;
        S3 = 0.0;
        for( Long k : bc.keySet() ) {
          S1 += bc.get( k );
          S2 += Math.pow( bc.get( k ), 2 );
          S3 += Math.pow( bc.get( k ), 3 );
        }
      } else {
        for( int i = 0; i < dim; i++ ) {
          S0_dim[i] = 0.0;
          S2_dim[i] = 0.0;
        }
        ArrayList<Long> coord;
        for( Long k : bc.keySet() ) {
          // genero le coordinate della cella
          //System.out.println("BoxCount "+k+" = "+bc.get(k));
          coord = ng.getCellCoord( k );
          for( int i = 0; i < dim; i++ ) {
            HashMap<Long, Long> bcc = bc_dim.get( i );
            if( bcc.containsKey( coord.get( i ) ) )
              bcc.put( coord.get( i ), bcc.get( coord.get( i ) ) + bc.get( k ) );
            else
              bcc.put( coord.get( i ), bc.get( k ) );
          }
        }
        for( int i = 0; i < dim; i++ ) {
          HashMap<Long, Long> bcc = bc_dim.get( i );
          S0_dim[i] = bcc.size();
          for( Long k : bcc.keySet() ) {
            S2_dim[i] += Math.pow( bcc.get( k ), 2 );
          }
          System.out.println( "Griglia 1 ---> Dim " + i + " S0[" + cellSide[0] + "," + cellSide[1] + "...]= " + S0_dim[i] );
          System.out.println( "Griglia 1 ---> Dim " + i + " S2[" + cellSide[0] + "," + cellSide[1] + "...]= " + S2_dim[i] );
        }
      }
      //double MI_val3 = 0.0;
      //System.exit(1);

      // i valori di cellSize vanno normalizzati rispetto alla griglia
      // dividendo cellSize per gr.width
      double width;
      if( inputType.equalsIgnoreCase( "WKT" ) )
        width = gr.width;
      else
        // attenzione uso la larghezza della griglia sulla prima dimensione (coordinata x o prima coordinata)
        // anche se la griglia potrebbe non essere un nCubo: in realtà è quasi sempre un nCubo
        width = ng.size.get( 0 );

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // introdotte celle rettangolari: per il parametro r usiamo la width
        D0.put( Math.log( cellSide[0] / width ), Math.log( S0 ) );
        D2.put( Math.log( cellSide[0] / width ), Math.log( S2 ) );
        D3.put( Math.log( cellSide[0] / width ), Math.log( S3 ) );
        Ncell.put( Math.log( cellSide[0] / width ), ( N - S0 ) / N );
			  /*salto la prima griglia perchè troppo oneroso il calcolo
			  M0.put((cellSize/gr.width), MI_val0);
			  M2.put((cellSize/gr.width), MI_val2);
			  M3.put((cellSize/gr.width), MI_val3);
			  */
      } else {
        for( int i = 0; i < dim; i++ ) {
          ( D0_dim.get( i ) ).put( Math.log( cellSide[0] / width ), Math.log( S0_dim[i] ) );
          System.out.println( "Dim0_dim[" + i + "] = " + D0_dim.get( i ) );
          ( D2_dim.get( i ) ).put( Math.log( cellSide[0] / width ), Math.log( S2_dim[i] ) );
          System.out.println( "Dim2_dim[" + i + "] = " + D0_dim.get( i ) );
        }
      }

      if( type.equalsIgnoreCase( "oneGrid" ) ) {
        // Valori successivi in D0, D2 e D3
        // Copio bc in boxCountPrev
        HashMap<Long, Long> boxCountPrev = new HashMap<Long, Long>();
        HashMap<Long, Long> boxCountNew = new HashMap<Long, Long>();
        for( Long k : bc.keySet() ) {
          boxCountNew.put( k, bc.get( k ) );
        }
        Long knew; //DEBUG int i=1;
        NGrid ngprev, ngnew = ng;
        Grid gprev, gnew = gr;
        int numgrid = 2;
        double[] cs = new double[10];
        for( int i = 0; i < dim; i++ ) {
          cs[i] = cellSide[i];
          System.out.println( "cs[" + i + "] = " + cs[i] );
        }
        System.out.println( "width = " + width );
        while( cs[0] * 2 < width ) {
          gprev = gnew;
          ngprev = ngnew;
          boxCountPrev.clear();
          for( Long k : boxCountNew.keySet() ) {
            boxCountPrev.put( k, boxCountNew.get( k ) );
          }
          // nuova griglia
          boxCountNew.clear();
          // introdotte celle rettangolari
          // cellWidth = cellWidth*2;
          // cellHeight = cellHeight*2;
          for( int i = 0; i < dim; i++ )
            cs[i] = cs[i] * 2;

          if( inputType.equalsIgnoreCase( "WKT" ) )
            // il metodo costruttore Grid qui invocato tiene già conto delle celle rettangolari
            gnew = new Grid( mbr, cs[0] );
          else
            ngnew = new NGrid( mbr, cs[0], dim );

          System.out.println( "NewGrid " + numgrid + " [" + cs[0] + "," + cs[1] + ", ...]" );

          for( Long k : boxCountPrev.keySet() ) {
            if( inputType.equalsIgnoreCase( "WKT" ) )
              knew = gnew.getCellId( gprev.getRow( k ) / 2 + gprev.getRow( k ) % 2, gprev.getCol( k ) / 2 + gprev.getCol( k ) % 2 );
            else {
              ArrayList<Long> cell = ngprev.getCellCoord( k );
              //if (numgrid == 12)
              //	  System.out.println("Old cell ["+k+"]: "+cell.get(0)+" "+cell.get(1)+" "+cell.get(2));
              // calcolo in quale cella della nuova griglia cade cell
              for( int i = 0; i < dim; i++ )
                cell.set( i, ( (long) ( cell.get( i ) / 2 ) ) + ( (long) cell.get( i ) % 2 ) );

              knew = ngnew.getCellNumber( cell );
              //if (numgrid == 12)
              //	  System.out.println("New cell ["+knew+"]: "+cell.get(0)+" "+cell.get(1)+" "+cell.get(2));
            }

            if( boxCountNew.containsKey( knew ) )
              boxCountNew.put( knew, boxCountNew.get( knew ) + boxCountPrev.get( k ) );
            else
              boxCountNew.put( knew, boxCountPrev.get( k ) );
          }
          // DEBUG
				  /* FOR debugging you need to set the OutputType to LongWritable,LongWritable instead of
				   * Text,Text
				   *
				  key = new LongWritable(-1);
				  value = new LongWritable(i++);
				  context.write(key,value);
				  for (Long k: boxCountNew.keySet()) {
					  key = new LongWritable(k);
					  value = new LongWritable(boxCountNew.get(k));
					  context.write(key,value);
				  }
				  */
          // --------------------------------------------
          // fine DEBUG

          if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
            // compute S0, S2 e S3
            S0 = boxCountNew.size();
            System.out.println( "Griglia " + numgrid + "---> S0[" + cs[0] + "," + cs[1] + ", ...]= " + S0 );
            numgrid++;
            S2 = 0.0;
            S3 = 0.0;
            S1 = 0.0;

            if( inputType.equalsIgnoreCase( "WKT" ) )
              N = gnew.numTiles;
            else
              N = ngnew.numTiles;

            maxCount = 0.0;
            minCount = 1000000.0;
            for( Long k : boxCountNew.keySet() ) {
              S1 += boxCountNew.get( k );
              S2 += Math.pow( boxCountNew.get( k ), 2 );
              S3 += Math.pow( boxCountNew.get( k ), 3 );
              minCount = ( minCount > boxCountNew.get( k ) ? boxCountNew.get( k ) : minCount );
              maxCount = ( maxCount < boxCountNew.get( k ) ? boxCountNew.get( k ) : maxCount );
            }
          } else {
            // CSVmulti
            // Reinizializzo ogni volta i totali e gli istogrammi bcNew_dim
            for( int i = 0; i < dim; i++ ) {
              S0_dim[i] = 0.0;
              S2_dim[i] = 0.0;
            }
            ArrayList<HashMap<Long, Long>> bcNew_dim =
              new ArrayList<HashMap<Long, Long>>();
            for( int i = 0; i < dim; i++ )
              bcNew_dim.add( new HashMap<Long, Long>() );

            ArrayList<Long> coord;
            for( Long k : boxCountNew.keySet() ) {
              // genero le coordinate della cella
              coord = ngnew.getCellCoord( k );
              for( int i = 0; i < dim; i++ ) {
                HashMap<Long, Long> bccNew = bcNew_dim.get( i );
                if( bccNew.containsKey( coord.get( i ) ) )
                  bccNew.put( coord.get( i ), bccNew.get( coord.get( i ) ) + boxCountNew.get( k ) );
                else
                  bccNew.put( coord.get( i ), boxCountNew.get( k ) );
              }
            }
            for( int i = 0; i < dim; i++ ) {
              HashMap<Long, Long> bccNew = bcNew_dim.get( i );
              S0_dim[i] = bccNew.size();
              for( Long k : bccNew.keySet() ) {
                S2_dim[i] += Math.pow( bccNew.get( k ), 2 );
              }
            }
          }

          // Calcolo Moran's index
          if( MoranIndex ) {
            if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
              // intervallo classi
              double SClassi = 0.0, avgClassi = 0.0;
              // compute MI
              final int num_classi = 10;
              double deltaClassi = ( maxCount - minCount ) / num_classi;

              for( Long k : boxCountNew.keySet() ) {
                SClassi += ( Math.ceil( ( boxCountNew.get( k ) - minCount ) / deltaClassi ) == 0 ? 1 : Math.ceil( ( boxCountNew.get( k ) - minCount ) / deltaClassi ) );
              }

              double avg0, avg1, avg2, avg3;
              avgClassi = SClassi / N;
              avg0 = S0 / N;
              avg1 = S1 / N;
              avg2 = S2 / N;
              avg3 = S3 / N;

              // Modifica del 28/03/2019
              if( avg0 == 1 ) avg0 = 0.999;
              if( avg1 == 1 ) avg1 = 0.999;
              // fine modifica del 28/3/2019

              System.out.println( "REDUCE-cleanUP: card -> " + card + " avg0-> " + avg0 + " avg1-> " + avg1 + " avg2-> " + avg2 + " avg3-> " + avg3 + " for N-> " + N );
              System.out.println( "REDUCE-cleanUP: deltaClassi: " + deltaClassi + " Avgclassi: " + avgClassi );
              System.out.println( "REDUCE-cleanUP: start computation Moran's Index..." );
              W = 0.0;
              NUM0 = 0.0;
              NUM2 = 0.0;
              //NUM3 = 0.0;
              DEN0 = 0.0;
              DEN2 = 0.0;
              //DEN3 = 0.0;
              NUM1 = 0.0;
              DEN1 = 0.0;
              double maxrow = 1.0, maxcol = 1.0;
              ArrayList<Long> cell = null, cellnew = null;
              if( inputType.equalsIgnoreCase( "WKT" ) ) {
                maxrow = gnew.numRows;
                maxcol = gnew.numColumns;
              }

              boolean salto = ( N > maxIterations ); //salto la prima griglia per il calcolo dell'indice Moran

              if( salto )
                System.out.println( "Salto calcolo Moran's index: N troppo alto: " + N );
              else {
                for( Long k = 0L; k < N; k++ ) {
                  // DEN computation
                  x0_i = ( boxCountNew.containsKey( k ) ? 1.0 : 0.0 );
                  DEN0 += Math.pow( ( x0_i - avg0 ), 2 );

                  // calcolo senza elevare al quadrato o cubo
                  x1_i = ( boxCountNew.containsKey( k ) ? boxCountNew.get( k ) : 0.0 );
                  DEN1 += Math.pow( ( x1_i - avg1 ), 2 );
                  // fine - calcolo senza elevare al quadrato o cubo

                  // Quadrato
                  //x2_i = (boxCountNew.containsKey(k)?Math.pow(boxCountNew.get(k),2):0.0);
                  //DEN2 += Math.pow((x2_i - avg2),2);

                  // Divisione in 10 classi
                  x2_i = ( boxCountNew.containsKey( k ) ? Math.ceil( ( boxCountNew.get( k ) - minCount ) / deltaClassi ) : 1 );
                  if( x2_i == 0 ) x2_i = 1;
                  DEN2 += Math.pow( ( x2_i - avgClassi ), 2 );

                  // Cubo
                  //x3_i = (boxCountNew.containsKey(k)?Math.pow(boxCountNew.get(k),3):0.0);
                  //DEN3 += Math.pow((x3_i - avg3),2);

                  // current CELL
                  row = 0L;
                  col = 0L;
                  if( inputType.equalsIgnoreCase( "WKT" ) ) {
                    // position (row,col)
                    row = gnew.getRow( k );
                    col = gnew.getCol( k );
                  } else
                    // position cell
                    cell = ngnew.getCellCoord( k );
                  // debug
								  /*
								  if (x0_i == 1) {
								  	  System.out.println("Current cell("+k+")->["+row+","+col+"]");
								  } */
                  // end debug
                  // NUM computation
                  if( inputType.equalsIgnoreCase( "WKT" ) ) {
                    // posizione (-1, 0)
                    if( ( row - 1L ) > 0L ) {
                      kad = gnew.getCellId( ( row - 1L ), col );
                      x_j = ( boxCountNew.containsKey( kad ) ? boxCountNew.get( kad ) : 0.0 );
                      x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                      NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                      // calcolo senza elevare al quadrato o cubo
                      NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                      // fine - calcolo senza elevare al quadrato o cubo

                      // Quadrato
                      //x2_j = Math.pow(x_j,2);
                      //NUM2 += (x2_i - avg2)*(x2_j - avg2);

                      // Divisione in 10 classi
                      x2_j = Math.ceil( ( x_j - minCount ) / deltaClassi );
                      if( x2_j == 0 ) x2_j = 1;
                      NUM2 += ( x2_i - avgClassi ) * ( x2_j - avgClassi );

                      // Cubo
                      //x3_j = Math.pow(x_j,3);
                      //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                      W++;
                      //debug
										 /*if (x0_i == 1.0) {
										 	  System.out.println("Cell ("+k+") - Adjacent cell("+kad+")->["+(row-1L)+","+col+"]");
										 	  System.out.println("x_j: "+x_j);
											  System.out.println("x2_j: "+x2_j);
										 	  System.out.println("Num1: "+(x1_i - avg1)*(x_j - avg1)+" Num2: "+(x2_i - avgClassi)*(x2_j - avgClassi));
										 	  System.out.println("NUM1: "+NUM1+" NUM2: "+NUM2);
										 }*/
                    }
                    // posizione (0,-1)
                    if( ( col - 1L ) > 0L ) {
                      kad = gnew.getCellId( row, col - 1L );
                      x_j = ( boxCountNew.containsKey( kad ) ? boxCountNew.get( kad ) : 0.0 );
                      x0_j = ( x_j > 0 ? 1 : 0 );
                      NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                      // calcolo senza elevare al quadrato o cubo
                      NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                      // fine - calcolo senza elevare al quadrato o cubo

                      // Quadrato
                      //x2_j = Math.pow(x_j,2);
                      //NUM2 += (x2_i - avg2)*(x2_j - avg2);

                      // Divisione in 10 classi
                      x2_j = Math.ceil( ( x_j - minCount ) / deltaClassi );
                      if( x2_j == 0 ) x2_j = 1;
                      NUM2 += ( x2_i - avgClassi ) * ( x2_j - avgClassi );

                      // Cubo
                      //x3_j = Math.pow(x_j,3);
                      //NUM3 += (x3_i - avg3)*(x3_j - avg3);

                      W++;
                      // debug
										 /*if (x0_i == 1.0) {
										 	  System.out.println("Cell ("+k+") - Adjacent cell("+kad+")->["+row+","+(col-1L)+"]");
										 	  System.out.println("x_j: "+x_j);
										 	  System.out.println("x2_j: "+x2_j);
										 	  System.out.println("Num1: "+(x1_i - avg1)*(x_j - avg1)+" Num2: "+(x2_i - avgClassi)*(x2_j - avgClassi));
										 	  System.out.println("NUM1: "+NUM1+" NUM2: "+NUM2);
										 }*/
                    }
                    // posizione (1, 0)
                    if( ( row + 1L ) <= maxrow ) {
                      kad = gnew.getCellId( row + 1L, col );
                      x_j = ( boxCountNew.containsKey( kad ) ? boxCountNew.get( kad ) : 0.0 );
                      x0_j = ( x_j > 0 ? 1 : 0 );
                      NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                      // calcolo senza elevare al quadrato o cubo
                      NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                      // fine - calcolo senza elevare al quadrato o cubo

                      // Quadrato
                      //x2_j = Math.pow(x_j,2);
                      //NUM2 += (x2_i - avg2)*(x2_j - avg2);

                      // Divisione in 10 classi
                      x2_j = Math.ceil( ( x_j - minCount ) / deltaClassi );
                      if( x2_j == 0 ) x2_j = 1;
                      NUM2 += ( x2_i - avgClassi ) * ( x2_j - avgClassi );

                      // Cubo
                      //x3_j = Math.pow(x_j,3);
                      //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                      W++;
                      // debug
										 /*if (x0_i == 1) {
										 	  System.out.println("Cell ("+k+") - Adjacent cell("+kad+")->["+(row+1L)+","+col+"]");
										 	  System.out.println("x_j: "+x_j);
										 	  System.out.println("x2_j: "+x2_j);
										 	  System.out.println("Num1: "+(x1_i - avg1)*(x_j - avg1)+" Num2: "+(x2_i - avgClassi)*(x2_j - avgClassi));
										 	  System.out.println("NUM1: "+NUM1+" NUM2: "+NUM2);
										 }*/
                    }
                    // posizione (0,1)
                    if( ( col + 1L ) <= maxcol ) {
                      kad = gnew.getCellId( row, ( col + 1L ) );
                      x_j = ( boxCountNew.containsKey( kad ) ? boxCountNew.get( kad ) : 0.0 );
                      x0_j = ( x_j > 0 ? 1 : 0 );
                      NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                      // calcolo senza elevare al quadrato o cubo
                      NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                      // fine - calcolo senza elevare al quadrato o cubo

                      // Quadrato
                      //x2_j = Math.pow(x_j,2);
                      //NUM2 += (x2_i - avg2)*(x2_j - avg2);

                      // Divisione in 10 classi
                      x2_j = Math.ceil( ( x_j - minCount ) / deltaClassi );
                      if( x2_j == 0 ) x2_j = 1;
                      NUM2 += ( x2_i - avgClassi ) * ( x2_j - avgClassi );

                      // Cubo
                      //x3_j = Math.pow(x_j,3);
                      //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                      W++;
                      // debug
										 /*if (x0_i == 1) {
										 	  System.out.println("Cell ("+k+") - Adjacent cell("+kad+")->["+row+","+(col+1L)+"]");
										 	  System.out.println("x_j: "+x_j);
										 	  System.out.println("x2_j: "+x2_j);
											  System.out.println("Num1: "+(x1_i - avg1)*(x_j - avg1)+" Num2: "+(x2_i - avgClassi)*(x2_j - avgClassi));
										 	  System.out.println("NUM1: "+NUM1+" NUM2: "+NUM2);
										 }*/
                    }
                  } else {
                    // caso CSV
                    for( int j = 0; j < dim; j++ ) {
                      if( ( cell.get( j ) - 1L ) > 0L ) {
                        cellnew = cell;
                        cellnew.set( j, ( cell.get( j ) - 1L ) );
                        kad = ngnew.getCellNumber( cellnew );
                        x_j = ( boxCountNew.containsKey( kad ) ? boxCountNew.get( kad ) : 0.0 );
                        x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                        NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                        // calcolo senza elevare al quadrato o cubo
                        NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                        // fine - calcolo senza elevare al quadrato o cubo

                        // Quadrato
                        //x2_j = Math.pow(x_j,2);
                        //NUM2 += (x2_i - avg2)*(x2_j - avg2);

                        // Divisione in 10 classi
                        x2_j = Math.ceil( ( x_j - minCount ) / deltaClassi );
                        if( x2_j == 0 ) x2_j = 1;
                        NUM2 += ( x2_i - avgClassi ) * ( x2_j - avgClassi );

                        // Cubo
                        //x3_j = Math.pow(x_j,3);
                        //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                        W++;
                      }
                    }
                    for( int h = 0; h < dim; h++ ) {
                      if( ( cell.get( h ) + 1L ) <= ngnew.numCell ) {
                        cellnew = cell;
                        cellnew.set( h, ( cell.get( h ) + 1L ) );
                        kad = ngnew.getCellNumber( cellnew );
                        x_j = ( boxCountNew.containsKey( kad ) ? boxCountNew.get( kad ) : 0.0 );
                        x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                        NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                        // calcolo senza elevare al quadrato o cubo
                        NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                        // fine - calcolo senza elevare al quadrato o cubo

                        // Quadrato
                        //x2_j = Math.pow(x_j,2);
                        //NUM2 += (x2_i - avg2)*(x2_j - avg2);

                        // Divisione in 10 classi
                        x2_j = Math.ceil( ( x_j - minCount ) / deltaClassi );
                        if( x2_j == 0 ) x2_j = 1;
                        NUM2 += ( x2_i - avgClassi ) * ( x2_j - avgClassi );

                        // Cubo
                        //x3_j = Math.pow(x_j,3);
                        //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                        W++;
                      }
                    }
                  }
                }
              }

              if( !salto ) {
                MI_val0 = N / W * ( NUM0 / DEN0 );
                MI_val1 = N / W * ( NUM1 / DEN1 );
                MI_val2 = N / W * ( NUM2 / DEN2 );
                //MI_val3 = N/W*(NUM3/DEN3);
                System.out.println( "REDUCE-cleanUP: end computation Moran's Index" + " N: " + N + " W: " + W );
                System.out.println( "REDUCE-cleanUP:--> MI_val0=" + MI_val0 + " = " + N / W + "(" + NUM0 + "/" + DEN0 + ")" );
                System.out.println( "REDUCE-cleanUP:--> MI_val1=" + NUM1 + "/" + DEN1 );
                System.out.println( "REDUCE-cleanUP:--> MI_val2=" + NUM2 + "/" + DEN2 );
                //System.out.println("REDUCE-cleanUP:--> MI_val3="+NUM3+"/"+DEN3);
              }

              if( inputType.equalsIgnoreCase( "WKT" ) )
                width = gr.width;
              else
                // attenzione uso la larghezza della griglia sulla prima dimensione (coordinata x)
                // anche se la griglia potrebbe non essere un nCubo: in realtà è quasi sempre un nCubo
                width = ng.size.get( 0 );

              // i valori di cellSize vanno normalizzati rispetto alla griglia
              // dividendo cellSize per width
              // introdotte celle rettangolari: qui usiamo la width
              if( !salto ) {
                M0.put( ( cs[0] / width ), MI_val0 );
                M2.put( ( cs[0] / width ), MI_val2 );
                M1.put( ( cs[0] / width ), MI_val1 );
              }
              salto = false;
            } // fine calcolo Moran's index
          } // fine if che esclude il calcolo del Moran's index
          if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
            D0.put( Math.log( cs[0] / width ), Math.log( S0 ) );
            D2.put( Math.log( cs[0] / width ), Math.log( S2 ) );
            D3.put( Math.log( cs[0] / width ), Math.log( S3 ) );
            Ncell.put( Math.log( cs[0] / width ), ( N - S0 ) / N );
          } else {
            for( int i = 0; i < dim; i++ ) {
              ( D0_dim.get( i ) ).put( Math.log( cs[0] / width ), Math.log( S0_dim[i] ) );
              ( D2_dim.get( i ) ).put( Math.log( cs[0] / width ), Math.log( S2_dim[i] ) );
            }
          }
        }
      } else if( type.equalsIgnoreCase( "multipleGrid" ) ) {
        // TODO introdurre le classi anche qui
        boolean salto;
        for( int i = 1; i < NumGriglie; i++ ) {
          if( inputType.equalsIgnoreCase( "WKT" ) )
            gr = griglia.get( i );
          else {
            ng = Ngriglia.get( i );
          }

          bc = boxCount.get( i );

          // DEBUG
				  /* FOR debugging you need to set the OutputType to LongWritable,LongWritable instead of
				   * Text,Text
				   *
				  for (Long k: bc.keySet()) {
					  key = new LongWritable(k);
					  value = new LongWritable(bc.get(k));
					  context.write(key,value);
				  }
				  */
          // --------------------------------------------
          // fine DEBUG
          double[] cs = new double[10];
          if( inputType.equalsIgnoreCase( "WKT" ) ) {
            cs[0] = gr.tileWidth;
            cs[1] = gr.tileHeight;
          } else
            for( int j = 0; j < dim; j++ ) {
              cs[i] = ng.size.get( i );
            }

          // Compute values of D0, D2 and D3 on the current grid
          //if (inputType.equalsIgnoreCase("WKT")) {
          //  cellWidth = gr.tileWidth;
          //  cellHeight = gr.tileHeight;
          //} else {
          //  cellWidth = ng.tileSide;
          //}
          S0 = bc.size();
          S2 = 0.0;
          S3 = 0.0;
          S1 = 0.0;
          // compute MI
          if( inputType.equalsIgnoreCase( "WKT" ) )
            N = gr.numTiles;
          else
            N = ng.numTiles;

          for( Long k : bc.keySet() ) {
            S1 += bc.get( k );
            S2 += Math.pow( bc.get( k ), 2 );
            S3 += Math.pow( bc.get( k ), 3 );
          }
          //
          double avg0 = S0 / N;
          double avg1 = S1 / N;
          double avg2 = S2 / N;
          //double avg3 = S3/N;
          System.out.println( "REDUCE-cleanUP: avg-> " + avg0 + " for N-> " + N );

          if( inputType.equalsIgnoreCase( "WKT" ) )
            width = gr.width;
          else
            // attenzione uso la larghezza della griglia sulla prima dimensione (coordinata x)
            // anche se la griglia potrebbe non essere un nCubo: in realtà è quasi sempre un nCubo
            width = ng.size.get( 0 );

          if( MoranIndex ) {
            System.out.println( "REDUCE-cleanUP: start computation Moran's Index..." );
            W = 0.0;
            NUM0 = 0.0;
            NUM2 = 0.0;
            NUM1 = 0.0;
            DEN0 = 0.0;
            DEN2 = 0.0;
            DEN1 = 0.0;
            double maxrow = 1.0, maxcol = 1.0;
            if( inputType.equalsIgnoreCase( "WKT" ) ) {
              maxrow = gr.numRows;
              maxcol = gr.numColumns;
            }
            ArrayList<Long> cell = null, cellnew = null;

            salto = ( N > maxIterations ); //salto la prima griglia per il calcolo dell'indice Moran

            if( salto )
              System.out.println( "Salto calcolo Moran's index: N troppo alto: " + N );
            else {
              for( Long k = 0L; k < N; k++ ) {
                // DEN computation
                x0_i = ( bc.containsKey( k ) ? 1.0 : 0.0 );
                DEN0 += Math.pow( ( x0_i - avg0 ), 2 );
                // calcolo senza elevare al quadrato o cubo x1_i
                x1_i = ( bc.containsKey( k ) ? bc.get( k ) : 0.0 );
                DEN1 += Math.pow( ( x1_i - avg1 ), 2 );
                // fine - calcolo senza elevare al quadrato o cubo x1_i
                x2_i = ( bc.containsKey( k ) ? Math.pow( bc.get( k ), 2 ) : 0.0 );
                DEN2 += Math.pow( ( x2_i - avg2 ), 2 );
                //x3_i = (bc.containsKey(k)?Math.pow(bc.get(k),3):0.0);
                //DEN3 += Math.pow((x3_i - avg3),2);

                // current CELL
                row = 1L;
                col = 1L;
                if( inputType.equalsIgnoreCase( "WKT" ) ) {
                  // position (row,col)
                  row = gr.getRow( k );
                  col = gr.getCol( k );
                } else
                  // position cell
                  cell = ng.getCellCoord( k );

                // NUM computation
                if( inputType.equalsIgnoreCase( "WKT" ) ) {
                  // position (0,-1)
                  if( ( col - 1L ) > 0L ) {
                    kad = gr.getCellId( row, ( col - 1L ) );
                    x_j = ( bc.containsKey( kad ) ? bc.get( kad ) : 0.0 );
                    x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                    NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                    // calcolo senza elevare al quadrato o cubo
                    NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                    // fine - calcolo senza elevare al quadrato o cubo
                    x2_j = Math.pow( x_j, 2 );
                    NUM2 += ( x2_i - avg2 ) * ( x2_j - avg2 );
                    //x3_j = Math.pow(x_j,3);
                    //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                    W++;
                  }
                  // posizione (-1, 0)
                  if( ( row - 1L ) > 0L ) {
                    kad = gr.getCellId( ( row - 1L ), col );
                    x_j = ( bc.containsKey( kad ) ? bc.get( kad ) : 0.0 );
                    x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                    NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                    // calcolo senza elevare al quadrato o cubo
                    NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                    // fine - calcolo senza elevare al quadrato o cubo
                    x2_j = Math.pow( x_j, 2 );
                    NUM2 += ( x2_i - avg2 ) * ( x2_j - avg2 );
                    //x3_j = Math.pow(x_j,3);
                    //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                    W++;
                  }
                  // posizione (1, 0)
                  if( ( row + 1L ) <= maxrow ) {
                    kad = gr.getCellId( ( row + 1L ), col );
                    x_j = ( bc.containsKey( kad ) ? bc.get( kad ) : 0.0 );
                    x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                    NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                    // calcolo senza elevare al quadrato o cubo
                    NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                    // fine - calcolo senza elevare al quadrato o cubo
                    x2_j = Math.pow( x_j, 2 );
                    NUM2 += ( x2_i - avg2 ) * ( x2_j - avg2 );
                    //x3_j = Math.pow(x_j,3);
                    //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                    W++;
                  }
                  // posizione (0,1)
                  if( ( col + 1L ) <= maxcol ) {
                    kad = gr.getCellId( row, ( col + 1L ) );
                    x_j = ( bc.containsKey( kad ) ? bc.get( kad ) : 0.0 );
                    x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                    NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                    // calcolo senza elevare al quadrato o cubo
                    NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                    // fine - calcolo senza elevare al quadrato o cubo
                    x2_j = Math.pow( x_j, 2 );
                    NUM2 += ( x2_i - avg2 ) * ( x2_j - avg2 );
                    //x3_j = Math.pow(x_j,3);
                    //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                    W++;
                  }
                } else {
                  for( int j = 0; j < dim; j++ ) {
                    if( ( cell.get( j ) - 1L ) > 0 ) {
                      cellnew = cell;
                      cellnew.set( j, ( cell.get( j ) - 1L ) );
                      kad = ng.getCellNumber( cellnew );
                      x_j = ( bc.containsKey( kad ) ? bc.get( kad ) : 0.0 );
                      x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                      NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                      // calcolo senza elevare al quadrato o cubo
                      NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                      // fine - calcolo senza elevare al quadrato o cubo
                      x2_j = Math.pow( x_j, 2 );
                      NUM2 += ( x2_i - avg2 ) * ( x2_j - avg2 );
                      //x3_j = Math.pow(x_j,3);
                      //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                      W++;
                    }
                  }
                  for( int h = 0; h < dim; h++ ) {
                    if( ( cell.get( h ) + 1L ) <= ng.numCell ) {
                      cellnew = cell;
                      cellnew.set( h, ( cell.get( h ) + 1L ) );
                      kad = ng.getCellNumber( cellnew );
                      x_j = ( bc.containsKey( kad ) ? bc.get( kad ) : 0.0 );
                      x0_j = ( x_j > 0 ? 1.0 : 0.0 );
                      NUM0 += ( x0_i - avg0 ) * ( x0_j - avg0 );
                      // calcolo senza elevare al quadrato o cubo
                      NUM1 += ( x1_i - avg1 ) * ( x_j - avg1 );
                      // fine - calcolo senza elevare al quadrato o cubo
                      x2_j = Math.pow( x_j, 2 );
                      NUM2 += ( x2_i - avg2 ) * ( x2_j - avg2 );
                      //x3_j = Math.pow(x_j,3);
                      //NUM3 += (x3_i - avg3)*(x3_j - avg3);
                      W++;
                    }
                  }
                }
              }
            }

            if( !salto ) {
              MI_val0 = N / W * ( NUM0 / DEN0 );
              MI_val2 = N / W * ( NUM2 / DEN2 );
              MI_val1 = N / W * ( NUM1 / DEN1 );
              System.out.println( "REDUCE-cleanUP: end computation Moran's Index" + " N: " + N + " W: " + W );
              System.out.println( "REDUCE-cleanUP:--> MI_val0=" + NUM0 + "/" + DEN0 );
              System.out.println( "REDUCE-cleanUP:--> MI_val2=" + NUM2 + "/" + DEN2 );
              System.out.println( "REDUCE-cleanUP:--> MI_val1=" + NUM1 + "/" + DEN1 );
            }

            // i valori di cellSize vanno normalizzati rispetto alla griglia
            // dividendo cellSize per width
            // introdotte celle rettangolari qui usiamo width

            if( !salto ) {
              M0.put( ( cs[0] / width ), MI_val0 );
              M2.put( ( cs[0] / width ), MI_val2 );
              M1.put( ( cs[0] / width ), MI_val1 );
            }
          } // fine if per il calcolo del Moran's Index
          if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
            D0.put( Math.log( cs[0] / width ), Math.log( S0 ) );
            D2.put( Math.log( cs[0] / width ), Math.log( S2 ) );
            D3.put( Math.log( cs[0] / width ), Math.log( S3 ) );
            Ncell.put( Math.log( cs[0] / width ), ( N - S0 ) / N );
          }
        }

      }

      // Regressione per D0, D2 e D3
      // Revisione del 10 maggio
      // Proviamo a calcolare le variazioni di slope e a individuare le variazioni più grandi
      // Cerchiamo intervalli di epsilon dove valgono valori diversi
      ArrayList<Double> D0ord = null;
      ArrayList<Double> D2ord = null;
      ArrayList<Double> D3ord = null;
      ArrayList<ArrayList<Double>> D0ord_dim = null;
      ArrayList<ArrayList<Double>> D2ord_dim = null;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        D0ord = new ArrayList<Double>( D0.keySet() );
        Collections.sort( D0ord );
        D2ord = new ArrayList<Double>( D2.keySet() );
        Collections.sort( D2ord );
        D3ord = new ArrayList<Double>( D3.keySet() );
        Collections.sort( D3ord );
      } else {
        // CSVmulti
        D0ord_dim = new ArrayList<ArrayList<Double>>();
        for( int i = 0; i < dim; i++ ) {
          //System.out.println("D0_dim.get("+i+").keySet(): "+D0_dim.get(i).keySet());
          D0ord_dim.add( i, new ArrayList<Double>( D0_dim.get( i ).keySet() ) );
          Collections.sort( D0ord_dim.get( i ) );
        }
        D2ord_dim = new ArrayList<ArrayList<Double>>();
        for( int i = 0; i < dim; i++ ) {
          D2ord_dim.add( i, new ArrayList<Double>( D2_dim.get( i ).keySet() ) );
          Collections.sort( D2ord_dim.get( i ) );
        }
        // DEBUG
        // stampo le liste di valori:
        for( int i = 0; i < dim; i++ ) {
          System.out.println( "D0_dim.get(" + i + "): " + D0_dim.get( i ) );
          System.out.println( "D2_dim.get(" + i + "): " + D2_dim.get( i ) );
        }
        System.out.println( "D0ord_dim.get(0): " + D0ord_dim.get( 0 ) );
        System.out.println( "D2ord_dim.get(0): " + D2ord_dim.get( 0 ) );
      }
      //System.exit(0);

      // Versione vecchia
      // definisco soglia per inizio discesa
      //double threshold, start;

      // Revisione del 10 maggio: calcolo le variazioni di slope
      HashMap<Double, Double> slopeVar = null;
      HashMap<Double, Double> slopeVar2 = null;
      HashMap<Double, Double> slopeVar3 = null;
      ArrayList<HashMap<Double, Double>> slopeVar_dim = null;
      ArrayList<HashMap<Double, Double>> slopeVar2_dim = null;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        slopeVar = new HashMap<Double, Double>();
        slopeVar2 = new HashMap<Double, Double>();
        slopeVar3 = new HashMap<Double, Double>();
      } else {
        // CSVmulti
        slopeVar_dim = new ArrayList<HashMap<Double, Double>>();
        for( int i = 0; i < dim; i++ )
          slopeVar_dim.add( i, new HashMap<Double, Double>() );
        slopeVar2_dim = new ArrayList<HashMap<Double, Double>>();
        for( int i = 0; i < dim; i++ )
          slopeVar2_dim.add( i, new HashMap<Double, Double>() );
      }

      // Revisione del 10 maggio: tolgo la soglia
      // soglia di scarto iniziale: 0.90
      //threshold = start*0.90;

      // primo punto variazione nulla
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        slopeVar.put( 0.0, D0ord.get( 0 ) );
        slopeVar2.put( 0.0, D2ord.get( 0 ) );
        slopeVar3.put( 0.0, D3ord.get( 0 ) );
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          slopeVar_dim.get( i ).put( 0.0, D0ord_dim.get( i ).get( 0 ) );
          slopeVar2_dim.get( i ).put( 0.0, D2ord_dim.get( i ).get( 0 ) );
        }
        System.out.println( "slopeVar_dim.get(0): " + slopeVar_dim.get( 0 ) );
        System.out.println( "slopeVar2_dim.get(0): " + slopeVar2_dim.get( 0 ) );
      }

      // calcolo prima slope e successiva
      double slope1 = 1.0;
      double slope2 = 1.0;
      double slope1_2 = 1.0;
      double slope2_2 = 1.0;
      double slope1_3 = 1.0;
      double slope2_3 = 1.0;
      double[] slope0_1_dim = new double[10];
      double[] slope0_2_dim = new double[10];
      double[] slope2_1_dim = new double[10];
      double[] slope2_2_dim = new double[10];

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        slope1 = ( D0.get( D0ord.get( 1 ) ) - D0.get( D0ord.get( 0 ) ) ) / ( D0ord.get( 1 ) - D0ord.get( 0 ) );
        slope2 = ( D0.get( D0ord.get( 2 ) ) - D0.get( D0ord.get( 1 ) ) ) / ( D0ord.get( 2 ) - D0ord.get( 1 ) );

        slope1_2 = ( D2.get( D2ord.get( 1 ) ) - D2.get( D2ord.get( 0 ) ) ) / ( D2ord.get( 1 ) - D2ord.get( 0 ) );
        slope2_2 = ( D2.get( D2ord.get( 2 ) ) - D2.get( D2ord.get( 1 ) ) ) / ( D2ord.get( 2 ) - D2ord.get( 1 ) );

        slope1_3 = ( D3.get( D3ord.get( 1 ) ) - D3.get( D3ord.get( 0 ) ) ) / ( D3ord.get( 1 ) - D3ord.get( 0 ) );
        slope2_3 = ( D3.get( D3ord.get( 2 ) ) - D3.get( D3ord.get( 1 ) ) ) / ( D3ord.get( 2 ) - D3ord.get( 1 ) );
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          slope0_1_dim[i] = ( D0_dim.get( i ).get( D0ord_dim.get( i ).get( 1 ) ) - D0_dim.get( i ).get( D0ord_dim.get( i ).get( 0 ) ) ) /
                            ( D0ord_dim.get( i ).get( 1 ) - D0ord_dim.get( i ).get( 0 ) );
          //System.out.println("slope0_1_dim["+i+"]: "+slope0_1_dim[i]);

          slope0_2_dim[i] = ( D0_dim.get( i ).get( D0ord_dim.get( i ).get( 2 ) ) - D0_dim.get( i ).get( D0ord_dim.get( i ).get( 1 ) ) ) /
                            ( D0ord_dim.get( i ).get( 2 ) - D0ord_dim.get( i ).get( 1 ) );
          //System.out.println("slope0_2_dim["+i+"]: "+slope0_2_dim[i]);

          slope2_1_dim[i] = ( D2_dim.get( i ).get( D2ord_dim.get( i ).get( 1 ) ) - D2_dim.get( i ).get( D2ord_dim.get( i ).get( 0 ) ) ) /
                            ( D2ord_dim.get( i ).get( 1 ) - D2ord_dim.get( i ).get( 0 ) );
          //System.out.println("slope2_1_dim["+i+"]: "+slope2_1_dim[i]);

          slope2_2_dim[i] = ( D2_dim.get( i ).get( D2ord_dim.get( i ).get( 2 ) ) - D2_dim.get( i ).get( D2ord_dim.get( i ).get( 1 ) ) ) /
                            ( D2ord_dim.get( i ).get( 2 ) - D2ord_dim.get( i ).get( 1 ) );
          //System.out.println("slope2_2_dim["+i+"]: "+slope2_2_dim[i]);
        }
      }

      // calcolo variazione di angolo
      double alpha = 0.0;
      double alpha2 = 0.0;
      double alpha3 = 0.0;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        alpha = Math.atan( Math.abs( slope2 - slope1 ) );
        slopeVar.put( alpha, D0ord.get( 1 ) );

        alpha2 = Math.atan( Math.abs( slope2_2 - slope1_2 ) );
        slopeVar2.put( alpha2, D2ord.get( 1 ) );

        alpha3 = Math.atan( Math.abs( slope2_3 - slope1_3 ) );
        slopeVar3.put( alpha3, D3ord.get( 1 ) );
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          alpha = Math.atan( Math.abs( slope0_2_dim[i] - slope0_1_dim[i] ) );
          slopeVar_dim.get( i ).put( alpha, D0ord_dim.get( i ).get( 1 ) );

          alpha2 = Math.atan( Math.abs( slope2_2_dim[i] - slope2_1_dim[i] ) );
          slopeVar2_dim.get( i ).put( alpha2, D2ord_dim.get( i ).get( 1 ) );
        }
      }

      // stampo sequenza valori D0
		  /*System.out.println("D0 key[0]: "+D0ord.get(0)+" value: "+D0.get(D0ord.get(0)));
		  System.out.println("D0 key[1]: "+D0ord.get(1)+" value: "+D0.get(D0ord.get(1)));
		  System.out.println("D0 key[2]: "+D0ord.get(2)+" value: "+D0.get(D0ord.get(2)));
		  System.out.println("D0 alpha[1]: "+alpha);

		  // stampo sequenza valori D2
		  System.out.println("D2 key[0]: "+D2ord.get(0)+" value: "+D2.get(D2ord.get(0)));
		  System.out.println("D2 key[1]: "+D2ord.get(1)+" value: "+D2.get(D2ord.get(1)));
		  System.out.println("D2 key[2]: "+D2ord.get(2)+" value: "+D2.get(D2ord.get(2)));
		  System.out.println("D2 alpha[1]: "+alpha2);

		  // stampo sequenza valori D3
		  System.out.println("D3 key[0]: "+D3ord.get(0)+" value: "+D3.get(D3ord.get(0)));
		  System.out.println("D3 key[1]: "+D3ord.get(1)+" value: "+D3.get(D3ord.get(1)));
		  System.out.println("D3 key[2]: "+D3ord.get(2)+" value: "+D3.get(D3ord.get(2)));
		  System.out.println("D3 alpha[1]: "+alpha3);
		  */

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D0
        int i = 3;
        double first = 0.0, second = D0ord.get( 2 );
        while( i < D0ord.size() ) {
          first = second;
          second = D0ord.get( i );
          slope1 = slope2;
          slope2 = ( D0.get( first ) - D0.get( second ) ) / ( first - second );
          //System.out.println("D0 key["+i+"]: "+second+" value: "+D0.get(second));
          alpha = Math.atan( Math.abs( slope2 - slope1 ) );
          //System.out.println("D0 alpha["+(i-1)+"]: "+alpha);
          slopeVar.put( alpha, first );
          i++;
        }
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          // D0_dim
          int j = 3;
          double first = 0.0, second = D0ord_dim.get( i ).get( 2 );
          slope2 = slope0_2_dim[i];
          while( j < D0ord_dim.get( i ).size() ) {
            first = second;
            second = D0ord_dim.get( i ).get( j );
            slope1 = slope2;
            slope2 = ( D0_dim.get( i ).get( first ) - D0_dim.get( i ).get( second ) ) / ( first - second );
            //System.out.println("D0 key["+i+"]: "+second+" value: "+D0.get(second));
            alpha = Math.atan( Math.abs( slope2 - slope1 ) );
            //System.out.println("D0 alpha["+(j-1)+"]: "+alpha);
            slopeVar_dim.get( i ).put( alpha, first );
            j++;
          }
          System.out.println( "slopeVar_dim.get(" + i + "): " + slopeVar_dim.get( i ) );
        }
      }

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D2
        int i = 3;
        double first = 0.0;
        double second = D2ord.get( 2 );
        while( i < D2ord.size() ) {
          first = second;
          second = D2ord.get( i );
          slope1_2 = slope2_2;
          slope2_2 = ( D2.get( first ) - D2.get( second ) ) / ( first - second );
          //System.out.println("D2 key["+i+"]: "+second+" value: "+D2.get(second));
          alpha2 = Math.atan( Math.abs( slope2_2 - slope1_2 ) );
          //System.out.println("D2 alpha["+(i-1)+"]: "+alpha2);
          slopeVar2.put( alpha2, first );
          i++;
        }
      } else {
        //CSVmulti
        for( int i = 0; i < dim; i++ ) {
          // D2_dim
          int j = 3;
          double first = 0.0;
          double second = D2ord_dim.get( i ).get( 2 );
          slope2_2 = slope2_2_dim[i];
          while( j < D2ord_dim.get( i ).size() ) {
            first = second;
            second = D2ord_dim.get( i ).get( j );
            slope1_2 = slope2_2;
            slope2_2 = ( D2_dim.get( i ).get( first ) - D2_dim.get( i ).get( second ) ) / ( first - second );
            //System.out.println("D2 key["+j+"]: "+second+" value: "+D2.get(second));
            alpha2 = Math.atan( Math.abs( slope2_2 - slope1_2 ) );
            //System.out.println("D2 alpha["+(j-1)+"]: "+alpha2);
            slopeVar2_dim.get( i ).put( alpha2, first );
            j++;
          }
          System.out.println( "slopeVar2_dim.get(" + i + "): " + slopeVar2_dim.get( i ) );
        }
      }

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D3
        int i = 3;
        double first = 0.0;
        double second = D3ord.get( 2 );
        while( i < D3ord.size() ) {
          first = second;
          second = D3ord.get( i );
          slope1_3 = slope2_3;
          slope2_3 = ( D3.get( first ) - D3.get( second ) ) / ( first - second );
          //System.out.println("D3 key["+i+"]: "+second+" value: "+D3.get(second));
          alpha3 = Math.atan( Math.abs( slope2_3 - slope1_3 ) );
          //System.out.println("D3 alpha["+(i-1)+"]: "+alpha3);
          slopeVar3.put( alpha3, first );
          i++;
        }
      }

      // Revisione 10 maggio
      // variazione slope
      // ordino in base alla variazione dell'angolo
      ArrayList<Double> alphaOrd = null;
      ArrayList<Double> alphaOrd2 = null;
      ArrayList<Double> alphaOrd3 = null;
      ArrayList<ArrayList<Double>> alphaOrd_dim = new ArrayList<ArrayList<Double>>();
      ArrayList<ArrayList<Double>> alphaOrd2_dim = new ArrayList<ArrayList<Double>>();

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        alphaOrd = new ArrayList<Double>( slopeVar.keySet() );
        Collections.sort( alphaOrd );

        alphaOrd2 = new ArrayList<Double>( slopeVar2.keySet() );
        Collections.sort( alphaOrd2 );

        alphaOrd3 = new ArrayList<Double>( slopeVar3.keySet() );
        Collections.sort( alphaOrd3 );
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          alphaOrd_dim.add( new ArrayList<Double>( slopeVar_dim.get( i ).keySet() ) );
          Collections.sort( alphaOrd_dim.get( i ) );
          alphaOrd2_dim.add( new ArrayList<Double>( slopeVar2_dim.get( i ).keySet() ) );
          Collections.sort( alphaOrd2_dim.get( i ) );
        }
      }


      // scelgo le 5 variazioni più grandi
      // Oct 2018: riduco a 3 le variazioni da considerare
      final int numVar = 3;

      ArrayList<Double> splitPoint = null;
      ArrayList<ArrayList<Double>> splitPoint_dim = new ArrayList<ArrayList<Double>>();
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D0
        splitPoint = new ArrayList<Double>( numVar );
        //System.out.println("D0: slopeVarMAX "+slopeVar.get(alphaOrd.get(alphaOrd.size()-1)));
        splitPoint.add( 0, slopeVar.get( alphaOrd.get( alphaOrd.size() - 1 ) ) );
        if( alphaOrd.size() > 1 ) {
          splitPoint.add( 1, slopeVar.get( alphaOrd.get( alphaOrd.size() - 2 ) ) );
          if( alphaOrd.size() > 2 )
            splitPoint.add( 2, slopeVar.get( alphaOrd.get( alphaOrd.size() - 3 ) ) );
        }
        //splitPoint.add(3, slopeVar.get(alphaOrd.get(alphaOrd.size()-4)));
        //splitPoint.add(4, slopeVar.get(alphaOrd.get(alphaOrd.size()-5)));
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          splitPoint_dim.add( i, new ArrayList<Double>( numVar ) );
          splitPoint_dim.get( i ).add( 0, slopeVar_dim.get( i ).get( alphaOrd_dim.get( i ).get( alphaOrd_dim.get( i ).size() - 1 ) ) );
          if( alphaOrd_dim.get( i ).size() > 1 ) {
            splitPoint_dim.get( i ).add( 1, slopeVar_dim.get( i ).get( alphaOrd_dim.get( i ).get( alphaOrd_dim.get( i ).size() - 2 ) ) );
            if( alphaOrd_dim.get( i ).size() > 2 )
              splitPoint_dim.get( i ).add( 2, slopeVar_dim.get( i ).get( alphaOrd_dim.get( i ).get( alphaOrd_dim.get( i ).size() - 3 ) ) );
          }
        }
      }

      ArrayList<Double> splitPoint2 = null;
      ArrayList<ArrayList<Double>> splitPoint2_dim = new ArrayList<ArrayList<Double>>();
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D2
        splitPoint2 = new ArrayList<Double>( numVar );
        splitPoint2.add( 0, slopeVar2.get( alphaOrd2.get( alphaOrd2.size() - 1 ) ) );
        if( alphaOrd2.size() > 1 ) {
          splitPoint2.add( 1, slopeVar2.get( alphaOrd2.get( alphaOrd2.size() - 2 ) ) );
          if( alphaOrd2.size() > 2 )
            splitPoint2.add( 2, slopeVar2.get( alphaOrd2.get( alphaOrd2.size() - 3 ) ) );
        }
        //splitPoint2.add(3, slopeVar2.get(alphaOrd2.get(alphaOrd2.size()-4)));
        //splitPoint2.add(4, slopeVar2.get(alphaOrd2.get(alphaOrd2.size()-5)));
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          splitPoint2_dim.add( i, new ArrayList<Double>( numVar ) );
          splitPoint2_dim.get( i ).add( 0, slopeVar2_dim.get( i ).get( alphaOrd2_dim.get( i ).get( alphaOrd2_dim.get( i ).size() - 1 ) ) );
          if( alphaOrd2_dim.get( i ).size() > 1 ) {
            splitPoint2_dim.get( i ).add( 1, slopeVar2_dim.get( i ).get( alphaOrd2_dim.get( i ).get( alphaOrd2_dim.get( i ).size() - 2 ) ) );
            if( alphaOrd2_dim.get( i ).size() > 2 )
              splitPoint2_dim.get( i ).add( 2, slopeVar2_dim.get( i ).get( alphaOrd2_dim.get( i ).get( alphaOrd2_dim.get( i ).size() - 3 ) ) );
          }
        }
      }

      ArrayList<Double> splitPoint3 = null;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D3
        splitPoint3 = new ArrayList<Double>( numVar );
        splitPoint3.add( 0, slopeVar3.get( alphaOrd3.get( alphaOrd3.size() - 1 ) ) );
        if( alphaOrd3.size() > 1 ) {
          splitPoint3.add( 1, slopeVar3.get( alphaOrd3.get( alphaOrd3.size() - 2 ) ) );
          if( alphaOrd3.size() > 1 )
            splitPoint3.add( 2, slopeVar3.get( alphaOrd3.get( alphaOrd3.size() - 3 ) ) );
        }
        //splitPoint1.add(3, slopeVar1.get(alphaOrd1.get(alphaOrd1.size()-4)));
      }

      // ordino i punti di split
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        Collections.sort( splitPoint );
        Collections.sort( splitPoint2 );
        Collections.sort( splitPoint3 );
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          Collections.sort( splitPoint_dim.get( i ) );
          Collections.sort( splitPoint2_dim.get( i ) );
        }
      }

      // Eliminazione dei punti di split troppo vicini D0 e D0_dim
      ArrayList<Double> splitPointToKeep = null;
      ArrayList<ArrayList<Double>> splitPointToKeep_dim = new ArrayList<ArrayList<Double>>();
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D0
        final int minLength = 3;
        splitPointToKeep = new ArrayList<Double>( 10 );
        splitPointToKeep.clear();
        int i = 0;
        int length = 1, a = 0;
        System.out.println( "\nD0 SplitPoint..." );
        while( i < D0ord.size() ) {
          System.out.println( "D0 SplitPoint: " + splitPoint.get( a ) + " D0 x: " + D0ord.get( i ) );
          if( splitPoint.get( a ).equals( D0ord.get( i ) ) ) {
            System.out.println( "D0 lenght: " + length );
            if( length >= minLength ) {
              System.out.println( "D0 SplitPointToKeep inserito: " + D0ord.get( i ) );
              splitPointToKeep.add( splitPoint.get( a ) );
              length = 0;
            }
            if( a < ( splitPoint.size() - 1 ) ) {
              a++;
              length++;
            }
          } else {
            length++;
          }
          i++;
        }
        // tolgo l'ultimo split se la lunghezza dell'ultimo intervallo è troppo breve (<3)
        System.out.println( "D0 lenght last: " + length );
        if( length <= minLength ) {
          System.out.println( "D0 SplitPointToKeep tolto: " + splitPointToKeep.get( splitPointToKeep.size() - 1 ) );
          splitPointToKeep.remove( splitPointToKeep.size() - 1 );
        }
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          // D0_dim
          // Eliminazione dei punti di split troppo vicini
          final int minLength = 3;
          splitPointToKeep_dim.add( i, new ArrayList<Double>( 10 ) );
          splitPointToKeep_dim.get( i ).clear();
          int j = 0;
          int length = 1, a = 0;
          System.out.println( "\nD0_dim SplitPoint..." );
          while( j < D0ord_dim.get( i ).size() ) {
            System.out.println( "D0_dim SplitPoint: " + splitPoint_dim.get( i ).get( a ) + " D0 x: " + D0ord_dim.get( i ).get( j ) );
            if( splitPoint_dim.get( i ).get( a ).equals( D0ord_dim.get( i ).get( j ) ) ) {
              System.out.println( "D0_dim[" + i + "] lenght: " + length );
              if( length >= minLength ) {
                System.out.println( "D0_dim SplitPointToKeep inserito: " + D0ord_dim.get( i ).get( j ) );
                splitPointToKeep_dim.get( i ).add( splitPoint_dim.get( i ).get( a ) );
                length = 0;
              }
              if( a < ( splitPoint_dim.get( i ).size() - 1 ) ) {
                a++;
                length++;
              }
            } else {
              length++;
            }
            j++;
          }
          // tolgo l'ultimo split se la lunghezza dell'ultimo intervallo è troppo breve (<3)
          System.out.println( "D0 lenght last: " + length );
          if( length <= minLength ) {
            System.out.println( "D0_dim[" + i + "] SplitPointToKeep tolto: " +
                                splitPointToKeep_dim.get( i ).get( splitPointToKeep_dim.get( i ).size() - 1 ) );
            splitPointToKeep_dim.get( i ).remove( splitPointToKeep_dim.get( i ).size() - 1 );
          }
        }
      }


      // Eliminazione dei punti di split troppo vicini D2 e D2_dim
      // D2
      ArrayList<Double> splitPointToKeep2 = null;
      ArrayList<ArrayList<Double>> splitPointToKeep2_dim = new ArrayList<ArrayList<Double>>();
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        splitPointToKeep2 = new ArrayList<Double>( 10 );
        splitPointToKeep2.clear();
        int i = 0;
        int length = 1;
        int a = 0;
        final int minLength2 = 3;
        System.out.println( "\nD2 SplitPoints..." );
        while( i < D2ord.size() ) {
          System.out.println( "D2 SplitPoint: " + splitPoint2.get( a ) + " D2 x: " + D2ord.get( i ) );
          if( splitPoint2.get( a ).equals( D2ord.get( i ) ) ) {
            System.out.println( "D2 lenght: " + length );
            if( length >= minLength2 ) {
              System.out.println( "D2 SplitPointToKeep inserito: " + D2ord.get( i ) );
              splitPointToKeep2.add( splitPoint2.get( a ) );
              length = 0;
            }
            if( a < ( splitPoint2.size() - 1 ) ) {
              a++;
              length++;
            }
          } else {
            length++;
          }
          i++;
        }
        // tolgo l'ultimo split se la lunghezza dell'ultimo intervallo è troppo breve (<3)
        if( length <= minLength2 ) {
          System.out.println( "D2 lenght: " + length );
          System.out.println( "D2 SplitPointToKeep tolto: " + splitPointToKeep2.get( splitPointToKeep2.size() - 1 ) );
          splitPointToKeep2.remove( splitPointToKeep2.size() - 1 );
        }
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          // D2_dim
          // Eliminazione dei punti di split troppo vicini
          splitPointToKeep2_dim.add( i, new ArrayList<Double>( 10 ) );
          splitPointToKeep2_dim.get( i ).clear();
          int j = 0;
          int length = 1;
          int a = 0;
          final int minLength2 = 3;
          System.out.println( "\nD2_dim SplitPoints..." );
          while( j < D2ord_dim.get( i ).size() ) {
            System.out.println( "D2_dim SplitPoint: " + splitPoint2_dim.get( i ).get( a ) + " D2_dim x: " + D2ord_dim.get( i ).get( j ) );
            if( splitPoint2_dim.get( i ).get( a ).equals( D2ord_dim.get( i ).get( j ) ) ) {
              System.out.println( "D2_dim lenght: " + length );
              if( length >= minLength2 ) {
                System.out.println( "D2_dim[" + i + "] SplitPointToKeep inserito: " + D2ord_dim.get( i ).get( j ) );
                splitPointToKeep2_dim.get( i ).add( splitPoint2_dim.get( i ).get( a ) );
                length = 0;
              }
              if( a < ( splitPoint2_dim.get( i ).size() - 1 ) ) {
                a++;
                length++;
              }
            } else {
              length++;
            }
            j++;
          }
          // tolgo l'ultimo split se la lunghezza dell'ultimo intervallo è troppo breve (<3)
          if( length <= minLength2 ) {
            System.out.println( "D2_dim[" + i + "] lenght: " + length );
            System.out.println( "D2_dim SplitPointToKeep tolto: " + splitPointToKeep2_dim.get( i ).get( splitPointToKeep2_dim.get( i ).size() - 1 ) );
            splitPointToKeep2_dim.get( i ).remove( splitPointToKeep2_dim.get( i ).size() - 1 );
          }
        }
      }

      // Eliminazione dei punti di split troppo vicini D3
      ArrayList<Double> splitPointToKeep3 = null;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        // D3
        splitPointToKeep3 = new ArrayList<Double>( 4 );
        splitPointToKeep3.clear();
        int i = 0;
        int length = 1;
        int a = 0;
        final int minLength3 = 3;
        System.out.println( "\nD3 SplitPoints..." );
        while( i < D3ord.size() ) {
          System.out.println( "D3 SplitPoint: " + splitPoint3.get( a ) + " D3 x: " + D3ord.get( i ) );
          if( splitPoint3.get( a ).equals( D3ord.get( i ) ) ) {
            System.out.println( "D3 lenght: " + length );
            if( length >= minLength3 ) {
              System.out.println( "D3 SplitPointToKeep inserito: " + D3ord.get( i ) );
              splitPointToKeep3.add( splitPoint3.get( a ) );
              length = 0;
            }
            if( a < ( splitPoint3.size() - 1 ) ) {
              a++;
              length++;
            }
          } else {
            length++;
          }
          i++;
        }
        // tolgo l'ultimo split se la lunghezza dell'ultimo intervallo è troppo breve (<3)
        if( length <= minLength3 ) {
          System.out.println( "D3 lenght: " + length );
          System.out.println( "D3 SplitPointToKeep tolto: " + splitPointToKeep3.get( splitPointToKeep3.size() - 1 ) );
          splitPointToKeep3.remove( splitPointToKeep3.size() - 1 );
        }
      }

      // --------------------------------------------------------------
      // SEZIONE PER IL CALCOLO DELLA SLOPE NEI DIVERSI CASI
      // --------------------------------------------------------------

      // D0
      ArrayList<ArrayList<Double>> D0final = null;
      ArrayList<ArrayList<ArrayList<Double>>> D0final_dim =
        new ArrayList<ArrayList<ArrayList<Double>>>();
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        System.out.println( "\nD0 ->>> inizio calcolo slope" );
        SimpleRegression regressionD0 = new SimpleRegression();
        int a = 0;
        double k;
        Double support = 0.0;
        int i = 0;
        D0final = new ArrayList<ArrayList<Double>>( 10 );
        ArrayList<Double> elem;
        while( i < D0.size() ) {
          k = D0ord.get( i++ );
          System.out.println( "D0: intervallo " + a + " key[" + ( i - 1 ) + "]: " + k + " value: " + D0.get( k ) );
          regressionD0.addData( k, D0.get( k ) );
          support++;
          if( a < splitPointToKeep.size() && splitPointToKeep.get( a ).equals( k ) ) {
            System.out.println( "D0 regression slope ->> = " + regressionD0.getSlope() +
                                "regression intercept ->> = " + regressionD0.getIntercept() + " to ->> " + splitPointToKeep.get( a ) );
            elem = new ArrayList<Double>( 2 );
            elem.add( support );
            elem.add( -regressionD0.getSlope() );
            D0final.add( elem );

            regressionD0.clear();
            support = 0.0;
            //regressionD0.addData(k,D0.get(k));
            a++;
          }
        }
        System.out.println( "D0 regression slope ->> = " + regressionD0.getSlope() +
                            "regression intercept ->> = " + regressionD0.getIntercept() + " to ->> " + D0ord.get( D0ord.size() - 1 ) );

        elem = new ArrayList<Double>( 2 );
        elem.add( support );
        elem.add( -regressionD0.getSlope() );
        D0final.add( elem );
      } else {
        // CSVmulti
        for( int i = 0; i < dim; i++ ) {
          System.out.println( "\nD0_dim[" + i + "] ->>> inizio calcolo slope" );
          SimpleRegression regressionD0 = new SimpleRegression();
          int a = 0;
          double k;
          Double support = 0.0;
          int j = 0;
          D0final_dim.add( i, new ArrayList<ArrayList<Double>>( 10 ) );
          ArrayList<Double> elem;
          while( j < D0_dim.get( i ).size() ) {
            k = D0ord_dim.get( i ).get( j++ );
            System.out.println( "D0_dim[" + i + "]: intervallo " + a + " key[" + ( j - 1 ) + "]: " + k + " value: " + D0_dim.get( i ).get( k ) );
            regressionD0.addData( k, D0_dim.get( i ).get( k ) );
            support++;
            if( a < splitPointToKeep_dim.get( i ).size() &&
                splitPointToKeep_dim.get( i ).get( a ).equals( k ) ) {
              System.out.println( "D0_dim[" + i + "] regression slope ->> = " + regressionD0.getSlope() +
                                  "regression intercept ->> = " + regressionD0.getIntercept() + " to ->> " +
                                  splitPointToKeep_dim.get( i ).get( a ) );
              elem = new ArrayList<Double>( 2 );
              elem.add( support );
              elem.add( -regressionD0.getSlope() );
              D0final_dim.get( i ).add( elem );

              regressionD0.clear();
              support = 0.0;
              //regressionD0.addData(k,D0.get(k));
              a++;
            }
          }
          System.out.println( "D0_dim[" + i + "] regression slope ->> = " + regressionD0.getSlope() +
                              "regression intercept ->> = " + regressionD0.getIntercept() + " to ->> " +
                              D0ord_dim.get( i ).get( D0ord_dim.get( i ).size() - 1 ) );

          elem = new ArrayList<Double>( 2 );
          elem.add( support );
          elem.add( -regressionD0.getSlope() );
          D0final_dim.get( i ).add( elem );
        }
      }

      //D0max = (-regressionD0.getSlope()>D0max?-regressionD0.getSlope():D0max);
      //regressionD0.clear();

      // D2
      ArrayList<ArrayList<Double>> D2final = null;
      ArrayList<ArrayList<ArrayList<Double>>> D2final_dim =
        new ArrayList<ArrayList<ArrayList<Double>>>();
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        System.out.println( "\nD2 ->>> inizio calcolo slope" );
        SimpleRegression regressionD2 = new SimpleRegression();
        int a = 0;
        double k = 0.0;
        double support = 0.0;
        int i = 0;
        //Double D2max = 0.0;
        D2final = new ArrayList<ArrayList<Double>>( 10 );
        ArrayList<Double> elem = null;
        while( i < D2.size() ) {
          k = D2ord.get( i++ );
          System.out.println( "D2: intervallo " + a + " key[" + ( i - 1 ) + "]: " + k + " value: " + D2.get( k ) );
          regressionD2.addData( k, D2.get( k ) );
          support++;
          if( a < splitPointToKeep2.size() && splitPointToKeep2.get( a ).equals( k ) ) {
            System.out.println( "D2 regression slope ->> = " + regressionD2.getSlope() +
                                "regression intercept ->> = " + regressionD2.getIntercept() + " to ->> " + splitPointToKeep2.get( a ) );
            elem = new ArrayList<Double>( 2 );
            elem.add( support );
            elem.add( regressionD2.getSlope() );
            D2final.add( elem );

            regressionD2.clear();
            support = 0.0;
            //regressionD2.addData(k,D2.get(k));
            a++;
          }
        }
        System.out.println( "D2 regression slope ->> = " + regressionD2.getSlope() +
                            "regression intercept ->> = " + regressionD2.getIntercept() + " to ->> " + D2ord.get( D2ord.size() - 1 ) );

        elem = new ArrayList<Double>( 2 );
        elem.add( support );
        elem.add( regressionD2.getSlope() );
        D2final.add( elem );
        regressionD2.clear();
      } else {
        //CSVmulti
        for( int i = 0; i < dim; i++ ) {
          System.out.println( "\nD2_dim[" + i + "] ->>> inizio calcolo slope" );
          SimpleRegression regressionD2 = new SimpleRegression();
          int a = 0;
          double k = 0.0;
          double support = 0.0;
          int j = 0;
          //Double D2max = 0.0;
          D2final_dim.add( i, new ArrayList<ArrayList<Double>>( 10 ) );
          ArrayList<Double> elem = null;
          while( j < D2_dim.get( i ).size() ) {
            k = D2ord_dim.get( i ).get( j++ );
            System.out.println( "D2_dim[" + i + "]: intervallo " + a + " key[" + ( j - 1 ) + "]: " + k + " value: " + D2_dim.get( i ).get( k ) );
            regressionD2.addData( k, D2_dim.get( i ).get( k ) );
            support++;
            if( a < splitPointToKeep2_dim.get( i ).size() &&
                splitPointToKeep2_dim.get( i ).get( a ).equals( k ) ) {
              System.out.println( "D2_dim[" + i + "] regression slope ->> = " + regressionD2.getSlope() +
                                  "regression intercept ->> = " + regressionD2.getIntercept() + " to ->> " +
                                  splitPointToKeep2_dim.get( i ).get( a ) );
              elem = new ArrayList<Double>( 2 );
              elem.add( support );
              elem.add( regressionD2.getSlope() );
              D2final_dim.get( i ).add( elem );

              regressionD2.clear();
              support = 0.0;
              //regressionD2.addData(k,D2.get(k));
              a++;
            }
          }
          System.out.println( "D2_dim[" + i + "] regression slope ->> = " + regressionD2.getSlope() +
                              "regression intercept ->> = " + regressionD2.getIntercept() + " to ->> " +
                              D2ord_dim.get( i ).get( D2ord_dim.get( i ).size() - 1 ) );

          elem = new ArrayList<Double>( 2 );
          elem.add( support );
          elem.add( regressionD2.getSlope() );
          D2final_dim.get( i ).add( elem );
          regressionD2.clear();
        }
      }

      // D3
      ArrayList<ArrayList<Double>> D3final = null;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        System.out.println( "\nD3 ->>> inizio calcolo slope" );

        SimpleRegression regressionD3 = new SimpleRegression();
        int a = 0;
        double k = 0.0;
        double support = 0.0;
        int i = 0;
        D3final = new ArrayList<ArrayList<Double>>( 10 );
        ArrayList<Double> elem;
        while( i < D3.size() ) {
          k = D3ord.get( i++ );
          System.out.println( "D3: intervallo " + a + " key[" + ( i - 1 ) + "]: " + k + " value: " + D3.get( k ) );
          regressionD3.addData( k, D3.get( k ) );
          support++;
          if( a < splitPointToKeep3.size() && splitPointToKeep3.get( a ).equals( k ) ) {
            System.out.println( "D3 regression slope ->> = " + regressionD3.getSlope() +
                                "regression intercept ->> = " + regressionD3.getIntercept() + " to ->> " + splitPointToKeep3.get( a ) );
            elem = new ArrayList<Double>( 2 );
            elem.add( support );
            elem.add( regressionD3.getSlope() / 2 );
            D3final.add( elem );
            regressionD3.clear();
            support = 0.0;
            //regressionD3.addData(k,D3.get(k));
            a++;
          }
        }
        System.out.println( "D3 regression slope (to be divided by 1/2) ->> = " + regressionD3.getSlope() +
                            "regression intercept ->> = " + regressionD3.getIntercept() + " to ->> " + D3ord.get( D3ord.size() - 1 ) );

        elem = new ArrayList<Double>( 2 );
        elem.add( support );
        elem.add( regressionD3.getSlope() / 2 );
        D3final.add( elem );

        regressionD3.clear();
      }

      // Moran's Index
      int i;
      final int scarto = 4;
      Double sum0 = 0.0, sum2 = 0.0, sum1 = 0.0,
        min0 = 1.0, max0 = -1.0,
        min2 = 1.0, max2 = -1.0,
        min1 = 1.0, max1 = -1.0;

      if( MoranIndex ) {
        ArrayList<Double> kl0 = null;
        ArrayList<Double> kl2 = null;
        ArrayList<Double> kl1 = null;
        Double val;

        if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
          System.out.println( "Moran's I ->> = " );

          //Double[] kl = (Double[]) MI.keySet().toArray();
          kl0 = new ArrayList<Double>( M0.keySet() );
          kl2 = new ArrayList<Double>( M2.keySet() );
          kl1 = new ArrayList<Double>( M1.keySet() );
          Collections.sort( kl0 );
          Collections.sort( kl2 );
          Collections.sort( kl1 );
          i = 0;
          while( i < kl0.size() - scarto ) {
            val = M0.get( kl0.get( i ) );
            sum0 += val;
            min0 = ( val < min0 ? val : min0 );
            max0 = ( val > max0 ? val : max0 );
            System.out.println( "M0 Value" + i + "[" + kl0.get( i++ ) + "] =" + val );
          }
          sum0 = sum0 / ( kl0.size() - scarto );
          i = 0;

          while( i < kl2.size() - scarto ) {
            val = M2.get( kl2.get( i ) );
            sum2 += val;
            min2 = ( val < min2 ? val : min2 );
            max2 = ( val > max2 ? val : max2 );
            System.out.println( "M2 Value" + i + "[" + kl2.get( i++ ) + "] =" + val );
          }
          sum2 = sum2 / ( kl2.size() - scarto );
          i = 0;

          while( i < kl1.size() - scarto ) {
            val = M1.get( kl1.get( i ) );
            sum1 += val;
            min1 = ( val < min1 ? val : min1 );
            max1 = ( val > max1 ? val : max1 );
            System.out.println( "M1 Value" + i + "[" + kl1.get( i++ ) + "] =" + val );
          }
          sum1 = sum1 / ( kl1.size() - scarto );
        }
      } // fine if calcolo Moran's index

      // Calcolo percentuale di celle vuote
      ArrayList<Double> klcell = new ArrayList<Double>( Ncell.keySet() );
      Collections.sort( klcell );

      i = 0;
      Double val, avgVuoto = 0.0;
      while( i < klcell.size() - scarto ) {
        val = Ncell.get( klcell.get( i ) );
        avgVuoto += val;
        System.out.println( "PercVuoto Value" + i + "[" + klcell.get( i++ ) + "] =" + val );
      }
      avgVuoto = avgVuoto / ( klcell.size() - scarto );

      // Scelgo i due valori di D0 con supporto massimo
      Double D0_1 = 0.0, D0_2 = 0.0, supp, supp_1, supp_2;
      Double[] D0_1_dim = new Double[10];
      Double[] D0_2_dim = new Double[10];
      Double[] Supp0_1_dim = new Double[10];
      Double[] Supp0_2_dim = new Double[10];
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        ArrayList<Double> elem;
        Double D0;
        if( D0final.size() <= 2 ) {
          elem = D0final.get( 0 );
          D0_1 = elem.get( 1 );
          if( D0final.size() == 1 )
            D0_2 = D0_1;
          else {
            elem = D0final.get( 1 );
            D0_2 = elem.get( 1 );
          }
        } else {
          elem = D0final.get( 0 );
          supp_1 = elem.get( 0 );
          D0_1 = elem.get( 1 );
          elem = D0final.get( 1 );
          supp_2 = elem.get( 0 );
          D0_2 = elem.get( 1 );
          for( i = 2; i < D0final.size(); i++ ) {
            elem = D0final.get( i );
            supp = elem.get( 0 );
            D0 = elem.get( 1 );
            if( supp <= supp_1 && supp <= supp_2 )
              continue;
            else {
              if( supp > supp_2 && supp <= supp_1 ) {
                // sostituisco il secondo
                supp_2 = supp;
                D0_2 = D0;
              } else if( supp > supp_1 && supp <= supp_2 ) {
                // sostituisco il primo con shift
                supp_1 = supp_2;
                D0_1 = D0_2;
                supp_2 = supp;
                D0_2 = D0;
              } else if( supp > supp_1 && supp > supp_2 ) {
                // devo decidere chi togliere tra gli attuali due valori di D0
                if( supp_1 >= supp_2 ) {
                  // sostituisco il secondo
                  supp_2 = supp;
                  D0_2 = D0;
                } else {
                  // sostituisco il primo con shift
                  supp_1 = supp_2;
                  D0_1 = D0_2;
                  supp_2 = supp;
                  D0_2 = D0;
                }
              }
            }
          }
        }
      } else {
        //CSVmulti
        ArrayList<Double> elem;
        Double D0;
        for( i = 0; i < dim; i++ ) {
          if( D0final_dim.get( i ).size() <= 2 ) {
            elem = D0final_dim.get( i ).get( 0 );
            D0_1_dim[i] = elem.get( 1 );
            Supp0_1_dim[i] = elem.get( 0 );
            if( D0final_dim.get( i ).size() == 1 ) {
              D0_2_dim[i] = D0_1_dim[i];
              Supp0_2_dim[i] = Supp0_1_dim[i];
            } else {
              elem = D0final_dim.get( i ).get( 1 );
              D0_2_dim[i] = elem.get( 1 );
              Supp0_2_dim[i] = elem.get( 0 );
            }
          } else {
            elem = D0final_dim.get( i ).get( 0 );
            supp_1 = elem.get( 0 );
            Supp0_1_dim[i] = supp_1;
            D0_1_dim[i] = elem.get( 1 );
            elem = D0final_dim.get( i ).get( 1 );
            supp_2 = elem.get( 0 );
            Supp0_2_dim[i] = supp_2;
            D0_2_dim[i] = elem.get( 1 );
            for( int j = 2; j < D0final_dim.get( i ).size(); j++ ) {
              elem = D0final_dim.get( i ).get( j );
              supp = elem.get( 0 );
              D0 = elem.get( 1 );
              if( supp <= supp_1 && supp <= supp_2 )
                continue;
              else {
                if( supp > supp_2 && supp <= supp_1 ) {
                  // sostituisco il secondo
                  supp_2 = supp;
                  Supp0_2_dim[i] = supp;
                  D0_2_dim[i] = D0;
                } else if( supp > supp_1 && supp <= supp_2 ) {
                  // sostituisco il primo con shift
                  supp_1 = supp_2;
                  Supp0_1_dim[i] = supp_1;
                  D0_1_dim[i] = D0_2_dim[i];
                  supp_2 = supp;
                  Supp0_2_dim[i] = supp_2;
                  D0_2_dim[i] = D0;
                } else if( supp > supp_1 && supp > supp_2 ) {
                  // devo decidere chi togliere tra gli attuali due valori di D0
                  if( supp_1 >= supp_2 ) {
                    // sostituisco il secondo
                    supp_2 = supp;
                    Supp0_2_dim[i] = supp_2;
                    D0_2_dim[i] = D0;
                  } else {
                    // sostituisco il primo con shift
                    supp_1 = supp_2;
                    Supp0_1_dim[i] = supp_1;
                    D0_1_dim[i] = D0_2_dim[i];
                    supp_2 = supp;
                    Supp0_2_dim[i] = supp_2;
                    D0_2_dim[i] = D0;
                  }
                }
              }
            }
          }
        }
      }

      // Scelgo i due valori di D2 con supporto massimo
      Double[] D2_1_dim = new Double[10];
      Double[] D2_2_dim = new Double[10];
      Double[] Supp2_1_dim = new Double[10];
      Double[] Supp2_2_dim = new Double[10];
      Double D2_1 = 0.0, D2_2 = 0.0;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        Double D2;
        ArrayList<Double> elem;
        if( D2final.size() <= 2 ) {
          elem = D2final.get( 0 );
          D2_1 = elem.get( 1 );
          if( D2final.size() == 1 )
            D2_2 = D2_1;
          else {
            elem = D2final.get( 1 );
            D2_2 = elem.get( 1 );
          }
        } else {
          elem = D2final.get( 0 );
          supp_1 = elem.get( 0 );
          D2_1 = elem.get( 1 );
          elem = D2final.get( 1 );
          supp_2 = elem.get( 0 );
          D2_2 = elem.get( 1 );
          for( i = 2; i < D2final.size(); i++ ) {
            elem = D2final.get( i );
            supp = elem.get( 0 );
            D2 = elem.get( 1 );
            if( supp <= supp_1 && supp <= supp_2 )
              continue;
            else {
              if( supp > supp_2 && supp <= supp_1 ) {
                // sostituisco il secondo
                supp_2 = supp;
                D2_2 = D2;
              } else if( supp > supp_1 && supp <= supp_2 ) {
                // sostituisco il primo con shift
                supp_1 = supp_2;
                D2_1 = D2_2;
                supp_2 = supp;
                D2_2 = D2;
              } else if( supp > supp_1 && supp > supp_2 ) {
                // devo decidere chi togliere tra gli attuali due valori di D0
                if( supp_1 >= supp_2 ) {
                  // sostituisco il secondo
                  supp_2 = supp;
                  D2_2 = D2;
                } else {
                  // sostituisco il primo con shift
                  supp_1 = supp_2;
                  D2_1 = D2_2;
                  supp_2 = supp;
                  D2_2 = D2;
                }
              }
            }
          }
        }
      } else {
        //CSVmulti
        for( i = 0; i < dim; i++ ) {
          Double D2;
          ArrayList<Double> elem;
          if( D2final_dim.get( i ).size() <= 2 ) {
            elem = D2final_dim.get( i ).get( 0 );
            D2_1_dim[i] = elem.get( 1 );
            Supp2_1_dim[i] = elem.get( 0 );
            if( D2final_dim.get( i ).size() == 1 ) {
              D2_2_dim[i] = D2_1_dim[i];
              Supp2_2_dim[i] = Supp2_1_dim[i];
            } else {
              elem = D2final_dim.get( i ).get( 1 );
              D2_2_dim[i] = elem.get( 1 );
              Supp2_2_dim[i] = elem.get( 0 );
            }
          } else {
            elem = D2final_dim.get( i ).get( 0 );
            supp_1 = elem.get( 0 );
            Supp2_1_dim[i] = supp_1;
            D2_1_dim[i] = elem.get( 1 );
            elem = D2final_dim.get( i ).get( 1 );
            supp_2 = elem.get( 0 );
            Supp2_2_dim[i] = supp_2;
            D2_2_dim[i] = elem.get( 1 );
            for( int j = 2; j < D2final_dim.get( i ).size(); j++ ) {
              elem = D2final_dim.get( i ).get( j );
              supp = elem.get( 0 );
              D2 = elem.get( 1 );
              if( supp <= supp_1 && supp <= supp_2 )
                continue;
              else {
                if( supp > supp_2 && supp <= supp_1 ) {
                  // sostituisco il secondo
                  supp_2 = supp;
                  Supp2_2_dim[i] = supp_2;
                  D2_2_dim[i] = D2;
                } else if( supp > supp_1 && supp <= supp_2 ) {
                  // sostituisco il primo con shift
                  supp_1 = supp_2;
                  Supp2_1_dim[i] = supp_1;
                  D2_1_dim[i] = D2_2_dim[i];
                  supp_2 = supp;
                  Supp2_2_dim[i] = supp_2;
                  D2_2_dim[i] = D2;
                } else if( supp > supp_1 && supp > supp_2 ) {
                  // devo decidere chi togliere tra gli attuali due valori di D0
                  if( supp_1 >= supp_2 ) {
                    // sostituisco il secondo
                    supp_2 = supp;
                    Supp2_2_dim[i] = supp_2;
                    D2_2_dim[i] = D2;
                  } else {
                    // sostituisco il primo con shift
                    supp_1 = supp_2;
                    Supp2_1_dim[i] = supp_1;
                    D2_1_dim[i] = D2_2_dim[i];
                    supp_2 = supp;
                    Supp2_2_dim[i] = supp_2;
                    D2_2_dim[i] = D2;
                  }
                }
              }
            }
          }
        }
      }

      // Scelgo i due valori di D3 con supporto massimo
      Double D3_1 = 0.0, D3_2 = 0.0;
      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        Double D3;
        ArrayList<Double> elem;
        if( D3final.size() <= 2 ) {
          elem = D3final.get( 0 );
          D3_1 = elem.get( 1 );
          if( D3final.size() == 1 )
            D3_2 = D3_1;
          else {
            elem = D3final.get( 1 );
            D3_2 = elem.get( 1 );
          }
        } else {
          elem = D3final.get( 0 );
          supp_1 = elem.get( 0 );
          D3_1 = elem.get( 1 );
          elem = D3final.get( 1 );
          supp_2 = elem.get( 0 );
          D3_2 = elem.get( 1 );
          for( i = 2; i < D3final.size(); i++ ) {
            elem = D3final.get( i );
            supp = elem.get( 0 );
            D3 = elem.get( 1 );
            if( supp <= supp_1 && supp <= supp_2 )
              continue;
            else {
              if( supp > supp_2 && supp <= supp_1 ) {
                // sostituisco il secondo
                supp_2 = supp;
                D3_2 = D3;
              } else if( supp > supp_1 && supp <= supp_2 ) {
                // sostituisco il primo con shift
                supp_1 = supp_2;
                D3_1 = D3_2;
                supp_2 = supp;
                D3_2 = D3;
              } else if( supp > supp_1 && supp > supp_2 ) {
                // devo decidere chi togliere tra gli attuali due valori di D0
                if( supp_1 >= supp_2 ) {
                  // sostituisco il secondo
                  supp_2 = supp;
                  D3_2 = D3;
                } else {
                  // sostituisco il primo con shift
                  supp_1 = supp_2;
                  D3_1 = D3_2;
                  supp_2 = supp;
                  D3_2 = D3;
                }
              }
            }
          }
        }
      }

      ArrayList<ArrayList<Double>> partition = new ArrayList<ArrayList<Double>>();
      String[] partitionType = new String[10];
      if( inputType.equalsIgnoreCase( "CSVmulti" ) ) {
        //Partizionamento
        // Decisone di quale partizionamento produrre per ogni dimensione
        // scelgo la DO_1 se ha supporto massimo, altrimenti faccio la media tra DO_1 e D0_2 considerando a 1 i valori superiori a 0.9, lo stesso per D2
        // applico albero di decisione:
        // D0 < 0.25 -> D2 < 0.5 -> quadtree else rtree
        // 0.25 >= D0 -> D2 >= 0.75 -> regular grid else D2 < 0.5 -> quadtree else rtree
        Double[] D0 = new Double[10];
        Double[] D2 = new Double[10];
        for( i = 0; i < dim; i++ ) {
          if( Supp0_1_dim[i] > Supp0_2_dim[i] )
            D0[i] = D0_1_dim[i];
          else {
            if( Supp0_1_dim[i] < Supp0_2_dim[i] )
              D0[i] = D0_2_dim[i];
            else
              D0[i] = ( D0_1_dim[i] + ( D0_2_dim[i] >= 0.9 ? 1 : D0_2_dim[i] ) ) / 2;
          }
          if( Supp2_1_dim[i] > Supp2_2_dim[i] )
            D2[i] = D2_1_dim[i];
          else {
            if( Supp2_1_dim[i] < Supp2_2_dim[i] )
              D2[i] = D2_2_dim[i];
            else
              D2[i] = ( D2_1_dim[i] + ( D2_2_dim[i] >= 0.9 ? 1 : D2_2_dim[i] ) ) / 2;
          }
        }

        // Calcolo il numero di divisioni da applicare per ogni dimensione
        Double dim_file = conf.getDouble( "dimFile", 72142150.0 );
        Double dim_split = conf.getDouble( "dimSplit", 1024.0 * 1024.0 );

        long num_tot_split = (long) Math.ceil( dim_file / dim_split );
        long num_div = (long) Math.ceil( Math.pow( (double) num_tot_split, (double) ( 1.0 / dim ) ) );
        System.out.println( "num_div: " + num_div );

        for( i = 0; i < dim; i++ ) {
          HashMap<Long, Long> bcc = bc_dim.get( i );
          double orig = ng.orig.get( i );
          double size = ng.size.get( i );
          if( D0[i] < 0.25 ) {
            if( D2[i] < 0.5 ) {
              partitionType[i] = "QuadTree";
              System.out.println( "Chiamata Quadtree[" + i + "] orig size tileSide soglia: " +
                                  orig + " " + size + " " + ng.tileSide.get( i ) + " " + Math.ceil( card / num_div ) );
              partition.add( i, QuadTree( bcc, orig, orig, size, ng.tileSide.get( i ),
                                          (long) Math.ceil( card / num_div ) ) );
            } else {
              partitionType[i] = "RTree";
              System.out.println( "Chiamata Rtree[" + i + "] orig size tileSide soglia: " +
                                  orig + " " + size + " " + ng.tileSide.get( i ) + " " + Math.ceil( card / num_div ) );
              partition.add( i, RTree( bcc, orig, orig, size, ng.tileSide.get( i ),
                                       (long) Math.ceil( card / num_div ) ) );
            }
          } else {
            if( D2[i] >= 0.75 ) {
              partitionType[i] = "RegularGrid";
              System.out.println( "Chiamata RegularGrid[" + i + "] orig size soglia: " +
                                  orig + " " + size + " " + Math.ceil( card / num_div ) );
              partition.add( i, RegGrid( orig, size, num_div ) );
            } else if( D2[i] < 0.5 ) {
              partitionType[i] = "QuadTree";
              System.out.println( "Chiamata Quadtree[" + i + "] orig size tileSide soglia: " +
                                  orig + " " + size + " " + ng.tileSide.get( i ) + " *" + Math.ceil( card / num_div ) );
              partition.add( i, QuadTree( bcc, orig, orig, size, ng.tileSide.get( i ),
                                          (long) Math.ceil( card / num_div ) ) );
            } else {
              partitionType[i] = "RTree";
              System.out.println( "Chiamata Rtree[" + i + "] orig size tileSide soglia: " +
                                  orig + " " + size + " " + ng.tileSide.get( i ) + " " + Math.ceil( card / num_div ) );
              partition.add( i, RTree( bcc, orig, orig, size, ng.tileSide.get( i ),
                                       (long) Math.ceil( card / num_div ) ) );
            }
          }
        }
      }

      // Generazione OUTPUT: <datasetName> <FD0_1> <FD0_2> <FD2_1> <FD2_2> <FD3_1> <FD3_2> <MoranI0min> <MoranI0avg> <MoranI0max> <MoranI2min> <MoranI2avg> <MoranI2max> <MoranI3min> <MoranI3avg> <MoranI3max>
      Text p1 = new Text();
      p1.set( inputFile.substring( inputFile.indexOf( "/" ) + 1 ) );
      Text p2 = new Text();

      reduceEnd = System.currentTimeMillis();
      //Add this to have a measure of the time of the reduce task
      //(reduceEnd-reduceStart)+" "+

      if( inputType.equalsIgnoreCase( "WKT" ) || inputType.equalsIgnoreCase( "CSV" ) ) {
        String s = "Card: " + card +
                   "\narea_AVG: " + areaAVG / n +
                   " x_AVG: " + xAVG / n +
                   " y_AVG: " + yAVG / n +
                   " vertAVG: " + vertAVG / n +
                   "\nDF (D0_1, D0_2, D2_1, D2_2, D3_1, D3_2): " + String.format( "%.3f", D0_1 ) + " " + String.format( "%.3f", D0_2 ) + " " +
                   String.format( "%.3f", D2_1 ) + " " + String.format( "%.3f", D2_2 ) + " " +
                   String.format( "%.3f", D3_1 ) + " " + String.format( "%.3f", D3_2 ) +
                   "\nPercCelleVuote: " + String.format( "%.3f", avgVuoto );
        ;
        if( MoranIndex )
          s += "\nMI (MI0_min, MI0_avg, MI0_max, ...):" + String.format( "%.3f", min0 ) + " " + String.format( "%.3f", sum0 ) + " " + String.format( "%.3f", max0 ) + " " +
               String.format( "%.3f", min1 ) + " " + String.format( "%.3f", sum1 ) + " " + String.format( "%.3f", max1 ) + " " +
               String.format( "%.3f", min2 ) + " " + String.format( "%.3f", sum2 ) + " " + String.format( "%.3f", max2 );
        p2.set( s );
      } else {
        String s = "";
        for( i = 0; i < dim; i++ ) {
          s += "Dim[" + i + "]:";
          s += " D0_1: " + String.format( "%.3f", D0_1_dim[i] ) + " (" + String.format( "%.3f", Supp0_1_dim[i] ) + ")";
          s += " D0_2: " + String.format( "%.3f", D0_2_dim[i] ) + " (" + String.format( "%.3f", Supp0_2_dim[i] ) + ")";
          s += " D2_1: " + String.format( "%.3f", D2_1_dim[i] ) + " (" + String.format( "%.3f", Supp2_1_dim[i] ) + ")";
          s += " D2_2: " + String.format( "%.3f", D2_2_dim[i] ) + " (" + String.format( "%.3f", Supp2_2_dim[i] ) + ")\n";
        }

        for( i = 0; i < dim; i++ ) {
          s += "Partition[" + i + "]: " + partitionType[i] + " (";
          for( int j = 0; j < partition.get( i ).size(); j++ )
            s += ( j == 0 ? String.format( "%f", partition.get( i ).get( j ) ) : "-" + String.format( "%f", partition.get( i ).get( j ) ) );
          s += ")\n";
        }
        p2.set( "CARD: " + card + "\n" + s );
      }
      // Write nel file di output
      context.write( p1, p2 );

      System.out.println( "REDUCE-cleanUP: end" );
      // Detecting clusters when D0 in the range 0.5 - 1.5
		  /*
		  if (D0final > 0.5 && D0final < 1.5) {
			  Grid grid;
			  HashMap<Integer,Integer> bcMed, cluster = new HashMap<Integer,Integer>();
			  System.out.println("Cerco clusters...");
			  if (type.equalsIgnoreCase("MultipleGrid")) {
				  int ind = minNumGriglie/2; boolean adjacent;
				  bcMed = boxCount.get(ind);
				  grid = griglia.get(ind);
				  System.out.println("Griglia: "+ind+" cell size: "+grid.width+ " numRows: "+grid.numRows+" numCol: "+grid.numColumns);
				  for (Integer m: bcMed.keySet()) {
					  adjacent = false;
					  for (Integer n: cluster.keySet()) {
						  if (grid.getDistance(m, n) == 0) {
						     cluster.remove(n);
						     adjacent = true;
						  }
					  if (!adjacent)
						  cluster.put(m, bc.get(m));
					  }
				  }
				  // cluster trovati
				  System.out.println("Cerco clusters...");

			  } else {
				  bcMed = boxCount.get(0);
				  grid = griglia.get(0);
			  }
		  }
		  */
    }

    protected ArrayList<Double> QuadTree( HashMap<Long, Long> bcc, double origGriglia, double orig, double size, double cellSide,
                                          long thr ) {
      ArrayList<Double> part = new ArrayList<Double>();
      // parto dall'origine
      part.add( 0, new Double( orig ) );

      long j;
      // calcolo la cella da cui parto in base all'origine della griglia
      if( orig == origGriglia )
        j = 1L;
      else
        j = ( (long) Math.ceil( (double) ( orig - origGriglia ) / cellSide ) );

      long totleft = 0L;

      double split = orig;
      double midpoint = orig + ( size / 2 );
      boolean left = true;
      while( totleft < thr && split < orig + size ) {
        if( bcc.containsKey( j ) )
          totleft += bcc.get( j );
        split += cellSide;
        j++;
        if( split > midpoint ) left = false;
      }
      if( split >= orig + size )
        part.add( 1, new Double( orig + size ) );
      else if( left ) {
        // divido a sinistra
        part = QuadTree( bcc, origGriglia, orig, size / 2, cellSide, thr );
        part.addAll( QuadTree( bcc, origGriglia, orig + ( size / 2 ), size / 2, cellSide, thr ) );
      } else
        // divido a destra
        part.addAll( QuadTree( bcc, origGriglia, orig + ( size / 2 ), size / 2, cellSide, thr ) );

      return part;
    }

    protected ArrayList<Double> RTree( HashMap<Long, Long> bcc, double origGriglia, double orig, double size, double cellSide,
                                       long thr ) {
      ArrayList<Double> part = new ArrayList<Double>();
      long j;
      // calcolo la cella da cui parto in base all'origine della griglia
      if( orig == origGriglia )
        j = 1L;
      else
        j = ( (long) Math.ceil( (double) ( orig - origGriglia ) / cellSide ) );

      long jstart = j;

      double split = orig;
      long totleft = 0L;
      boolean first = true;
      while( totleft < thr && split < orig + size ) {
        if( bcc.containsKey( j ) ) {
          totleft += bcc.get( j );
          if( first ) {
            first = false;
            part.add( 0, new Double( orig + cellSide * ( j - jstart ) ) );
          }
        }
        j++;
        split += cellSide;
      }
      if( split >= orig + size )
        if( part.size() == 0 )
          // caso particolare di blocco vuoto
          part.add( 0, new Double( orig ) );
        else
          part.add( 1, new Double( orig + size ) );
      else
        part.addAll( RTree( bcc, origGriglia, orig + ( cellSide * ( j - jstart ) ), size - ( cellSide * ( j - jstart ) ), cellSide, thr ) );

      return part;
    }

    protected ArrayList<Double> RegGrid( double orig, double size, long div ) {
      ArrayList<Double> part = new ArrayList<Double>();
      // parto dall'origine
      part.add( 0, new Double( orig ) );
      for( int i = 1; i <= div; i++ ) {
        part.add( i, new Double( orig + ( size / div ) * i ) );
      }
      return part;
    }
  }


}