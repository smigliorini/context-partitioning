package it.univr.restaurant.descriptors;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.util.Pair;
import it.univr.util.ReflectionUtil;
import it.univr.restaurant.RestaurantCSVInputFormat;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

import static edu.umn.cs.spatialHadoop.operations.FileMBR.fileMBR;
import static it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce.runMBBoxMapReduce;
import static java.lang.Double.*;
import static java.lang.Integer.parseInt;
import static java.lang.Math.*;
import static java.lang.String.format;
import static org.apache.hadoop.mapreduce.lib.input.TextInputFormat.setInputPaths;
import static org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath;

/**
 * Main class for the computation of the computation of E0, E2, E3 and
 * MoranIndex spatial descriptors, as well as the determination of the best
 * partitioning technique based on the dataset distribution.
 *
 * @author Alberto Belussi
 * @author Sara Migliorini
 */

public class OneGrid extends Configured {

  // === Attributes ============================================================

  public static final String computeMbrPar = "compute";
  public static final String wktFormat = "WKT";
  public static final String csvFormat = "CSV";
  public static final String csvMultiFormat = "CSVmulti";
  public static final String oneGridAlgorithm = "oneGrid";
  public static final String multiGridAlgorithm = "multipleGrid";

  protected static final int minNumGrids = 12;
  protected static final int numGrids = 12;
  protected static final int maxNumDims = 10;
  protected static final long gridId = 1000000000;
  // todo: workaround
  protected static final long defaultSplitSize = 1024 * 1024 * 128;

  private static final String shapePar = "shape";
  private static final String wktShape = "wkt";
  private static final String rectShape = "rect";

  private static final String dimFileLabel = "dimFile";
  private static final String dimSplitLabel = "dimSplit";
  private static final String mbrLabel = "mbr";
  private static final String cellSideLabel = "cellSide";
  private static final String algoTypeLabel = "type";
  private static final String inputFileLabel = "inputFile";
  private static final String inputTypeLabel = "inputType";
  private static final String numDimsLabel = "dim";
  private static final String moranIndexLabel = "MoranIndex";
  private static final String contextPartLabel = "context";

  private static final Long cardIndex = -1L;
  private static final Long avgAreaIndex = -2L;
  private static final Long avgXIndex = -3L;
  private static final Long avgYIndex = -4L;
  private static final Long avgVertIndex = -5L;
  private static final Long geomCounterIndex = -6L;

  private static final String mapPhaseLabel = "[MAP]";
  private static final String reducePhaseLabel = "[REDUCE]";
  private static final String setupMethodLabel = "[setup]";
  private static final String mapMethodLabel = "[map]";
  private static final String reduceMethodLabel = "[reduce]";
  private static final String cleanupMethodLabel = "[cleanup]";

  // === Properties ============================================================

  private String mbr;
  private int numReducers;
  private Integer[] contextPart;
  private Path inputPath;
  private Path outputPath;
  private String inputType;
  private Double[] cellSides; // = new Double[10];
  private Integer numDims;
  private String moranIndex;

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */

  public OneGrid(String[] args )
    throws IOException, InterruptedException, ClassNotFoundException {

    if( !checkArguments( args ) ) {
      System.exit( 0 );
    }
    this.numReducers = 1;
    this.cellSides = new Double[maxNumDims];

    processMbrParameter( args[3] );
    recomputeCellSides();
  }


  /**
   * MISSING_COMMENT
   *
   * @param args
   * @throws Exception
   */

  public static void main( String[] args ) throws Exception {
    final long start = System.currentTimeMillis();
    System.out.printf( "START: %d%n", start );

    final int res = run( new OneGrid( args ) );

    final long end = System.currentTimeMillis();
    System.out.printf( "END: %d%n", end );
    System.out.printf( "DURATION: %d%n", ( end - start ) );
    System.exit( res );
  }


  /**
   * MISSING_COMMENT
   *
   * @param o
   * @return
   * @throws Exception
   */

  public static int run( OneGrid o ) throws Exception {
    // define the new job configuration
    final Configuration conf = new Configuration();

    final FileSystem fs = FileSystem.get( conf );
    // determine the file dimension
    conf.setDouble( dimFileLabel, fs.getContentSummary( o.inputPath ).getSpaceConsumed() );
    // todo: sistemare!!!
    // conf.setDouble( "dimSplit", fs.getDefaultBlockSize( o.inputPath ) );
    conf.setDouble( dimSplitLabel, defaultSplitSize );
    // the MBR computed by the previous job
    conf.set( mbrLabel, o.mbr );
    // cell side for each dimension
    for( int i = 0; i < o.numDims; i++ ) {
      conf.setDouble( cellSideLabel + i, o.cellSides[i] );
    }
    // kind of computation: oneGrid or multipleGrid
    conf.set( algoTypeLabel, oneGridAlgorithm );
    // number of context fields
    for( int i = 0; i < o.numDims; i++ ) {
      conf.setInt( contextPartLabel + i, o.contextPart[i] );
    }
    // input file
    conf.set( inputFileLabel, o.inputPath.toString() );
    // kind of CSV or WKT or CSV_multi
    conf.set( inputTypeLabel, o.inputType );
    // number of dimensions
    conf.setInt( numDimsLabel, o.numDims );
    // flag for the computation of the Moran's Index
    conf.setBoolean( moranIndexLabel, o.moranIndex.equalsIgnoreCase( "MI" ) );
    // kind of separator
    conf.set( "mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t" );

    final Job job = Job.getInstance( conf, "oneGrid" );
    job.setJarByClass( OneGrid.class );

    // set job input format
    job.setInputFormatClass( TextInputFormat.class );

    // set map class and the map output key and value classes
    job.setMapOutputKeyClass( LongWritable.class );
    job.setMapOutputValueClass( LongWritable.class );
    job.setMapperClass( OneGridMapper.class );

    // set reduce class and the reduce output key and value classes
    job.setReducerClass( OneGridReducer.class );

    // set job output format
    job.setOutputFormatClass( TextOutputFormat.class );
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( Text.class );

    // add the input file as job input to the variable inputFile
    setInputPaths( job, o.inputPath );

    // set the output path for the job results to the variable outputPath
    setOutputPath( job, o.outputPath );

    // set the number of reducers using variable numberReducers
    job.setNumReduceTasks( o.numReducers );

    // set the jar class
    job.setJarByClass( OneGrid.class );

    // execute the job
    return job.waitForCompletion( true ) ? 0 : 1;
  }


  /* The methods checks the number and type of the given arguments and prints
   * the help menu in case of an invalid number of arguments.
   *
   * @param args
   * @return
   */

  /**
   * MISSING_COMMENT
   *
   * @param grids
   * @param mbr
   * @param cellSide
   * @param boxCount
   */

  protected static void initializeGrids
  ( List<Grid> grids,
    List<NGrid> nGrids,
    String mbr,
    Double cellSide,
    Integer numDims,
    List<HashMap<Long, Long>> boxCount,
    String inputType,
    String algorithmType,
    String phase,
    String method ) {

    if( grids == null ) {
      throw new NullPointerException();
    }
    if( nGrids == null ) {
      throw new NullPointerException();
    }
    if( mbr == null ) {
      throw new NullPointerException();
    }
    if( cellSide == null ) {
      throw new NullPointerException();
    }
    if( numDims == null ) {
      throw new NullPointerException();
    }
    if( boxCount == null ) {
      throw new NullPointerException();
    }
    if( inputType == null ) {
      throw new NullPointerException();
    }
    if( algorithmType == null ) {
      throw new NullPointerException();
    }
    if( phase == null ) {
      throw new NullPointerException();
    }
    if( method == null ) {
      throw new NullPointerException();
    }

    if( inputType.equalsIgnoreCase( wktFormat ) ) {
      if( algorithmType.equalsIgnoreCase( oneGridAlgorithm ) ) {
        // case: unique grid
        grids.add( new Grid( mbr, cellSide ) );
        System.out.printf
          ( "%s%s CASE WKT: grid -> %s%n",
            phase, method,
            grids.get( 0 ).toString() );
        // build the box counting structure for storing the counting
        boxCount.add( new HashMap<>() );

      } else if( algorithmType.equalsIgnoreCase( multiGridAlgorithm ) ) {
        // case: multiple grids
        // build the grids starting from the cell side of the first dimension!
        grids.add( new Grid( mbr, cellSide ) );
        System.out.printf
          ( "%s%s CASE WKT: grid[0] -> %s%n",
            phase, method,
            grids.get( 0 ).toString() );
        boxCount.add( new HashMap<>() );

        double currentCellSide = cellSide;
        for( int i = 1; i < ( numGrids - numDims + 2 ); i++ ) {
          currentCellSide = currentCellSide * 2;
          grids.add( new Grid( mbr, currentCellSide ) );
          System.out.printf
            ( "%s%s CASE WKT: grid[%i] -> %s%n",
              phase, method, i,
              grids.get( i ).toString() );
          boxCount.add( new HashMap<>() );
        }
      }
    } else { // case: CSV
      if( algorithmType.equalsIgnoreCase( oneGridAlgorithm ) ) {
        // case: unique grid
        final NGrid ng = new NGrid( mbr, cellSide, numDims );
        if( !ng.isValid ) {
          System.out.println( "Ngrid is not valid!" );
          return;
        }

        nGrids.add( ng );
        System.out.printf
          ( "%s%s CASE CSV: ngrid[0] -> %s%n",
            phase, method, nGrids.get( 0 ).toString() );
        boxCount.add( new HashMap<>() );

      } else if( algorithmType.equalsIgnoreCase( multiGridAlgorithm ) ) {
        // case: multiple grids
        final NGrid ng = new NGrid( mbr, cellSide, numDims );
        nGrids.add( ng );
        System.out.printf
          ( "%s%s CASE CSV: ngrid[0] -> %s%n",
            phase, method, nGrids.get( 0 ).toString() );
        boxCount.add( new HashMap<>() );

        double currentCellSide = cellSide;
        for( int i = 1; i < ( numGrids - numDims + 2 ); i++ ) {
          currentCellSide = currentCellSide * 2;
          nGrids.add( new NGrid( mbr, currentCellSide, numDims ) );
          System.out.printf
            ( "%s%s CASE WKT: grid[%i] -> %s%n",
              phase, method, i, grids.get( i ).toString() );
          boxCount.add( new HashMap<>() );
        }
      }
    }
  }

  private boolean checkArguments( String[] args ) {
    if( args.length != 8 ) {
      System.out.printf
        ( "Invalid number of arguments: %d, required: %d.%n", args.length, 8 );
      for( int i = 0; i < args.length; i++ ) {
        System.out.printf( "args[%d] = %s.%n", i, args[i] );
      }
      printMenu();
      return false;
    }

    this.inputType = args[0];

    try {
      this.numDims = parseInt( args[1] );
    } catch( NumberFormatException e ) {
      System.out.printf( "Invalid dimension: %s.%n", args[1] );
      printMenu();
      return false;
    }

    this.mbr = args[2];

    try {
      // only check the value, it will be assigned later
      parseDouble( args[3] );
    } catch( NumberFormatException e ) {
      System.out.printf( "Invalid cells side: %s.%n", args[3] );
      printMenu();
      return false;
    }

    this.moranIndex = args[4];

    final String tmp = args[5];
    if( !tmp.contains(contextPartLabel) ) {
      System.out.printf( "Invalid context: %s.%n", args[5] );
      printMenu();
      return false;
    }

    final int start = tmp.indexOf( "=" );
    if( start == -1 ) {
      throw new IllegalArgumentException( format( "Illegal context specification: %s", tmp ));
    }

    this.contextPart = new Integer[numDims];
    final String value = tmp.substring( start+1 );
    int i=0;
    final StringTokenizer tk = new StringTokenizer( value, "," );
    while( tk.hasMoreTokens() ) {
      final String token = tk.nextToken();
      contextPart[i] = Integer.parseInt( token );
      i++;
    }

    this.inputPath = new Path( args[6] );
    this.outputPath = new Path( args[7] );

    return true;
  }

  /**
   * The method prints an informative menu helper for using the program.
   */

  private void printMenu() {
    System.out.printf
      ( "Usage: OneGrid "
        + "<%s|%s|%s> " // 0
        + "<dim> " // 1
        + "<mbr (Rectangle: (xmin,ymin,...)-(xmax,ymax,...) or compute)> " // 2
        + "<cell_side (0.0 if mbr=compute)> " // 3
        + "<moran_index (MI|noMI)> " // 4
        + "<context> " // 5
        + "<input_path> " // 6
        + "<output_path>%n", // 7
        wktFormat, csvFormat, csvMultiFormat );
  }

  /**
   * The method processes the MBR string given as parameter.
   *
   * @param cellSide
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */


  private void processMbrParameter( String cellSide )
    throws IOException, InterruptedException, ClassNotFoundException {

    final Configuration confMBR = new Configuration();

    // compute the MBR
    if( this.mbr.equals( computeMbrPar ) ) {
      if( this.inputType.equalsIgnoreCase( wktFormat ) ||
          this.inputType.equalsIgnoreCase( csvFormat ) ) {

        if( this.inputType.equalsIgnoreCase( wktFormat ) ) {
          confMBR.set( shapePar, wktShape );
        } else if( this.inputType.equalsIgnoreCase( "CSV" ) ) {
          confMBR.set( shapePar, rectShape );
        }
        // compute the MBR by using SpatialHadoop
        final Partition p = fileMBR
          ( this.inputPath, new OperationsParams( confMBR ) );

        double deltax, deltay;
        deltax = p.x2 - p.x1;
        deltay = p.y2 - p.y1;
        if( deltay > deltax ) {
          deltax = deltay;
        } else {
          deltay = deltax;
        }

        this.mbr = format(
          "Rectangle: (%.15f,%.15f)-(%.15f,%.15f)",
          p.x1, p.y1, ( p.x1 + deltax ), ( p.y1 + deltay ) );

        this.cellSides[0] = p.getWidth() / 2;
        this.cellSides[1] = p.getHeight() / 2;

      } else if( this.inputType.equalsIgnoreCase( csvMultiFormat ) ) {
        // todo!!!
        final Vector<Path> inputs = new Vector<>();
        inputs.add( inputPath );
        final OperationConf config = new OperationConf( confMBR );
        config.setFileInputPaths( inputs );
        config.setOutputDirectory( new Path( "tmp/" ) );
        final Pair<ContextData, ContextData> mmbox = runMBBoxMapReduce
          ( config, RestaurantCSVInputFormat.class, false );

        final StringBuilder lb = new StringBuilder();
        final StringBuilder rb = new StringBuilder();
        final String[] fields = mmbox.getLeft().getContextFields();
        for( int i = 0; i < fields.length; i++ ) {
          final String f = fields[i];
          final Double min = (Double) ReflectionUtil.readMethod( f, mmbox.getLeft() );
          lb.append( format( "%.15f", min ) );
          if( i < fields.length - 1 ) {
            lb.append( "," );
          }
          final Double max = (Double) ReflectionUtil.readMethod( f, mmbox.getRight() );
          rb.append( format( "%.15f", max ) );
          if( i < fields.length - 1 ) {
            rb.append( "," );
          }
        }

        this.mbr = format( "Rectangle: (%s)-(%s)", lb.toString(), rb.toString() );
        // todo!!
        this.cellSides[0] = parseDouble( cellSide );
        this.cellSides[1] = parseDouble( cellSide );
      }

    } else { // assigned MBR
      this.cellSides[0] = parseDouble( cellSide );
      this.cellSides[1] = this.cellSides[0];
    }
  }

  /**
   * The method recomputes the cell sides to have at least a number of grids
   * equal to {@code minNumGrids}.
   */

  private void recomputeCellSides() {
    if( this.inputType.equalsIgnoreCase( wktFormat ) ) {
      final Grid g = new Grid( mbr, cellSides[0] );
      if( ( this.cellSides[0] * ( pow( 2, minNumGrids ) ) ) > g.width ) {
        this.cellSides[0] = g.width / ( pow( 2, minNumGrids ) );
        this.cellSides[1] = g.height / ( pow( 2, minNumGrids ) );
      }

    } else {
      // the granularity is computed also on the basis of the space dimension
      System.out.printf( "MBR: %s cellSide[0]: %.15f Dim: %d.%n",
                         this.mbr, cellSides[0], numDims );
      final NGrid ng = new NGrid( mbr, cellSides[0], numDims );
      if( !ng.isValid ) {
        System.out.println( "NGrid not properly generated!" );
      }

      int granularity = minNumGrids;
      if( numDims == 3 ) granularity = minNumGrids - 0;
      if( numDims == 4 ) granularity = minNumGrids - 0;
      if( numDims == 5 ) granularity = minNumGrids - 1;

      for( int i = 0; i < numDims; i++ ) {
        System.out.println( "ng.size[" + i + "]" + ng.size.get( i ) );
        this.cellSides[i] = ng.size.get( i ) / ( pow( 2, granularity ) );
      }
    }
    System.out.printf( "CellSide IN: %.15f %.15f %.15f %.15f%n",
                       this.cellSides[0],
                       this.cellSides[1],
                       this.cellSides[2],
                       this.cellSides[3] );
  }

  // ===========================================================================

  /*
   * Mapper class.
   */

  public static class OneGridMapper
    extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    // === Properties ==========================================================

    private Double[] cellSides = new Double[maxNumDims];
    private Integer[] contextPart;
    private List<Grid> grids = new ArrayList<Grid>( numGrids );
    private List<NGrid> nGrids = new ArrayList<NGrid>( numGrids );
    private List<HashMap<Long, Long>> boxCounts = new ArrayList<HashMap<Long, Long>>( numGrids );
    // configuration parameters
    private String algorithmType;
    private String mbr;
    private String inputType;
    private int numDims;
    // statistics
    private Double avgArea = 0.0;
    private Double avgX = 0.0;
    private Double avgY = 0.0;
    private Double avgVert = 0.0;
    private long geomCounter = 0L;

    // === Methods =============================================================

    /**
     * MISSING_COMMENT
     *
     * @param context
     */

    @Override
    protected void setup( Context context ) {
      // retrieve the configuration parameters
      final Configuration conf = context.getConfiguration();
      algorithmType = conf.get( algoTypeLabel );
      mbr = conf.get( mbrLabel );
      inputType = conf.get( inputTypeLabel );
      numDims = conf.getInt( numDimsLabel, 2 );
      for( int i = 0; i < numDims; i++ ) {
        cellSides[i] = conf.getDouble( cellSideLabel + i, 0.0 );
      }

      this.contextPart = new Integer[numDims];
      for( int i = 0; i < numDims; i++ ) {
        contextPart[i] = conf.getInt( contextPartLabel + i, -1 ); // TODO: default value
      }

      System.out.printf( "%s%s start ...%n", mapPhaseLabel, setupMethodLabel );
      System.out.printf( "%s%s cell sides: ", mapPhaseLabel, setupMethodLabel );
      for( int i = 0; i < numDims; i++ ) {
        System.out.printf( "%.15f ", this.cellSides[i] );
      }
      System.out.printf( "%n" );

      // build the grid using the cell side of the first dimension!!
      initializeGrids
        ( grids, nGrids, mbr, cellSides[0], numDims, boxCounts,
          inputType, algorithmType, mapPhaseLabel, setupMethodLabel );
    }


    /**
     * The map is responsible for updating a vector where any element represents
     * the box count of grid cell. For each cell of the grid, if the current
     * shape overlaps it, then the corresponding counter is incremented by one.
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    public void map( LongWritable key, Text value, Context context )
      throws IOException, InterruptedException {

      // --- take only context value ---------------------

      final String[] tokens = value.toString().trim().split("," );
      final int init = tokens.length / 2;

      final StringBuilder sbLeft = new StringBuilder();
      final StringBuilder sbRight = new StringBuilder();

      for( int num : contextPart) {
        sbLeft.append( tokens[num] ).append( "," );
        sbRight.append( tokens[init+num] ).append( "," );
      }

      sbRight.deleteCharAt( sbLeft.length()-1 );
      final String partValue = sbLeft.toString() + sbRight.toString();

      final Geometry shape;
      final NRectangle nrect;

      // --- parse the geometry and updates the statistics ---------------------

      if( inputType.equalsIgnoreCase( wktFormat ) ) {
        //shape = readWktGeometry( value.toString().trim() );
        shape = readWktGeometry( partValue );
        System.out.println(shape);
        nrect = null;
        geomCounter = geomCounter + 1;
        avgArea = ( avgArea * (double) ( geomCounter - 1 ) +
                    shape.getArea() )
                  / geomCounter;
        avgX = ( avgX * (double) ( geomCounter - 1 ) +
                 shape.getEnvelopeInternal().getWidth() )
               / geomCounter;
        avgY = ( avgY * (double) ( geomCounter - 1 ) +
                 shape.getEnvelopeInternal().getHeight() )
               / geomCounter;
        avgVert = ( avgVert * (double) ( geomCounter - 1 ) +
                    shape.getNumPoints() )
                  / geomCounter;

      } else {
        shape = null;

        //nrect = new NRectangle( value.toString().trim() );
        nrect = new NRectangle( partValue );

        if( nrect.isValid ) {
          geomCounter = geomCounter + 1;
          avgArea = ( avgArea * (double) ( geomCounter - 1 )
                      + nrect.getNVol() )
                    / geomCounter;
          avgX = ( avgX * (double) ( geomCounter - 1 )
                   + nrect.getSize( 0 ) )
                 / geomCounter;
          avgY = ( avgY * (double) ( geomCounter - 1 )
                   + nrect.getSize( 1 ) )
                 / geomCounter;
          avgVert = ( avgVert * (double) ( geomCounter - 1 ) +
                      pow( 2, nrect.getDim() ) )
                    / geomCounter;
        }
      }

      // -- check the cells that are intersected by the shape geometry ---------

      if( inputType.equalsIgnoreCase( wktFormat ) ) {
        if( shape == null ) {
          System.out.printf( "%s%s CASE WKT: shape is null.%n",
                             mapPhaseLabel, mapMethodLabel );
          return;
        }

        if( algoTypeLabel.equalsIgnoreCase( oneGridAlgorithm ) ) {
          updateBoxCounting( 0, shape, true );
        } else if( algoTypeLabel.equalsIgnoreCase( multiGridAlgorithm ) ) {
          for( int i = 0; i < numGrids; i++ ) {
            updateBoxCounting( i, shape, false );
          }
        }

      } else { // csv format
        if( nrect == null || !nrect.isValid ) {
          System.out.printf( "%s%s CASE WKT: rectangle %s is not valid!%n",
                             mapPhaseLabel, mapMethodLabel,
                             //value.toString() );
                             partValue );
          return;
        }

        if( algorithmType.equalsIgnoreCase( oneGridAlgorithm ) ) {
          updateBoxCounting( 0, nrect, true );
        } else if( algorithmType.equalsIgnoreCase( multiGridAlgorithm ) ) {
          for( int i = 0; i < numGrids; i++ ) {
            updateBoxCounting( i, nrect, false );
          }
        }
      }
    }

    /**
     * MISSING_COMMENT
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void cleanup( Context context )
      throws IOException, InterruptedException {

      System.out.printf( "%s%s: start...%n", mapPhaseLabel, cleanupMethodLabel );

      if( algorithmType.equalsIgnoreCase( oneGridAlgorithm ) ) {
        final Map<Long, Long> bc = boxCounts.get( 0 );
        for( Long k : bc.keySet() ) {
          context.write
            ( new LongWritable( k ), new LongWritable( bc.get( k ) ) );
        }
      } else if( algorithmType.equalsIgnoreCase( multiGridAlgorithm ) ) {
        for( int i = 0; i < numGrids; i++ ) {
          final Map<Long, Long> bc = boxCounts.get( i );
          for( Long k : bc.keySet() ) {
            context.write
              ( new LongWritable( k + gridId * i ),
                new LongWritable( bc.get( k ) ) );
          }
        }
      }

      // save a double as a long with the method doubleToLongBits
      System.out.printf( "%s%s avg area: %s for n (%d): %.15f.%n",
                         mapPhaseLabel, cleanupMethodLabel,
                         avgArea.toString(),
                         geomCounter,
                         ( avgArea * geomCounter ) );
      context.write
        ( new LongWritable( avgAreaIndex ),
          new LongWritable( doubleToLongBits( avgArea * geomCounter ) ) );
      context.write
        ( new LongWritable( avgXIndex ),
          new LongWritable( doubleToLongBits( avgX * geomCounter ) ) );
      context.write
        ( new LongWritable( avgYIndex ),
          new LongWritable( doubleToLongBits( avgY * geomCounter ) ) );
      context.write
        ( new LongWritable( avgVertIndex ),
          new LongWritable( doubleToLongBits( avgVert * geomCounter ) ) );
      context.write
        ( new LongWritable( geomCounterIndex ),
          new LongWritable( geomCounter ) );

      System.out.printf( "%s%s... end.%n", mapPhaseLabel, cleanupMethodLabel );
    }


    /**
     * MISSING_COMMENT
     *
     * @param text
     * @return
     */

    private Geometry readWktGeometry( String text ) {
      final WKTReader wktReader = new WKTReader();
      try {
        System.out.println(text);
        return wktReader.read( text );
      } catch( ParseException e ) {
        return null;
      }
    }

    /**
     * MISSING_COMMENT
     *
     * @param gridIndex
     * @param shape
     * @param single
     */

    private void updateBoxCounting
    ( int gridIndex,
      Geometry shape,
      boolean single ) {

      if( shape == null ) {
        throw new NullPointerException();
      }

      final Grid grid = grids.get( gridIndex );
      final Long[] intersectedCells = grid.overlapPartitions( shape );

      // update the box count
      final HashMap<Long, Long> bc = boxCounts.get( gridIndex );
      if( intersectedCells != null ) {
        for( int i = 0; i < intersectedCells.length; i++ )
          if( bc.get( intersectedCells[i] ) == null ) {
            bc.put( intersectedCells[i], 1L );
          } else {
            bc.put( intersectedCells[i], bc.get( intersectedCells[i] ) + 1L );
          }
      }

      if( single ) {
        if( bc.containsKey( cardIndex ) ) {
          bc.put( cardIndex, bc.get( cardIndex ) + 1L );
        } else {
          bc.put( cardIndex, 1L );
        }
      }
    }


    /**
     * MISSING_COMMENT
     *
     * @param gridIndex
     * @param nrect
     * @param single
     */

    private void updateBoxCounting
    ( int gridIndex,
      NRectangle nrect,
      boolean single ) {

      if( nrect == null ) {
        throw new NullPointerException();
      }

      final NGrid ng = nGrids.get( gridIndex );
      final Long[] intersectedCells = ng.overlapPartitions( nrect );

      // update the box count
      final HashMap<Long, Long> bc = boxCounts.get( gridIndex );
      if( intersectedCells != null ) {
        for( int i = 0; i < intersectedCells.length; i++ ) {
          if( bc.get( intersectedCells[i] ) == null ) {
            bc.put( intersectedCells[i], 1L );
          } else {
            bc.put( intersectedCells[i], bc.get( intersectedCells[i] ) + 1L );
          }
        }
      }

      if( single ) {
        if( bc.containsKey( cardIndex ) ) {
          bc.put( cardIndex, bc.get( cardIndex ) + 1L );
        } else {
          bc.put( cardIndex, 1L );
        }
      }
    }
  }


  // ===========================================================================

  /*
   * MISSING_COMMENT
   */

  public static class OneGridReducer
    extends Reducer<LongWritable, LongWritable, Text, Text> {

    // === Properties ==========================================================

    private static final long maxIterations = 5000000L;

    private List<Grid> grids = new ArrayList<Grid>( numGrids );
    private List<NGrid> nGrids = new ArrayList<NGrid>( numGrids );
    private List<HashMap<Long, Long>> boxCounts = new ArrayList<HashMap<Long, Long>>( numGrids );

    private Long reduceStart;
    private Long reduceEnd;

    // statistics
    private Double avgArea = 0.0;
    private Double avgX = 0.0, avgY = 0.0;
    private Double avgVert = 0.0;
    private long geomCounter = 0L;


    // configuration parameters
    private String algorithmType;
    private String inputType;
    private String mbr;
    private String inputFile;
    private Double splitSize;
    private Double fileSize;
    private boolean computeMoranIndex;
    private int numDims;
    private Double[] cellSides = new Double[maxNumDims];

    // === Methods =============================================================

    /**
     * MISSING_COMMENT
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    protected void setup( Context context )
      throws IOException, InterruptedException {

      // retrieve the configuration parameters
      final Configuration conf = context.getConfiguration();
      algorithmType = conf.get( algoTypeLabel );
      inputType = conf.get( inputTypeLabel );
      mbr = conf.get( mbrLabel );
      inputFile = conf.get( inputFileLabel );
      fileSize = conf.getDouble( dimFileLabel, 0 );
      splitSize = conf.getDouble( dimSplitLabel, defaultSplitSize );
      computeMoranIndex = conf.getBoolean( moranIndexLabel, false );
      numDims = conf.getInt( numDimsLabel, 2 );
      for( int i = 0; i < numDims; i++ ) {
        cellSides[i] = conf.getDouble( cellSideLabel + i, 0.0 );
      }

      reduceStart = System.currentTimeMillis();
      System.out.printf( "%s%s start...%n", reducePhaseLabel, setupMethodLabel );
      System.out.printf( "%s%s cellSide: ", reducePhaseLabel, setupMethodLabel );
      for( int i = 0; i < numDims; i++ ) {
        System.out.print( this.cellSides[i] + " " );
      }
      System.out.printf( "%n" );

      initializeGrids
        ( grids, nGrids,
          mbr, cellSides[0], numDims, boxCounts,
          inputType, algorithmType,
          reducePhaseLabel, setupMethodLabel );
    }

    /**
     * The unique reducer has to sum up for each key the corresponding values.
     * The produced result is a new vector where each element corresponds to a
     * cell and contains the number of overlapping geometries.
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    public void reduce( LongWritable key, Iterable<LongWritable> values, Context context )
      throws IOException, InterruptedException {

      // each reducer receive a pair composed by a key and list of values for
      // the same key
      long count = 0L;
      if( ( key.get() >= cardIndex ) || ( key.get() == geomCounterIndex ) ) {
        // sum up the values with the same key
        for( LongWritable v : values ) {
          count += v.get();
        }
        if( key.get() == geomCounterIndex ) {
          geomCounter = count;
        }
      } else {
        // sum up the values with the same key, after converting back the
        // long value into a double
        double countd = 0.0;
        for( LongWritable v : values ) {
          countd += longBitsToDouble( v.get() );
        }

        if( key.get() == avgVertIndex ) {
          avgVert = countd;
        } else if( key.get() == avgYIndex ) {
          avgY = countd;
        } else if( key.get() == avgXIndex ) {
          avgX = countd;
        } else if( key.get() == avgAreaIndex ) {
          avgArea = countd;
        }
      }

      if( key.get() >= cardIndex ) {
        Long k, c;
        if( algorithmType.equalsIgnoreCase( oneGridAlgorithm ) ) {
          k = new Long( key.get() );
          c = new Long( count );
          final HashMap<Long, Long> bc = boxCounts.get( 0 );
          bc.put( k, c );

        } else if( algorithmType.equalsIgnoreCase( multiGridAlgorithm ) ) {
          k = new Long( key.get() );
          c = new Long( count );
          final HashMap<Long, Long> bc = boxCounts.get( (int) ( k / gridId ) );
          bc.put( k % gridId, c );
        }
      }
    }

    /**
     * MISSING_COMMENT
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup( Context context )
      throws IOException, InterruptedException {

      System.out.printf( "%s%s: start...%n", reduceMethodLabel, cleanupMethodLabel );
      System.out.printf( "%s%s cellSide: ", reduceMethodLabel, cleanupMethodLabel );
      for( int i = 0; i < numDims; i++ ) {
        System.out.print( this.cellSides[i] + " " );
      }
      System.out.printf( "%n" );

      // -----------------------------------------------------------------------
      final Map<Double, Double> d0Map;
      final Map<Double, Double> d2Map;
      final Map<Double, Double> d3Map;
      final Map<Double, Double> m0Map;
      final Map<Double, Double> m1Map;
      final Map<Double, Double> m2Map;
      final Map<Double, Double> ncellMap;

      final List<Map<Double, Double>> d0MapDims;
      final List<Map<Double, Double>> d2MapDims;
      final List<Map<Double, Double>> d3MapDims;
      final List<Map<Double, Double>> ncellMapDims;
      // -----------------------------------------------------------------------


      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {
        // build the list of values for computing the linear regression and
        // then obtaining the slope D0, D2 and D3.
        d0Map = new HashMap<>();
        d2Map = new HashMap<>();
        d3Map = new HashMap<>();
        if( computeMoranIndex ) {
          m0Map = new HashMap<>();
          m2Map = new HashMap<>();
          m1Map = new HashMap<>();
        } else {
          m0Map = null;
          m2Map = null;
          m1Map = null;
        }
        ncellMap = new HashMap<>();

        // not used variables
        d0MapDims = null;
        d2MapDims = null;
        d3MapDims = null;
        ncellMapDims = null;

      } else { // multi dimensional case
        d0MapDims = new ArrayList<>();
        d2MapDims = new ArrayList<>();
        d3MapDims = new ArrayList<>();
        ncellMapDims = new ArrayList<>();

        for( int i = 0; i < numDims; i++ ) {
          d0MapDims.add( new HashMap<>() );
          d2MapDims.add( new HashMap<>() );
          d3MapDims.add( new HashMap<>() );
          ncellMapDims.add( new HashMap<>() );
        }

        // not used variables
        d0Map = null;
        d2Map = null;
        d3Map = null;
        m0Map = null;
        m1Map = null;
        m2Map = null;
        ncellMap = null;
      }

      // the first grid is equal for both methods oneGrid and multipleGrid
      final Grid grid0;
      final NGrid ngrid0;
      if( inputType.equalsIgnoreCase( wktFormat ) ) {
        grid0 = grids.get( 0 );
        ngrid0 = null;
      } else {
        ngrid0 = nGrids.get( 0 );
        grid0 = null;
      }

      final Map<Long, Long> boxCountG0 = boxCounts.get( 0 );
      final long card = boxCountG0.get( cardIndex );
      boxCountG0.remove( cardIndex );

      double numTiles;
      if( inputType.equalsIgnoreCase( wktFormat ) ) {
        numTiles = grid0.numTiles;
      } else {
        //numTiles = ngrid0.numTiles;
        // 2020-06-23: we do not consider the number of tiles, but the number
        // o cells for each dimension!!!
        numTiles = ngrid0.numCell;
      }

      // -----------------------------------------------------------------------

      // variables for the computation with cases WKT and CSV
      double s0 = 0.0;
      double s1 = 0.0;
      double s2 = 0.0;
      double s3 = 0.0;

      double maxCount = 0.0, minCount = 0.0;

      // variables for the computation with case CSVmulti
      final List<Map<Long, Long>> bcDims = new ArrayList<>();
      for( int i = 0; i < numDims; i++ ) {
        bcDims.add( new HashMap<>() );
      }

      final double[] s0Dims = new double[maxNumDims];
      final double[] s1Dims = new double[maxNumDims];
      final double[] s2Dims = new double[maxNumDims];
      final double[] s3Dims = new double[maxNumDims];

      // === box count for the first grid ======================================

      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {

        final double[] s = combineBoxCountingMono( boxCountG0 );
        s0 = s[0];
        s1 = s[1];
        s2 = s[2];
        s3 = s[3];

      } else {
        combineBoxCountingMulti
          ( ngrid0, boxCountG0, numDims, bcDims,
            s0Dims, s1Dims, s2Dims, s3Dims, 0 );
      }

      // the values of cellSize have to be normalized w.r.t. the grid by
      // dividing the cellSize for grid width
      double width;
      if( inputType.equalsIgnoreCase( wktFormat ) ) {
        width = grid0.width;
      } else {
        // warning: we use the width of the grid on the first dimension
        // (x coordiante or first coordinate), even if the grid could not be
        // a nCube!!
        width = ngrid0.size.get( 0 );
      }

      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {
        // we introduce rectangular cells: for the computation of the
        // r parameter, we use the width
        d0Map.put( log( cellSides[0] / width ), log( s0 ) );
        d2Map.put( log( cellSides[0] / width ), log( s2 ) );
        d3Map.put( log( cellSides[0] / width ), log( s3 ) );
        ncellMap.put( log( cellSides[0] / width ), ( numTiles - s0 ) / numTiles );

			  /* skip the first grid, because the computation is too expensive
			  M0.put((cellSize/gr.width), MI_val0);
			  M2.put((cellSize/gr.width), MI_val2);
			  M3.put((cellSize/gr.width), MI_val3);
			  */
      } else {
        for( int i = 0; i < numDims; i++ ) {
          ( d0MapDims.get( i ) ).put
            ( log( cellSides[0] / width ), log( s0Dims[i] ) );
          System.out.println( "D0_dim[" + i + "] = " + d0MapDims.get( i ) );

          ( d2MapDims.get( i ) ).put
            ( log( cellSides[0] / width ), log( s2Dims[i] ) );
          System.out.println( "D2_dim[" + i + "] = " + d2MapDims.get( i ) );

          ( d3MapDims.get( i ) ).put
            ( log( cellSides[0] / width ), log( s3Dims[i] ) );
          System.out.println( "D3_dim[" + i + "] = " + d3MapDims.get( i ) );

          // 2020-06-15 update ncellMapDims
          ncellMapDims.get( i ).put
            ( log( cellSides[0] / width ),
              ( numTiles - s0Dims[i] ) / numTiles );
        }
      }

      // --- next values in D0, D2 and D3 --------------------------------------
      if( algorithmType.equalsIgnoreCase( oneGridAlgorithm ) ) {
        final Map<Long, Long> boxCountPrev = new HashMap<>();
        final Map<Long, Long> boxCountNew = new HashMap<>();
        for( Long cellKey : boxCountG0.keySet() ) {
          boxCountNew.put( cellKey, boxCountG0.get( cellKey ) );
        }

        double[] currentCellSide = new double[maxNumDims];
        for( int i = 0; i < numDims; i++ ) {
          currentCellSide[i] = cellSides[i];
          System.out.printf( "cs[%d] = %.15f%n", i, currentCellSide[i] );
        }
        System.out.printf( "width = %.15f%n%n", width );

        NGrid ngridPrev, ngridNew = ngrid0;
        Grid gridPrev, gridNew = grid0;
        Long cellKeyNew;
        int numgrid = 2;

        while( currentCellSide[0] * 2 < width ) {
          gridPrev = gridNew;
          ngridPrev = ngridNew;
          boxCountPrev.clear();

          for( Long bcKey : boxCountNew.keySet() ) {
            boxCountPrev.put( bcKey, boxCountNew.get( bcKey ) );
          }
          boxCountNew.clear();

          // todo: introduce rectangular cells!!
          // cellWidth = cellWidth*2;
          // cellHeight = cellHeight*2;
          for( int i = 0; i < numDims; i++ ) {
            currentCellSide[i] = currentCellSide[i] * 2;
          }

          if( inputType.equalsIgnoreCase( wktFormat ) ) {
            // the constructor already considers rectangular cells
            gridNew = new Grid( mbr, currentCellSide[0] );
          } else {
            ngridNew = new NGrid( mbr, currentCellSide[0], numDims );
          }
          System.out.printf( "NewGrid %d [%.15f, %.15f, ...]%n",
                             numgrid, currentCellSide[0], currentCellSide[1] );

          for( Long k : boxCountPrev.keySet() ) {
            if( inputType.equalsIgnoreCase( wktFormat ) ) {
              cellKeyNew = gridNew.getCellId
                ( gridPrev.getRow( k ) / 2 + gridPrev.getRow( k ) % 2,
                  gridPrev.getCol( k ) / 2 + gridPrev.getCol( k ) % 2 );

            } else { // csvFormat
              final List<Long> cell = ngridPrev.getCellCoords( k );
              // determine in which cell of the new grid the current cell fall
              for( int i = 0; i < numDims; i++ ) {
                cell.set( i, cell.get( i ) / 2 + cell.get( i ) % 2 );
              }
              cellKeyNew = ngridNew.getCellNumber( cell );
            }

            if( boxCountNew.containsKey( cellKeyNew ) ) {
              boxCountNew.put( cellKeyNew, boxCountNew.get( cellKeyNew ) + boxCountPrev.get( k ) );
            } else {
              boxCountNew.put( cellKeyNew, boxCountPrev.get( k ) );
            }
          }

          if( inputType.equalsIgnoreCase( wktFormat ) ||
              inputType.equalsIgnoreCase( csvFormat ) ) {

            System.out.printf( "NewGrid %d --> S0[%.15f, %.15f, ...] = %.15f%n",
                               numgrid,
                               currentCellSide[0], currentCellSide[1],
                               s0 );

            if( inputType.equalsIgnoreCase( wktFormat ) ) {
              numTiles = gridNew.numTiles;
            } else {
              // numTiles = ngridNew.numTiles;
              numTiles = ngridNew.numCell;
            }

            final double[] s = combineBoxCountingMono( boxCountNew );
            s0 = s[0];
            s1 = s[1];
            s2 = s[2];
            s3 = s[3];

            maxCount = MIN_VALUE;
            minCount = MAX_VALUE;
            for( Long k : boxCountNew.keySet() ) {
              minCount = ( minCount > boxCountNew.get( k ) ? boxCountNew.get( k ) : minCount );
              maxCount = ( maxCount < boxCountNew.get( k ) ? boxCountNew.get( k ) : maxCount );
            }

          } else { // csvmulti
            final List<Map<Long, Long>> bcDimsNew = new ArrayList<>();
            for( int i = 0; i < numDims; i++ ) {
              bcDimsNew.add( new HashMap<>() );
            }
            combineBoxCountingMulti
              ( ngridNew, boxCountNew, numDims, bcDimsNew,
                s0Dims, s1Dims, s2Dims, s3Dims, numgrid );
          }

          // ===================================================================

          // computation of the Moran's index
          if( computeMoranIndex ) {
            computeMoranIndex
              ( inputType, maxCount, minCount,
                gridNew, ngridNew,
                boxCountNew, numTiles, numDims,
                s0, s1, s2, s3, card, 0L, 0L,
                currentCellSide[0], m0Map, m1Map, m2Map );
          }


          if( inputType.equalsIgnoreCase( wktFormat ) ||
              inputType.equalsIgnoreCase( csvFormat ) ) {
            d0Map.put( log( currentCellSide[0] / width ), log( s0 ) );
            d2Map.put( log( currentCellSide[0] / width ), log( s2 ) );
            d3Map.put( log( currentCellSide[0] / width ), log( s3 ) );
            ncellMap.put( log( currentCellSide[0] / width ), ( numTiles - s0 ) / numTiles );
          } else {
            for( int i = 0; i < numDims; i++ ) {
              // todo: mods from cs[0] to cs[i]
              // width = ngrid0.size.get( i );
              ( d0MapDims.get( i ) ).put( log( currentCellSide[0] / width ), log( s0Dims[i] ) );
              ( d2MapDims.get( i ) ).put( log( currentCellSide[0] / width ), log( s2Dims[i] ) );
              ( d3MapDims.get( i ) ).put( log( currentCellSide[0] / width ), log( s3Dims[i] ) );

              // 2020-06-15 update ncellMapDims
              ncellMapDims.get( i ).put
                ( log( currentCellSide[0] / width ),
                  ( numTiles - s0Dims[i] ) / numTiles );
            }
          }
          numgrid = numgrid + 1;
        }

      } else if( algorithmType.equalsIgnoreCase( multiGridAlgorithm ) ) {
        // TODO introdurre le classi anche qui
        for( int i = 1; i < numGrids; i++ ) {
          final Grid gridi;
          final NGrid nGridi;

          if( inputType.equalsIgnoreCase( wktFormat ) ) {
            gridi = grids.get( i );
            nGridi = null;

          } else {
            gridi = null;
            nGridi = nGrids.get( i );
          }

          final Map<Long, Long> boxCountGi = boxCounts.get( i );

          final double[] currentCellSide = new double[maxNumDims];
          if( inputType.equalsIgnoreCase( wktFormat ) ) {
            currentCellSide[0] = grid0.tileWidth;
            currentCellSide[1] = grid0.tileHeight;
          } else {
            for( int j = 0; j < numDims; j++ ) {
              currentCellSide[i] = ngrid0.size.get( i );
            }
          }

          final double[] s = combineBoxCountingMono( boxCountGi );
          s0 = s[0];
          s2 = s[1];
          s3 = s[2];
          s1 = s[3];

          // compute MI
          if( inputType.equalsIgnoreCase( wktFormat ) ) {
            numTiles = grid0.numTiles;
          } else {
            //numTiles = ngrid0.numTiles;
            numTiles = ngrid0.numCell;
          }

          final double avg0 = s0 / numTiles;
          final double avg1 = s1 / numTiles;
          final double avg2 = s2 / numTiles;

          System.out.printf( "%s%s: avg-> %.15f for N->%.15f.%n", avg0, numTiles );

          if( inputType.equalsIgnoreCase( wktFormat ) ) {
            width = gridi.width;
          } else {
            // attenzione uso la larghezza della griglia sulla prima dimensione (coordinata x)
            // anche se la griglia potrebbe non essere un nCubo: in realtà è quasi sempre un nCubo
            width = nGridi.size.get( 0 );
          }

          if( computeMoranIndex ) {
            computeMoranIndex
              ( inputType, maxCount, minCount,
                gridi, nGridi, boxCountGi, numTiles,
                numDims, s0, s1, s2, s3, card, 1L, 1L,
                currentCellSide[0], m0Map, m1Map, m2Map );
          }


          if( inputType.equalsIgnoreCase( wktFormat ) ||
              inputType.equalsIgnoreCase( csvFormat ) ) {
            d0Map.put( log( currentCellSide[0] / width ), log( s0 ) );
            d2Map.put( log( currentCellSide[0] / width ), log( s2 ) );
            d3Map.put( log( currentCellSide[0] / width ), log( s3 ) );
            ncellMap.put( log( currentCellSide[0] / width ), ( numTiles - s0 ) / numTiles );
          } else {
            for( int j = 0; j < numDims; j++ ) {
              // todo: mods from cs[0] to cs[i]
              // width = ngrid0.size.get( i );
              ( d0MapDims.get( j ) ).put( log( currentCellSide[0] / width ), log( s0Dims[j] ) );
              ( d2MapDims.get( j ) ).put( log( currentCellSide[0] / width ), log( s2Dims[j] ) );
              ( d3MapDims.get( j ) ).put( log( currentCellSide[0] / width ), log( s3Dims[j] ) );

              // 2020-06-15 update ncellMapDims
              ncellMapDims.get( j ).put
                ( log( currentCellSide[0] / width ),
                  ( numTiles - s0Dims[j] ) / numTiles );
            }
          }
        }
      }

      // === compute regression for D0, D2 e D3 ================================

      final Double d01;
      final Double d0Supp1;
      final Double d02;
      final Double d0Supp2;
      final Double d21;
      final Double d2Supp1;
      final Double d22;
      final Double d2Supp2;
      final Double d31;
      final Double d3Supp1;
      final Double d32;
      final Double d3Supp2;

      final Double[] d01Dim = new Double[10];
      final Double[] d0Supp1Dim = new Double[10];
      final Double[] d02Dim = new Double[10];
      final Double[] d0Supp2Dim = new Double[10];
      final Double[] d21Dim = new Double[10];
      final Double[] d2Supp1Dim = new Double[10];
      final Double[] d22Dim = new Double[10];
      final Double[] d2Supp2Dim = new Double[10];
      final Double[] d31Dim = new Double[10];
      final Double[] d3Supp1Dim = new Double[10];
      final Double[] d32Dim = new Double[10];
      final Double[] d3Supp2Dim = new Double[10];


      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {

        final List<List<Double>> d0Final =
          computeRegression( d0Map, true, "D0" );
        final Double[][] d0Array =
          valuesWithGreatestSupport( "D0", d0Final );
        d01 = d0Array[0][0];
        d0Supp1 = d0Array[0][1];
        d02 = d0Array[1][0];
        d0Supp2 = d0Array[1][1];

        final List<List<Double>> d2Final =
          computeRegression( d2Map, false, "D2" );
        final Double[][] d2Array =
          valuesWithGreatestSupport( "D2", d2Final );
        d21 = d2Array[0][0];
        d2Supp1 = d2Array[0][1];
        d22 = d2Array[1][0];
        d2Supp2 = d2Array[1][1];

        final List<List<Double>> d3Final =
          computeRegression( d3Map, false, "D3" );
        final Double[][] d3Array =
          valuesWithGreatestSupport( "D3", d3Final );
        d31 = d3Array[0][0];
        d3Supp1 = d3Array[0][1];
        d32 = d3Array[1][0];
        d3Supp2 = d3Array[1][1];

      } else { // CSVmulti
        final List<List<List<Double>>> d0FinalDim = new ArrayList<>();

        for( int i = 0; i < numDims; i++ ) {
          d0FinalDim.add
            ( computeRegression( d0MapDims.get( i ), true, "D0_" + i ) );
          final Double[][] d0Array =
            valuesWithGreatestSupport( "D0_" + i, d0FinalDim.get( i ) );
          d01Dim[i] = d0Array[0][0];
          d0Supp1Dim[i] = d0Array[0][1];
          d02Dim[i] = d0Array[1][0];
          d0Supp2Dim[i] = d0Array[1][1];
        }

        final List<List<List<Double>>> d2FinalDim = new ArrayList<>();

        for( int i = 0; i < numDims; i++ ) {
          d2FinalDim.add
            ( computeRegression( d2MapDims.get( i ), false, "D2_" + i ) );
          final Double[][] d2Array =
            valuesWithGreatestSupport( "D2_" + i, d2FinalDim.get( i ) );
          d21Dim[i] = d2Array[0][0];
          d2Supp1Dim[i] = d2Array[0][1];
          d22Dim[i] = d2Array[1][0];
          d2Supp2Dim[i] = d2Array[1][1];
        }

        final List<List<List<Double>>> d3FinalDim = new ArrayList<>();
        for( int i = 0; i < numDims; i++ ) {
          d3FinalDim.add
            ( computeRegression( d3MapDims.get( i ), false, "D3_" + i ) );
          final Double[][] d3Array =
            valuesWithGreatestSupport( "D3_" + i, d2FinalDim.get( i ) );
          d31Dim[i] = d3Array[0][0];
          d3Supp1Dim[i] = d3Array[0][1];
          d32Dim[i] = d3Array[1][0];
          d3Supp2Dim[i] = d3Array[1][1];
        }

        d01 = null;
        d02 = null;
        d21 = null;
        d22 = null;
        d31 = null;
        d32 = null;
        d0Supp1 = null;
        d0Supp2 = null;
        d2Supp1 = null;
        d2Supp2 = null;
        d3Supp1 = null;
        d3Supp2 = null;
      }

      // === Moran's Index =====================================================

      int i;
      final int margin = 4;
      Double sum0 = 0.0, sum2 = 0.0, sum1 = 0.0,
        min0 = 1.0, max0 = -1.0,
        min2 = 1.0, max2 = -1.0,
        min1 = 1.0, max1 = -1.0;

      if( computeMoranIndex ) {
        ArrayList<Double> kl0 = null;
        ArrayList<Double> kl2 = null;
        ArrayList<Double> kl1 = null;
        Double val;

        if( inputType.equalsIgnoreCase( wktFormat ) ||
            inputType.equalsIgnoreCase( csvFormat ) ) {
          System.out.println( "Moran's I ->> = " );

          kl0 = new ArrayList<>( m0Map.keySet() );
          kl1 = new ArrayList<>( m1Map.keySet() );
          kl2 = new ArrayList<>( m2Map.keySet() );
          Collections.sort( kl0 );
          Collections.sort( kl2 );
          Collections.sort( kl1 );
          i = 0;
          while( i < kl0.size() - margin ) {
            val = m0Map.get( kl0.get( i ) );
            sum0 += val;
            min0 = ( val < min0 ? val : min0 );
            max0 = ( val > max0 ? val : max0 );
            System.out.println( "M0 Value" + i + "[" + kl0.get( i++ ) + "] =" + val );
          }
          sum0 = sum0 / ( kl0.size() - margin );
          i = 0;

          while( i < kl2.size() - margin ) {
            val = m2Map.get( kl2.get( i ) );
            sum2 += val;
            min2 = ( val < min2 ? val : min2 );
            max2 = ( val > max2 ? val : max2 );
            System.out.println( "M2 Value" + i + "[" + kl2.get( i++ ) + "] =" + val );
          }
          sum2 = sum2 / ( kl2.size() - margin );
          i = 0;

          while( i < kl1.size() - margin ) {
            val = m1Map.get( kl1.get( i ) );
            sum1 += val;
            min1 = ( val < min1 ? val : min1 );
            max1 = ( val > max1 ? val : max1 );
            System.out.println( "M1 Value" + i + "[" + kl1.get( i++ ) + "] =" + val );
          }
          sum1 = sum1 / ( kl1.size() - margin );
        }
      } // fine if calcolo Moran's index

      // === computation of the percentage of empty cells ======================

      Double avgEmpty = 0.0;
      Double[] avgEmptyDims = new Double[numDims];

      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {
        avgEmpty = computeEmptyCells( ncellMap, card );

      } else {
        for( int d = 0; d < numDims; d++ ) {
          System.out.printf( "Dimension: %d%n", d );
          avgEmptyDims[d] = computeEmptyCells( ncellMapDims.get( d ), card );
          System.out.printf( "%n" );
        }
      }

      // =======================================================================


      final List<ArrayList<Double>> partitions = new ArrayList<>();
      final String[] partitionType = new String[10];
      if( inputType.equalsIgnoreCase( csvMultiFormat ) ) {
        // choose the best partitioning based on the fractal dimension
        // use the D0 and D2 with the gratest support or the average between
        // the greatest two:
        // D0 < 0.25 -> D2 < 0.5 -> quadtree else rtree
        // 0.25 >= D0 -> D2 >= 0.75 -> regular grid else D2 < 0.5 -> quadtree else rtree
        final Double[] d0 = new Double[10];
        final Double[] d2 = new Double[10];
        for( i = 0; i < numDims; i++ ) {
          if( d0Supp1Dim[i] > d0Supp2Dim[i] ) {
            d0[i] = d01Dim[i];
          } else {
            if( d0Supp1Dim[i] < d0Supp2Dim[i] ) {
              d0[i] = d02Dim[i];
            } else {
              d0[i] = ( d01Dim[i] + ( d02Dim[i] >= 0.9 ? 1 : d02Dim[i] ) ) / 2;
            }
          }

          if( d2Supp1Dim[i] > d2Supp2Dim[i] ) {
            d2[i] = d21Dim[i];
          } else {
            if( d2Supp1Dim[i] < d2Supp2Dim[i] ) {
              d2[i] = d22Dim[i];
            } else {
              d2[i] = ( d21Dim[i] + ( d22Dim[i] >= 0.9 ? 1 : d22Dim[i] ) ) / 2;
            }
          }
        }

        // compute the number of subdivisions
        //final Double fileSize = conf.getDouble
        //  ( dimFileLabel, 72142150.0 );
        //final Double splitSize = conf.getDouble
        //  ( dimSplitLabel, 1024.0 * 1024.0 * 128 );

        final long totNumSplits = (long) ceil( fileSize / splitSize );
        final long numDivs = (long) ceil( pow( (double) totNumSplits,
                                               (double) ( 1.0 / numDims ) ) );
        System.out.println( "Number of subdivisions: " + numDivs );

        for( i = 0; i < numDims; i++ ) {
          final Map<Long, Long> currentBc = bcDims.get( i );
          final double orig = ngrid0.orig.get( i );
          final double size = ngrid0.size.get( i );

          if( d0[i] < 0.25 ) {
            if( d2[i] < 0.5 ) {
              partitionType[i] = "QuadTree";
              System.out.printf( "Call to Quadtree[%d]: "
                                 + "orig %.15f, "
                                 + "size %.15f, "
                                 + "tileSide %.15f, "
                                 + "threshold %.15f.%n",
                                 i, orig, size,
                                 ngrid0.tileSide.get( i ),
                                 ceil( card / numDivs ) );
              partitions.add( i,
                              QuadTree( currentBc, orig, orig, size,
                                        ngrid0.tileSide.get( i ),
                                        (long) ceil( card / numDivs ) ) );
            } else {
              partitionType[i] = "RTree";
              System.out.printf( "Call to Rtree[%d]: "
                                 + "orig %.15f, "
                                 + "size %.15f, "
                                 + "tileSide %.15f, "
                                 + "threshold %.15f.%n",
                                 i, orig, size,
                                 ngrid0.tileSide.get( i ),
                                 ceil( card / numDivs ) );
              partitions.add( i,
                              RTree( currentBc, orig, orig, size,
                                     ngrid0.tileSide.get( i ),
                                     (long) ceil( card / numDivs ) ) );
            }
          } else {
            if( d2[i] >= 0.75 ) {
              // [2020-06-23] mod for improving balancing!
              if( avgEmptyDims[i] <= 0.5 ) {
                partitionType[i] = "RegularGrid";
                System.out.printf( "Call to RegularGrid[%d]:"
                                   + "orig %.15f,"
                                   + " size %.15f,"
                                   + " threshold %.15f.%n",
                                   i, orig, size, ceil( card / numDivs ) );
                partitions.add( i, RegGrid( orig, size, numDivs ) );
              } else {
                partitionType[i] = "RTree";
                System.out.printf( "Call to Rtree[%d]:"
                                   + "orig %.15f,"
                                   + "size %.15f,"
                                   + "tileSide %.15f,"
                                   + "threshold %.15f.%n",
                                   i, orig, size,
                                   ngrid0.tileSide.get( i ), ceil( card / numDivs ) );
                partitions.add( i,
                                RTree( currentBc, orig, orig, size,
                                       ngrid0.tileSide.get( i ),
                                       (long) ceil( card / numDivs ) ) );
              }
            } else if( d2[i] < 0.5 ) {
              partitionType[i] = "QuadTree";
              System.out.printf( "Call to Quadtree[%d]: "
                                 + "orig %.15f, "
                                 + "size %.15f, "
                                 + "tileSide %.15f,"
                                 + "threshold %.15f.%n",
                                 i, orig, size,
                                 ngrid0.tileSide.get( i ), ceil( card / numDivs ) );
              partitions.add( i,
                              QuadTree( currentBc, orig, orig, size,
                                        ngrid0.tileSide.get( i ),
                                        (long) ceil( card / numDivs ) ) );
            } else {
              partitionType[i] = "RTree";
              System.out.printf( "Call to Rtree[%d]:"
                                 + "orig %.15f,"
                                 + "size %.15f,"
                                 + "tileSide %.15f,"
                                 + "threshold %.15f.%n",
                                 i, orig, size,
                                 ngrid0.tileSide.get( i ), ceil( card / numDivs ) );
              partitions.add( i,
                              RTree( currentBc, orig, orig, size,
                                     ngrid0.tileSide.get( i ),
                                     (long) ceil( card / numDivs ) ) );
            }
          }
        }
      }

      // Generazione OUTPUT: <datasetName> <FD0_1> <FD0_2> <FD2_1> <FD2_2> <FD3_1> <FD3_2> <MoranI0min> <MoranI0avg> <MoranI0max> <MoranI2min> <MoranI2avg> <MoranI2max> <MoranI3min> <MoranI3avg> <MoranI3max>
      final Text p1 = new Text();
      p1.set( inputFile.substring( inputFile.indexOf( "/" ) + 1 ) );
      final Text p2 = new Text();

      reduceEnd = System.currentTimeMillis();

      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {
        final StringBuilder s = new StringBuilder();
        s.append( "Card: " + card +
                  "\narea_AVG: " + avgArea / geomCounter +
                  " x_AVG: " + avgX / geomCounter +
                  " y_AVG: " + avgY / geomCounter +
                  " vertAVG: " + avgVert / geomCounter +
                  "\nDF (D0_1, D0_2, D2_1, D2_2, D3_1, D3_2): " +
                  format( "%.3f", d01 ) + " " +
                  format( "%.3f", d02 ) + " " +
                  format( "%.3f", d21 ) + " " +
                  format( "%.3f", d22 ) + " " +
                  format( "%.3f", d31 ) + " " +
                  format( "%.3f", d32 ) +
                  "\nSUPP (D0_1, D0_2, D2_1, D2_2, D3_1, D3_2): " +
                  format( "%.3f", d0Supp1 ) + " " +
                  format( "%.3f", d0Supp2 ) + " " +
                  format( "%.3f", d2Supp1 ) + " " +
                  format( "%.3f", d2Supp2 ) + " " +
                  format( "%.3f", d3Supp1 ) + " " +
                  format( "%.3f", d3Supp2 ) +
                  "\nPercCelleVuote: " + format( "%.3f", avgEmpty ) );

        if( computeMoranIndex )
          s.append( "\nMI (MI0_min, MI0_avg, MI0_max, ...):" +
                    format( "%.3f", min0 ) + " " +
                    format( "%.3f", sum0 ) + " " +
                    format( "%.3f", max0 ) + " " +
                    format( "%.3f", min1 ) + " " +
                    format( "%.3f", sum1 ) + " " +
                    format( "%.3f", max1 ) + " " +
                    format( "%.3f", min2 ) + " " +
                    format( "%.3f", sum2 ) + " " +
                    format( "%.3f", max2 ) );
        p2.set( s.toString() );


      } else { // CSV mult
        final StringBuilder s = new StringBuilder();
        for( i = 0; i < numDims; i++ ) {
          s.append( "Dim[" + i + "]:" );
          s.append( " D0_1: " +
                    format( "%.3f", d01Dim[i] ) + " (" +
                    format( "%.3f", d0Supp1Dim[i] ) + ")" );
          s.append( " D0_2: " +
                    format( "%.3f", d02Dim[i] ) + " (" +
                    format( "%.3f", d0Supp2Dim[i] ) + ")" );
          s.append( " D2_1: " +
                    format( "%.3f", d21Dim[i] ) + " (" +
                    format( "%.3f", d2Supp1Dim[i] ) + ")" );
          s.append( " D2_2: " +
                    format( "%.3f", d22Dim[i] ) + " (" +
                    format( "%.3f", d2Supp2Dim[i] ) + ")\n" );
        }

        for( i = 0; i < numDims; i++ ) {
          s.append( "Partition[" + i + "]: " + partitionType[i] + " (" );
          for( int j = 0; j < partitions.get( i ).size(); j++ ) {
            s.append( j == 0 ? format( "%.15f", partitions.get( i ).get( j ) ) :
              "-" + format( "%.15f", partitions.get( i ).get( j ) ) );
          }
          s.append( ")\n" );
        }
        p2.set( "CARD: " + card + "\n" +
                "PercCelleVuote: " + format( "%.3f", avgEmpty ) + "\n" +
                s );
      }
      // write the file
      context.write( p1, p2 );

      System.out.printf( "%s%s: end%n", reducePhaseLabel, cleanupMethodLabel );
    }

    /**
     * MISSING_COMMENT
     *
     * @param boxCount
     * @return
     */

    protected double[] combineBoxCountingMono
    ( Map<Long, Long> boxCount ) {

      if( boxCount == null ) {
        throw new NullPointerException();
      }

      final double[] s = new double[4];
      s[0] = boxCount.size(); // number of cells
      s[1] = 0.0;
      s[2] = 0.0;
      s[3] = 0.0;

      for( Long k : boxCount.keySet() ) {
        s[1] += boxCount.get( k );
        s[2] += pow( boxCount.get( k ), 2 );
        s[3] += pow( boxCount.get( k ), 3 );
      }
      return s;
    }

    /**
     * MISSING_COMMENT
     *
     * @param ngrid
     * @param boxCount
     * @param numDims
     * @param bcDims
     * @param s0Dims
     * @param s1Dims
     * @param s2Dims
     * @param s3Dims
     */

    protected void combineBoxCountingMulti
    ( NGrid ngrid,
      Map<Long, Long> boxCount,
      int numDims,
      List<Map<Long, Long>> bcDims,
      double[] s0Dims,
      double[] s1Dims,
      double[] s2Dims,
      double[] s3Dims,
      int gridIndex ) {

      if( boxCount == null ) {
        throw new NullPointerException();
      }
      if( bcDims == null ) {
        throw new NullPointerException();
      }
      if( s0Dims == null ) {
        throw new NullPointerException();
      }
      if( s1Dims == null ) {
        throw new NullPointerException();
      }
      if( s2Dims == null ) {
        throw new NullPointerException();
      }
      if( s3Dims == null ) {
        throw new NullPointerException();
      }

      for( int i = 0; i < numDims; i++ ) {
        s0Dims[i] = 0.0;
        s1Dims[i] = 0.0;
        s2Dims[i] = 0.0;
        s3Dims[i] = 0.0;
      }

      for( Long k : boxCount.keySet() ) {
        // generate the cell coordinates from the string key
        final List<Long> coords = ngrid.getCellCoords( k );
        for( int i = 0; i < numDims; i++ ) {
          final Map<Long, Long> currentBc = bcDims.get( i );
          if( currentBc.containsKey( coords.get( i ) ) ) {
            currentBc.put( coords.get( i ),
                           currentBc.get( coords.get( i ) ) +
                           boxCount.get( k ) );
          } else {
            currentBc.put( coords.get( i ), boxCount.get( k ) );
          }
        }
      }
      for( int i = 0; i < numDims; i++ ) {
        final Map<Long, Long> currentBc = bcDims.get( i );
        s0Dims[i] = currentBc.size();
        for( Long k : currentBc.keySet() ) {
          s1Dims[i] += currentBc.get( k );
          s2Dims[i] += pow( currentBc.get( k ), 2 );
          s3Dims[i] += pow( currentBc.get( k ), 3 );
        }
        System.out.printf( "Grid %d ---> Dim %d S0[%.15f,%.15f,...]=%.15f%n",
                           gridIndex, i, cellSides[0], cellSides[1], s0Dims[i] );
        System.out.printf( "Grid %d ---> Dim %d S2[%.15f,%.15f,...]=%.15f%n",
                           gridIndex, i, cellSides[0], cellSides[1], s2Dims[i] );
      }
    }


    /**
     * MISSING_COMMENT
     *
     * @param inputType
     * @param maxCount
     * @param minCount
     * @param grid
     * @param ngrid
     * @param boxCount
     * @param numTiles
     * @param numDims
     * @param s0
     * @param s1
     * @param s2
     * @param s3
     * @param card
     * @param startRow
     * @param startCol
     * @param cellSide
     */

    protected void computeMoranIndex
    ( String inputType,
      Double maxCount,
      Double minCount,
      Grid grid,
      NGrid ngrid,
      Map<Long, Long> boxCount,
      Double numTiles,
      Integer numDims,
      Double s0,
      Double s1,
      Double s2,
      Double s3,
      Long card,
      long startRow,
      long startCol,
      double cellSide,
      Map<Double, Double> m0Map,
      Map<Double, Double> m1Map,
      Map<Double, Double> m2Map ) {

      if( m0Map == null ) {
        throw new NullPointerException();
      }
      if( m1Map == null ) {
        throw new NullPointerException();
      }
      if( m2Map == null ) {
        throw new NullPointerException();
      }

      if( inputType.equalsIgnoreCase( wktFormat ) ||
          inputType.equalsIgnoreCase( csvFormat ) ) {

        // range of classes
        double sumClasses = 0.0, avgClasses = 0.0;
        // compute Moran's index
        final int numClasses = 10;
        final double deltaClasses = ( maxCount - minCount ) / numClasses;

        for( Long k : boxCount.keySet() ) {
          sumClasses +=
            ( ceil( ( boxCount.get( k ) - minCount ) / deltaClasses ) == 0 ?
                1 :
              ceil( ( boxCount.get( k ) - minCount ) / deltaClasses ) );
        }

        double avg0, avg1, avg2, avg3;
        avgClasses = sumClasses / numTiles;
        avg0 = s0 / numTiles;
        avg1 = s1 / numTiles;
        avg2 = s2 / numTiles;
        avg3 = s3 / numTiles;

        // start update of 28/03/2019
        if( avg0 == 1 ) avg0 = 0.999;
        if( avg1 == 1 ) avg1 = 0.999;
        // end update of 28/3/2019

        System.out.printf
          ( "%s%s: card-> %d, "
            + "avg0-> %.15f, avg1-> %.15f, "
            + "avg2-> %.15f, avg3->%.15f, "
            + "for N-> %.15f%n",
            reducePhaseLabel, cleanupMethodLabel,
            card, avg0, avg1, avg2, avg3, numTiles );
        System.out.printf
          ( "%s%s: deltaClasses: %.15f, avgClasses: %.15f.%n",
            reducePhaseLabel, cleanupMethodLabel,
            deltaClasses, avgClasses );

        System.out.printf
          ( "%s%s: start computation Moran's Index...%n",
            reducePhaseLabel, cleanupMethodLabel );


        double w = 0.0;
        double num0 = 0.0;
        double num1 = 0.0;
        double num2 = 0.0;
        double den0 = 0.0;
        double den1 = 0.0;
        double den2 = 0.0;
        double maxrow = 1.0, maxcol = 1.0;

        List<Long> cell = null, cellnew = null;
        if( inputType.equalsIgnoreCase( wktFormat ) ) {
          maxrow = grid.numRows;
          maxcol = grid.numColumns;
        }

        // skip the first grid for the computation of the Moran's index
        boolean skip = ( numTiles > maxIterations );

        if( skip ) {
          System.out.printf
            ( "Skip the computation of the Moran's index "
              + "because the number of tiles %d is too high.%n",
              numTiles );

        } else {
          for( Long k = 0L; k < numTiles; k++ ) {
            // --- DEN computation ---------------------------------------------
            final double dens[] = new double[3];
            final double xis[] = new double[3];

            computeMiDenominators
              ( boxCount, k, avg0, avg1, avg2, minCount,
                deltaClasses, avgClasses, dens, xis );

            den0 += dens[0];
            den1 += dens[1];
            den2 += dens[2];

            final double x0i = xis[0];
            final double x1i = xis[1];
            final double x2i = xis[2];

            // current CELL
            long row = startRow;
            long col = startCol;
            if( inputType.equalsIgnoreCase( wktFormat ) ) {
              // position (row,col)
              row = grid.getRow( k );
              col = grid.getCol( k );
            } else {
              // position cell
              cell = ngrid.getCellCoords( k );
            }

            // --- NUM computation ---------------------------------------------
            if( inputType.equalsIgnoreCase( wktFormat ) ) {

              // position (-1, 0)
              if( ( row - 1L ) > 0L ) {
                final double kad = grid.getCellId( ( row - 1L ), col );
                final double[] nums = computeMiNumerators
                  ( boxCount, kad,
                    x0i, x1i, x2i,
                    avg0, avg1, avg2,
                    minCount, deltaClasses, avgClasses );
                num0 += nums[0];
                num1 += nums[1];
                num2 += nums[2];

                w++;
              }

              // position (0,-1)
              if( ( col - 1L ) > 0L ) {
                final double kad = grid.getCellId( row, col - 1L );
                final double[] nums = computeMiNumerators
                  ( boxCount, kad,
                    x0i, x1i, x2i,
                    avg0, avg1, avg2,
                    minCount, deltaClasses, avgClasses );
                num0 += nums[0];
                num1 += nums[1];
                num2 += nums[2];

                w++;
              }

              // position (1, 0)
              if( ( row + 1L ) <= maxrow ) {
                final double kad = grid.getCellId( row + 1L, col );
                final double[] nums = computeMiNumerators
                  ( boxCount, kad,
                    x0i, x1i, x2i,
                    avg0, avg1, avg2,
                    minCount, deltaClasses, avgClasses );
                num0 += nums[0];
                num1 += nums[1];
                num2 += nums[2];

                w++;
              }

              // position (0,1)
              if( ( col + 1L ) <= maxcol ) {
                final double kad = grid.getCellId( row, ( col + 1L ) );
                final double[] nums = computeMiNumerators
                  ( boxCount, kad,
                    x0i, x1i, x2i,
                    avg0, avg1, avg2,
                    minCount, deltaClasses, avgClasses );
                num0 += nums[0];
                num1 += nums[1];
                num2 += nums[2];

                w++;
              }

            } else { // csv case
              // caso CSV
              for( int j = 0; j < numDims; j++ ) {
                if( ( cell.get( j ) - 1L ) > 0L ) {
                  cellnew = cell;
                  cellnew.set( j, ( cell.get( j ) - 1L ) );
                  final double kad = ngrid.getCellNumber( cellnew );
                  final double[] nums = computeMiNumerators
                    ( boxCount, kad,
                      x0i, x1i, x2i,
                      avg0, avg1, avg2,
                      minCount, deltaClasses, avgClasses );
                  num0 += nums[0];
                  num1 += nums[1];
                  num2 += nums[2];

                  w++;
                }
              }
              for( int h = 0; h < numDims; h++ ) {
                if( ( cell.get( h ) + 1L ) <= ngrid.numCell ) {
                  cellnew = cell;
                  cellnew.set( h, ( cell.get( h ) + 1L ) );
                  final double kad = ngrid.getCellNumber( cellnew );
                  final double[] nums = computeMiNumerators
                    ( boxCount, kad,
                      x0i, x1i, x2i,
                      avg0, avg1, avg2,
                      minCount, deltaClasses, avgClasses );
                  num0 += nums[0];
                  num1 += nums[1];
                  num2 += nums[2];

                  w++;
                }
              }
            }
          }
          // }

          // if( !skip ) {
          final double miVal0 = numTiles / w * ( num0 / den0 );
          final double miVal1 = numTiles / w * ( num1 / den1 );
          final double miVal2 = numTiles / w * ( num2 / den2 );

          System.out.printf( "%s%s: end computation Moran's Index " +
                             "N: %.15f, W: %.15f.%n",
                             reducePhaseLabel, cleanupMethodLabel,
                             numTiles, w );
          System.out.printf( "%s%s: MI_val0 = %.15f = (%.15f/%.15f)%n",
                             reducePhaseLabel, cleanupMethodLabel,
                             miVal0, num0, den0 );
          System.out.printf( "%s%s: MI_val1 = %.15f = (%.15f/%.15f)%n",
                             reducePhaseLabel, cleanupMethodLabel,
                             miVal1, num1, den1 );
          System.out.printf( "%s%s: MI_val0 = %.15f = (%.15f/%.15f)%n",
                             reducePhaseLabel, cleanupMethodLabel,
                             miVal2, num2, den2 );

          final double width;
          if( inputType.equalsIgnoreCase( wktFormat ) ) {
            width = grid.width;
          } else {
            // attenzione uso la larghezza della griglia sulla prima dimensione (coordinata x)
            // anche se la griglia potrebbe non essere un nCubo: in realtà è quasi sempre un nCubo
            width = ngrid.size.get( 0 );
          }

          m0Map.put( ( cellSide / width ), miVal0 );
          m2Map.put( ( cellSide / width ), miVal2 );
          m1Map.put( ( cellSide / width ), miVal1 );
        }
      }
    }


    /**
     * MISSING_COMMENT
     *
     * @param boxCount
     * @param kad
     * @param x0i
     * @param x1i
     * @param x2i
     * @param avg0
     * @param avg1
     * @param avg2
     * @param minCount
     * @param deltaClasses
     * @param avgClasses
     * @return
     */

    private double[] computeMiNumerators
    ( Map<Long, Long> boxCount,
      double kad,
      double x0i,
      double x1i,
      double x2i,
      double avg0,
      double avg1,
      double avg2,
      double minCount,
      double deltaClasses,
      double avgClasses
    ) {

      if( boxCount == null ) {
        throw new NullPointerException();
      }

      final double[] nums = new double[3];

      final double xj = ( boxCount.containsKey( kad ) ?
                            boxCount.get( kad ) : 0.0 );
      final double x0j = ( xj > 0 ? 1.0 : 0.0 );
      nums[0] += ( x0i - avg0 ) * ( x0j - avg0 );

      // calcolo senza elevare al quadrato o cubo
      nums[1] += ( x1i - avg1 ) * ( xj - avg1 );
      // fine - calcolo senza elevare al quadrato o cubo

      // square
      //x2_j = Math.pow(x_j,2);
      //NUM2 += (x2_i - avg2)*(x2_j - avg2);

      // subdivision into 10 classes
      double x2j = ceil( ( xj - minCount ) / deltaClasses );
      if( x2j == 0 ) x2j = 1;
      nums[2] += ( x2i - avgClasses ) * ( x2j - avgClasses );

      return nums;
    }


    private void computeMiDenominators
      ( Map<Long, Long> boxCount,
        long k,
        double avg0,
        double avg1,
        double avg2,
        double minCount,
        double deltaClasses,
        double avgClasses,
        double[] dens,
        double[] xis ) {

      if( boxCount == null ) {
        throw new NullPointerException();
      }
      if( dens == null ) {
        throw new NullPointerException();
      }
      if( xis == null ) {
        throw new NullPointerException();
      }

      xis[0] = ( boxCount.containsKey( k ) ? 1.0 : 0.0 );
      dens[0] += pow( ( xis[0] - avg0 ), 2 );

      xis[1] = ( boxCount.containsKey( k ) ? boxCount.get( k ) : 0.0 );
      dens[1] += pow( ( xis[1] - avg1 ), 2 );

      // subdivision into 10 classes
      xis[2] =
        ( boxCount.containsKey( k ) ?
            ceil( ( boxCount.get( k ) - minCount ) / deltaClasses ) : 1 );
      if( xis[2] == 0 ) xis[2] = 1;
      dens[2] += pow( ( xis[2] - avgClasses ), 2 );

      // cube
      // x3_i = (boxCountNew.containsKey(k)?Math.pow(boxCountNew.get(k),3):0.0);
      // DEN3 += Math.pow((x3_i - avg3),2);
    }


    /**
     * MISSING_COMMENT
     *
     * @param dMap
     * @param changeRegressionSign
     * @param dkind
     * @return
     */

    private List<List<Double>> computeRegression
    ( Map<Double, Double> dMap,
      boolean changeRegressionSign,
      String dkind ) {

      if( dMap == null ) {
        throw new NullPointerException();
      }

      final List<Double> dsort = new ArrayList<>( dMap.keySet() );
      Collections.sort( dsort );

      // debug
      System.out.printf( "%s: {", dkind );
      for( Double d : dsort ) {
        System.out.printf( "%.15f=%.15f,", d, dMap.get( d ) );
      }
      System.out.printf( "}%n" );

      final Map<Double, Double> slopeVar = new HashMap<>();
      slopeVar.put( 0.0, dsort.get( 0 ) );

      // compute the slope
      double slope1 = ( dMap.get( dsort.get( 1 ) ) -
                        dMap.get( dsort.get( 0 ) ) ) /
                      ( dsort.get( 1 ) - dsort.get( 0 ) );
      double slope2 = ( dMap.get( dsort.get( 2 ) ) -
                        dMap.get( dsort.get( 1 ) ) ) /
                      ( dsort.get( 2 ) - dsort.get( 1 ) );

      // compute the angle variations
      double alpha = atan( abs( slope2 - slope1 ) );
      slopeVar.put( alpha, dsort.get( 1 ) );

      int i = 3;
      double first = 0.0, second = dsort.get( 2 );
      while( i < dsort.size() ) {
        first = second;
        second = dsort.get( i );
        slope1 = slope2;
        slope2 = ( dMap.get( first ) - dMap.get( second ) ) /
                 ( first - second );
        alpha = atan( abs( slope2 - slope1 ) );
        slopeVar.put( alpha, first );
        i++;
      }

      // slope variations
      final List<Double> alphaOrd = new ArrayList<>( slopeVar.keySet() );
      Collections.sort( alphaOrd );

      // debug
      System.out.printf( "Slope var %s: {", dkind );
      for( Double d : alphaOrd ) {
        System.out.printf( "%.15f=%.15f,", d, slopeVar.get( d ) );
      }
      System.out.printf( "}%n" );

      // choose the 3 bigger variations
      final int numVar = 3;
      final List<Double> splitPoint = new ArrayList<>( numVar );
      splitPoint.add( 0, slopeVar.get( alphaOrd.get( alphaOrd.size() - 1 ) ) );
      if( alphaOrd.size() > 1 ) {
        splitPoint.add( 1, slopeVar.get( alphaOrd.get( alphaOrd.size() - 2 ) ) );
        if( alphaOrd.size() > 2 )
          splitPoint.add( 2, slopeVar.get( alphaOrd.get( alphaOrd.size() - 3 ) ) );
      }
      Collections.sort( splitPoint );

      // remove the split points that are too near to each other
      final List<Double> splitPointToKeep = new ArrayList<>( 10 );
      final int minLength = 3;

      int j = 0;
      int length = 1, a = 0;
      System.out.printf( "%s SplitPoint...%n", dkind );
      while( j < dsort.size() ) {
        System.out.println( dkind + " SplitPoint: " +
                            splitPoint.get( a ) + " " +
                            dkind + " x: " +
                            dsort.get( j ) );
        if( splitPoint.get( a ).equals( dsort.get( j ) ) ) {
          System.out.println( dkind + " lenght: " + length );
          if( length >= minLength ) {
            System.out.println( dkind + " SplitPointToKeep inserito: " +
                                dsort.get( j ) );
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
        j++;
      }
      // remove the last split if the length of the last interval is too small
      System.out.println( dkind + " lenght last: " + length );
      if( length <= minLength ) {
        System.out.println( dkind + " SplitPointToKeep removed: " +
                            splitPointToKeep.get( splitPointToKeep.size() - 1 ) );
        splitPointToKeep.remove( splitPointToKeep.size() - 1 );
      }

      // compute the slope
      final List<List<Double>> dfinal = new ArrayList<>( 10 );
      System.out.printf( "%n%s ->>> start slope computation%n", dkind );
      final SimpleRegression regression = new SimpleRegression();
      int aCoeff = 0;
      //double k;
      Double support = 0.0;
      int index = 0;

      while( index < dMap.size() ) {
        final double k = dsort.get( index++ );
        System.out.println( dkind +
                            ": interval " + aCoeff +
                            " key[" + ( index - 1 ) + "]: "
                            + k + " value: " + dMap.get( k ) );
        regression.addData( k, dMap.get( k ) );
        support++;
        if( aCoeff < splitPointToKeep.size() &&
            splitPointToKeep.get( aCoeff ).equals( k ) ) {
          System.out.printf( dkind + " regression slope ->> = " +
                             regression.getSlope() +
                             " regression intercept ->> = " +
                             regression.getIntercept() + " to ->> " +
                             splitPointToKeep.get( aCoeff ) );
          final List<Double> elem = new ArrayList<>( 2 );
          elem.add( support );
          if( changeRegressionSign ) {
            elem.add( -regression.getSlope() );
          } else {
            elem.add( regression.getSlope() );
          }
          dfinal.add( elem );

          regression.clear();
          support = 0.0;
          aCoeff++;
        }
      }
      System.out.printf( "%s regression slope ->> = %.15f " +
                         "regression intercept ->> = %.15f " +
                         "to ->> %.15f%n",
                         dkind,
                         regression.getSlope(),
                         regression.getIntercept(),
                         dsort.get( dsort.size() - 1 ) );

      final List<Double> elem = new ArrayList<>( 2 );
      elem.add( support );
      if( changeRegressionSign ) {
        elem.add( -regression.getSlope() );
      } else {
        elem.add( regression.getSlope() );
      }
      dfinal.add( elem );

      return dfinal;
    }


    /**
     * MISSING_COMMENT
     *
     * @param dfinal
     */

    private Double[][] valuesWithGreatestSupport
    ( String dkind,
      List<List<Double>> dfinal ) {

      if( dfinal == null ) {
        throw new NullPointerException();
      }

      // chose the values with the greatest support
      Double d1 = 0.0, d2 = 0.0; //, supp; //
      Double d1Supp, d2Supp;
      //List<Double> dElem;
      // Double D0;
      if( dfinal.size() <= 2 ) {
        d1 = dfinal.get( 0 ).get( 1 );
        d1Supp = dfinal.get( 0 ).get( 0 );

        if( dfinal.size() == 1 ) {
          d2 = d1;
          d2Supp = d1Supp;
        } else {
          d2 = dfinal.get( 1 ).get( 1 );
          d2Supp = dfinal.get( 1 ).get( 0 );
        }

      } else { // more than 2 elements
        // dElem = D0final.get( 0 );

        d1 = dfinal.get( 0 ).get( 1 );
        d1Supp = dfinal.get( 0 ).get( 0 );

        // dElem = D0final.get( 1 );
        d2 = dfinal.get( 1 ).get( 1 );
        d2Supp = dfinal.get( 1 ).get( 0 );


        for( int i = 2; i < dfinal.size(); i++ ) {
          // dElem = D0final.get( i );
          final double currSupp = dfinal.get( i ).get( 0 );
          final double currD = dfinal.get( i ).get( 1 );

          if( currSupp <= d1Supp && currSupp <= d2Supp ) {
            continue;
          } else if( currSupp > d2Supp && currSupp <= d1Supp ) {
            // replace the second
            d2Supp = currSupp;
            d2 = currD;

          } else if( currSupp > d1Supp && currSupp <= d2Supp ) {
            // replace the first with a shif
            d1Supp = d2Supp;
            d1 = d2;
            d2Supp = currSupp;
            d2 = currD;

          } else if( currSupp > d1Supp && currSupp > d2Supp ) {
            // determine which between d1 and d2 have to be replaced
            if( d1Supp >= d2Supp ) {
              // replace the second
              d2Supp = currSupp;
              d2 = currD;
            } else {
              // replace the first with a shift
              d1Supp = d2Supp;
              d1 = d2;
              d2Supp = currSupp;
              d2 = currD;
            }
          }
        }
      }
      final Double[][] result = new Double[2][2];
      result[0] = new Double[]{d1, d1Supp};
      result[1] = new Double[]{d2, d2Supp};
      System.out.printf( "%s: %.15f (%.15f), %.15f (%.15f)%n%n",
                         dkind,
                         result[0][0], result[0][1],
                         result[1][0], result[1][1] );
      return result;
    }


    /**
     * MISSING_COMMENT
     *
     * @param ncellMap
     * @param card
     * @return
     */

    private double computeEmptyCells
    ( Map<Double, Double> ncellMap,
      Long card ) {

      final List<Double> klcell = new ArrayList<>( ncellMap.keySet() );
      Collections.sort( klcell );

      int i = 0;
      int den = 0;
      double val, avgEmpty = 0.0;

      while( i < klcell.size() - 3 ) {
        if( round( ( 1 / exp( klcell.get( i ).doubleValue() ) ) *
                   ( 1 / exp( klcell.get( i ).doubleValue() ) ) )
            <= card ) {
          val = ncellMap.get( klcell.get( i ) );
          avgEmpty += val;
          den = den + 1;
          System.out.printf
            ( "Percentage of emptiness for cell %d [%d] = %f%n",
              i,
              round( ( 1 / exp( klcell.get( i ).doubleValue() ) ) *
                     ( 1 / exp( klcell.get( i ).doubleValue() ) ) ),
              val );
        } else {
          System.out.printf
            ( "Num cells greater than cardinality %d [%d] card = %d%n",
              i,
              round( ( 1 / exp( klcell.get( i ).doubleValue() ) ) *
                     ( 1 / exp( klcell.get( i ).doubleValue() ) ) ),
              card );
        }
        i = i + 1;
      }
      if( den > 0 ) {
        avgEmpty = avgEmpty / den;
      } else {
        avgEmpty = -1.0;
      }
      System.out.printf( "Average percentage of emptiness: %f%n", avgEmpty );
      return avgEmpty;
    }

    // ===========================================================================

    protected ArrayList<Double> QuadTree( Map<Long, Long> bcc,
                                          double origGriglia, double orig, double size, double cellSide,
                                          long thr ) {
      ArrayList<Double> part = new ArrayList<Double>();
      // parto dall'origine
      part.add( 0, new Double( orig ) );

      long j;
      // calcolo la cella da cui parto in base all'origine della griglia
      if( orig == origGriglia )
        j = 1L;
      else
        j = ( (long) ceil( (double) ( orig - origGriglia ) / cellSide ) );

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

    protected ArrayList<Double> RTree( Map<Long, Long> bcc,
                                       double origGriglia, double orig, double size, double cellSide,
                                       long thr ) {
      ArrayList<Double> part = new ArrayList<Double>();
      long j;
      // calcolo la cella da cui parto in base all'origine della griglia
      if( orig == origGriglia )
        j = 1L;
      else
        j = ( (long) ceil( (double) ( orig - origGriglia ) / cellSide ) );

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
