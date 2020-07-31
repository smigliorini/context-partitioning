package it.univr.hadoop.input;


import it.univr.descriptors.NRectangle;
import it.univr.hadoop.ContextData;
import it.univr.operations.GlobalIndex;
import it.univr.operations.Partition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static it.univr.operations.RangeQuery.rangeQueryLabel;

public abstract class ContextBasedInputFormat<K, V extends ContextData>
  extends FileInputFormat<K, Iterable<V>> {

  // === Properties ============================================================

  private static final String masterFilePrefix = "_master";

  // === Methods ===============================================================

  private static Predicate<FileStatus> partByName( String prefix ) {
    return partition -> partition.getPath().getName().startsWith( prefix );
  }

  /**
   * MISSING_COMMENT
   *
   * @param fs
   * @param dir
   * @return
   */

  private GlobalIndex getGlobalIndex( FileSystem fs, Path dir ) {
    if( fs == null ) {
      throw new NullPointerException();
    }
    if( dir == null ) {
      throw new NullPointerException();
    }

    try {
      final FileStatus[] allFiles;
      if( dir.toString().indexOf( '*' ) != -1 ||
          dir.toString().indexOf( '?' ) != -1 ) {
        allFiles = fs.globStatus( dir );
      } else {
        allFiles = fs.listStatus( dir );
      }

      FileStatus masterFile = null;
      for( FileStatus status : allFiles ) {
        if( status.getPath().getName().startsWith( masterFilePrefix ) ) {
          if( masterFile == null ) {
            masterFile = status;
          } else {
            throw new RuntimeException
              ( String.format
                ( "Found more than one master file in %s.%n", dir ) );
          }
        }
      }

      // global index found!
      if( masterFile != null ) {
        return processMasterFile( fs, masterFile );
      }

    } catch( IOException e ) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * MISSING_COMMENT
   *
   * @param master
   * @return
   */

  private GlobalIndex processMasterFile( FileSystem fs, FileStatus master ) {
    if( fs == null ) {
      throw new NullPointerException();
    }

    if( master == null ) {
      throw new NullPointerException();
    }

    final GlobalIndex gi = new GlobalIndex();

    try {
      final FSDataInputStream inputStream = fs.open( master.getPath() );

      final List<String> lines = new ArrayList<>();
      byte[] buffer = new byte[512];
      int filePosition = 0;
      int offset = 0;

      while( inputStream.read( filePosition, buffer, offset, buffer.length ) > 0 ) {

        final int end = skiptoEol( buffer, offset );
        final byte[] tmpBuffer = new byte[end];
        System.arraycopy( buffer, 0, tmpBuffer, 0, end );

        lines.add( new String( tmpBuffer ).trim() );
        // reset the array
        buffer = new byte[512];
        filePosition += tmpBuffer.length;
      }

      for( String l : lines ) {
        gi.addPartition( parsePartitionLine( l ) );
      }//*/

    } catch( IOException e ) {
      e.printStackTrace();
    }

    return gi;
  }

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @return
   */

  private Partition parsePartitionLine( String line ) {
    if( line == null ) {
      throw new NullPointerException();
    }

    final Partition p = new Partition();
    Double minA = 0.0, maxA = 0.0, minT = 0.0, maxT = 0.0,
      minX = 0.0, maxX = 0.0, minY = 0.0, maxY = 0.0;

    final StringTokenizer tk = new StringTokenizer( line, "," );
    int counter = 0;

    // todo!!!
    // 0, => part counter
    // 12.0,14.0, => age
    // 9.456108785473048E11,1.1555538924901055E12, => time
    // 10.985806692694979,10.99544088098212, => x
    // 45.43135717475358,45.44067353466441, => y
    // part-000-000-000-000 => partition name
    while( tk.hasMoreTokens() ) {
      final String token = tk.nextToken();
      switch( counter ) {
        case 0:
          p.setNumber( new Integer( token ) );
          counter++;
          break;
        case 1:
          minA = new Double( token );
          counter++;
          break;
        case 2:
          maxA = new Double( token );
          counter++;
          break;
        case 3:
          minT = new Double( token );
          counter++;
          break;
        case 4:
          maxT = new Double( token );
          counter++;
          break;
        case 5:
          minX = new Double( token );
          counter++;
          break;
        case 6:
          maxX = new Double( token );
          counter++;
          break;
        case 7:
          minY = new Double( token );
          counter++;
          break;
        case 8:
          maxY = new Double( token );
          counter++;
          break;
        case 9:
          p.setFilename( token );
          counter++;
          break;
        default:
          break;
      }
    }

    final String boundaries = String.format
      ( "%s,%s,%s,%s,"
        + "%s,%s,%s,%s",
        minX, minY, minT, minA,
        maxX, maxY, maxT, maxA );
    p.setBoundaries( boundaries );
    return p;
  }

  /**
   * MISSING_COMMENT
   *
   * @param buffer
   * @param startOffset
   * @return
   */
  private int skiptoEol( byte[] buffer, int startOffset ) {
    int eol = startOffset;
    while( eol < buffer.length && ( buffer[eol] != '\n' && buffer[eol] != '\r' ) ) {
      eol++;
    }
    while( eol < buffer.length && ( buffer[eol] == '\n' || buffer[eol] == '\r' ) ) {
      eol++;
    }
    return eol;
  }

  @Override
  protected List<FileStatus> listStatus( JobContext job ) throws IOException {
    final List<FileStatus> result = new ArrayList<>();

    final Configuration conf = job.getConfiguration();
    final String rqs = conf.get( rangeQueryLabel );
    final NRectangle rq;

    if( rqs != null ) {
      rq = new NRectangle( rqs );
    } else {
      rq = null;
    }

    final Path[] inputPaths = getInputPaths( job );
    final List<FileStatus> partStatus = super.listStatus( job );

    for( Path ip : inputPaths ) {
      final FileSystem fs = ip.getFileSystem( conf );
      final GlobalIndex gi = getGlobalIndex( fs, ip );

      // found a global index
      if( gi != null ) {
        final List<Partition> partitions = gi.getPartitionList();
        for( Partition p : partitions ) {
          if( rq != null ) {
            // filter partition based on partition boundaries
            if( p.getBoundaries().intersectsMultiDims( rq ) ) {
              result.addAll(
                partStatus.stream().filter( partByName( p.getFilename() ) )
                          .collect( Collectors.toList() ) );
            }
          }
        }
      } else {
        result.add( fs.getFileStatus( ip ) );
      }
    }
    return result;
  }


}
