package it.univr.restaurant.descriptors;

import it.univr.restaurant.descriptors.NRectangle;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * @author Alberto Belussi
 */

public class NGrid {
  /**
   * Dimensions
   */
  protected int dim;

  /**
   * Origin of the grid
   */
  protected ArrayList<Double> orig;

  /**
   * Grid size
   */
  protected ArrayList<Double> size;

  /**
   * Number of tiles
   */
  protected long numTiles;

  /**
   * Total number of columns and rows within the input range
   */
  protected Long numCell;

  /**
   * Grid pow
   */
  protected ArrayList<Long> pow;

  /**
   * Size of a single tile
   */
  protected ArrayList<Double> tileSide;

  /**
   * Correct Ngrid
   */
  protected boolean isValid;

  /**Array of cells: la cella di indice zero corrisponde
   * alla cella in basso a sinistra del MBR.
   */

  /**
   * Crea una griglia in base a MBR e lato della cella.
   *
   * @param mbr      minimum bonding rectangle
   * @param cellSide lato di ogni cella della griglia
   */
  public NGrid(String mbr, double cellSide, int dim ) {
    Pattern p = Pattern.compile( "(-?\\d+(.|,)\\d+)" ); //originale
    ArrayList<Double> al = new ArrayList<Double>();
    Matcher m = p.matcher( mbr );
    while( m.find() ) {
      String tmp = m.group();
      tmp = tmp.replace( ',', '.' );
      al.add( new Double( tmp ) );
    }
    if( dim != al.size() / 2 ) {
      this.isValid = false;
      return;
    }

    this.dim = dim;
    this.orig = new ArrayList<Double>( dim );
    this.size = new ArrayList<Double>( dim );
    this.tileSide = new ArrayList<Double>( dim );
    this.pow = new ArrayList<Long>( dim );

    this.tileSide.add( 0, cellSide );
    this.numTiles = 1;
    Double diff;

    for( int i = 0; i < this.dim; i++ ) {
      this.orig.add( al.get( i ) );
      diff = al.get( i + this.dim ) - al.get( i );
      if( diff < 0 ) {
        this.isValid = false;
        return;
      }
      this.size.add( i, diff );
      if( i == 0 )
        // calcolo il numero di celle sulla prima dimensione e poi uso
        // tale numero per dividere anche le altre dimensioni così da
        // generare un cubo con lo stesso numero di celle su tutte le
        // dimensioni ma dove la larghezza su ogni dimensione può essere
        // diversa
        this.numCell = (long) Math.ceil( diff / cellSide );
      else
        this.tileSide.add( i, ( diff / this.numCell ) );

      this.numTiles *= this.numCell;
    }

    for( int i = 0; i < this.dim; i++ ) {
      long a = 1L;
      for( int j = i + 1; j < this.dim; j++ ) {
        a *= numCell;
      }
      pow.add( a );
    }

    this.isValid = true;
  }

  /*
   * Calcola la cella successiva nell'ordine dentro il range
   */
  private ArrayList<Long> nextCell( ArrayList<Long> cell,
                                    ArrayList<Long> rangeStart, ArrayList<Long> rangeEnd ) {

    ArrayList<Long> res = new ArrayList<Long>( this.dim );

    // inizializzo la cella risultato con la cella di partenza
    for( int i = 0; i < this.dim; i++ )
      res.add( cell.get( i ) );

    long riporto = 0;
    // incremento l'ultima dimensione
    if( cell.get( this.dim - 1 ) + 1 > rangeEnd.get( this.dim - 1 ) ) {
      res.set( this.dim - 1, rangeStart.get( this.dim - 1 ) );
      riporto = 1;
    } else
      res.set( this.dim - 1, cell.get( this.dim - 1 ) + 1 );

    // propago il riporto sulle dimensioni successive
    if( riporto == 1 ) {
      for( int i = this.dim - 2; i >= 0; i-- ) {
        if( cell.get( i ) + 1 > rangeEnd.get( i ) )
          res.set( i, rangeStart.get( i ) );
        else {
          res.set( i, res.get( i ) + 1 );
          riporto = 0;
          break;
        }
      }
      // gestire caso in cui ho raggiungo l'ultima cella
      if( riporto == 1 )
        // TODO CHECK
        //res.set(0, rangeEnd.get(0)+1);
        return null;
    }

    return res;
  }

  /*
   * Calcola le coordinate della cella. id=0 => cella (1,1,1,...)
   */
  public ArrayList<Long> getCellCoords( long id ) {
    ArrayList<Long> res = new ArrayList<Long>( this.dim );
    long a = id;
    for( int i = 0; i < this.dim; i++ ) {
      res.add( ( (long) ( a / pow.get( i ) ) ) + 1L );
      a = ( a % pow.get( i ) );
    }

    return res;
  }

  /*
   * Calcola l'identificativo della cella. Cella iniziale (1,1,1,...) => id=0
   */
  public long getCellNumber( List<Long> c ) {
    long res = 0;

    for( int i = 0; i < this.dim; i++ )
      res += ( c.get( i ) - 1 ) * pow.get( i );

    return res;
  }

  /**
   * Il metodo verifica l'intersezione tra una cella della griglia e il
   * rettangolo ndimensinoale passata come parametro. Restituisce un vettore.
   *
   * @param nrect
   * @return array con indice id della cella e valore il numero di
   * sovrapposizioni.
   */
  public Long[] overlapPartitions( NRectangle nrect ) {
    if( nrect == null )
      return null;

    ArrayList<Long> al = new ArrayList<Long>();

    //long row, col;
    //long startRow, endRow, startCol, endCol;
    ArrayList<Long> rangeStart = new ArrayList<Long>( this.dim );
    ArrayList<Long> rangeEnd = new ArrayList<Long>( this.dim );

    //compute ranges
    for( int i = 0; i < nrect.dim; i++ ) {
      rangeStart.add( (long) Math.min( this.numCell,
                                       Math.ceil( Math.abs( nrect.coordMin.get( i ) - this.orig.get( i ) ) / this.tileSide.get( i ) ) ) );
      rangeEnd.add( (long) Math.min( this.numCell,
                                     Math.ceil( Math.abs( nrect.coordMax.get( i ) - this.orig.get( i ) ) / this.tileSide.get( i ) ) ) );
    }

    ArrayList<Long> cellStart = new ArrayList<Long>( this.dim );
    // first intersected cell
    for( int i = 0; i < nrect.dim; i++ ) {
      cellStart.add( i, rangeStart.get( i ) );
    }

    // last intersected cell
    ArrayList<Long> cellEnd = new ArrayList<Long>( this.dim );
    for( int i = 0; i < nrect.dim; i++ ) {
      cellEnd.add( i, rangeEnd.get( i ) );
    }
    // TODO CHECK OK
    //System.out.println("Cell start: "+cellStart.get(0)+" "+cellStart.get(1)+" "+cellStart.get(2));
    //System.out.println("Cell end: "+cellEnd.get(0)+" "+cellEnd.get(1)+" "+cellEnd.get(2));

    ArrayList<Long> cell = cellStart;
    //long flag = this.getCellNumber(cellEnd);
    //System.out.println("Cell start id: "+this.getCellNumber(cellStart));
    //System.out.println("Cell end id: "+flag);
    //int i = 0;
    while( cell != null ) {
      al.add( this.getCellNumber( cell ) );
      cell = this.nextCell( cell, rangeStart, rangeEnd );
      //if (i++==0) {
      //	  System.out.println("Cell id: "+this.getCellNumber(cell));
      //	  System.out.println("Cell: "+cell.get(0)+" "+cell.get(1)+" "+cell.get(2));
      //}
    }
    if( !cellEnd.equals( cellStart ) )
      al.add( this.getCellNumber( cellEnd ) );
		/*
		startCol = (long)Math.ceil(Math.abs(mbrGeo.getMinX() - x) / tileSide);
		if (startCol == 0) startCol = 1;
		endCol = (long)Math.ceil(Math.abs(mbrGeo.getMaxX() - x) / tileSide);
		if (endCol == 0) endCol = 1;
		startRow = (long)Math.ceil(Math.abs(mbrGeo.getMinY() - y) / tileSide);
		if (startRow == 0) startRow = 1;
		endRow = (long)Math.ceil(Math.abs(mbrGeo.getMaxY() - y) / tileSide);
		if (endRow == 0) endRow = 1;
		*/

    Long[] a = new Long[al.size()];
    return al.toArray( a );
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append( "origin: " );
    for( int i = 0; i < dim; i++ ){
      sb.append( orig.get( i ) + " " );
    }
    for( int i = 0; i < dim; i++ ){
      sb.append( format( "size(%d)=%f ", i, size.get( i )));
    }
    sb.append( "tileside=" + tileSide );
    return sb.toString();
  }
}
