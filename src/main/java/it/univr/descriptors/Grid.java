package it.univr.descriptors;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;

/**
 * La classe genera una griglia uniforme.
 * 
 * @author Michele Reniero & Alberto Belussi
 */
public class Grid {

	/**Origin of the grid*/
	protected Double x, y;
	
	/**End of the grid*/
	protected double width, height;

	/**Number of tiles*/
	protected Long numTiles;

	/**Total number of columns and rows within the input range*/
	protected Long numColumns, numRows;

	/**With and height of a single tile*/
	//protected double tileWidth, tileHeight;
	protected double tileWidth, tileHeight;

	/**Array of cells: la cella di indice zero corrisponde 
	 * alla cella in basso a sinistra del MBR.
	 * Ogni elemeto dell'array conta il numero di intersezioni
	 * che tale cella ha con un poligono.
	 */
	//protected int[] cells; 

	/**
	 * Crea una griglia in base a MBR e lato della
	 * cella.
	 * 
	 * @param mbr minimum bonding rectangle
	 * @param cellSide lato di ogni cella della griglia
	 */
	public Grid(Rectangle mbr, double cellSide) 
	{
		this.x = mbr.x1;
		this.y = mbr.y1;
		this.width = mbr.getWidth();
		this.height = mbr.getHeight();
		this.numColumns = (long)Math.ceil(mbr.getWidth()/cellSide);
		this.tileWidth = cellSide;
		this.numRows = this.numColumns; // we force the grid to have numColumns = numRows
		this.tileHeight	= mbr.getHeight()/this.numRows; // the cells might be rectangular
		
		// total number of cells
		this.numTiles = this.numRows * this.numColumns;
		
		
		// this.cells = new int[this.numTiles];
		// long[] powers
		//this.cells = new int[this.numTiles];
		
		//for( int i = 0; i < this.numTiles; i++)
		//	this.cells[i] = 0;

	}
	
	/**
	 * Crea una griglia in base all'MBR fornito come stringa
	 * e lato della cella.
	 * 
	 * @param mbr minimo rettangolo che contiene tutte le geometrie
	 * del job.
	 * @param cellSide lato di ogni cella della griglia.
	 * @throws ParseException 
	 */
	public Grid(String mbr, double cellSide)
	{
		Pattern p = Pattern.compile("(-?\\d+[.|,]\\d+)"); //originale
		ArrayList<Double> al = new ArrayList<Double>(); 
		
		//mbr = "\"POLYGON((-0,0100000 0,00000, -0,0100000 86,6125, 100,000 86,6125, 100,000 0,00000, -0,0100000 0,00000))\"";
		//mbr = "Rectangle: (-0.01,0.0)-(100.0,86.6125)";
		//mbr = "Rectangle: (-0.01,-0.01)-(99.99,99.99)";
		
		// mbr in WKT
		if (mbr.contains("POLYGON"))
		{
			WKTReader rd = new WKTReader();
			Geometry g = null;
			try {
				g = rd.read(mbr);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
			System.out.println("MBR: " + mbr);
			if (g != null) {
				Envelope e = g.getEnvelopeInternal();
				this.x = e.getMinX();
				this.y = e.getMinY();
				this.width = e.getWidth();
				this.height = e.getHeight();
				this.numColumns = (long)Math.ceil(e.getWidth()/cellSide);
				// modifica celle rettangolari
				this.numRows = this.numColumns;
				this.tileHeight = e.getHeight()/this.numRows;
			}
		} else {
		// mbr in internal format
			Matcher m = p.matcher(mbr);
			while (m.find()) {
				String tmp = m.group();
				tmp = tmp.replace(',', '.');
				al.add(new Double(tmp));	
			}
			// al contiene tutte le cordinate
			if (al.size() < 10) {
				Rectangle r = new Rectangle(al.get(0), al.get(1), al.get(2), al.get(3));
				this.x = r.x1;
				this.y = r.y1;
				this.width = r.getWidth();
				this.height = r.getHeight();
				this.numColumns = (long)Math.ceil(r.getWidth()/cellSide);
				this.numRows =  this.numColumns;//(int)Math.ceil(r.getHeight()/cellSide);
				this.tileHeight = r.getHeight()/this.numRows;
			}
		}
		this.numTiles = this.numRows * this.numColumns;
		this.tileWidth = cellSide;
		
		//this.cells = new int[this.numTiles];
		//for( int i = 0; i < this.numTiles; i++)
		//	this.cells[i] = 0;

	}

	/**
	 * Il metodo verifica l'intersezione tra una cella della griglia
	 * e la figura passata come parametro. Restituisce un vettore. 
	 * 
	 * NOTA: The rectangle is considered open-ended. This means that
	 * the right and top edge are outside the rectangle.
	 * 
	 * @param shape figura
	 * @return array con indice id della cella e valore il numero di
	 * sovrapposizioni.
	 */
	public Long[] overlapPartitions(Geometry shape) {
		if (shape == null)
			return null;
		
		ArrayList<Long> al = new ArrayList<Long>();
		
		Geometry cell;
		Envelope env, mbrGeo = shape.getEnvelopeInternal();
		boolean simpl = false;
		// if shape has to many vertices, simplify it
		if (shape.getNumPoints() > 10000) {
			simpl = true;
			double w = mbrGeo.getWidth();
			double h = mbrGeo.getHeight();
			double tolerance;
			if (w > h) tolerance = w*0.001; else tolerance = h*0.001;
			System.out.println("Before simplifier: " + shape.getNumPoints());
			shape = TopologyPreservingSimplifier.simplify(shape, tolerance);
			System.out.println("simplification applied: " + shape.getNumPoints());
		}
		
		long row, col;
		long startRow, endRow, startCol, endCol;
		double x1, x2, y1, y2;
		
		// introdotte celle rettangolari
		startCol = (long)Math.ceil(Math.abs(mbrGeo.getMinX() - x) / this.tileWidth);
		if (startCol == 0) startCol = 1;
		endCol = (long)Math.ceil(Math.abs(mbrGeo.getMaxX() - x) / this.tileWidth);
		if (endCol == 0) endCol = 1;
		startRow = (long)Math.ceil(Math.abs(mbrGeo.getMinY() - y) / this.tileHeight);
		if (startRow == 0) startRow = 1; 
		endRow = (long)Math.ceil(Math.abs(mbrGeo.getMaxY() - y) / this.tileHeight);
		if (endRow == 0) endRow = 1;
		long id, i = 1;
		for (col = startCol; col <= endCol; col++)
			for (row = startRow; row <= endRow; row++)
			{
				// creo la cella usando gli angoli in basso a sx e in alto a dx.
				// introdotte celle rettangolari
				x1 = this.x + (col-1)*this.tileWidth;
				x2 = x1 + this.tileWidth;
				y1 = this.y + (row-1)*this.tileHeight;
				y2 = y1 + this.tileHeight;
				
				// creo una envelope MBR e poi la converto in geometry
				env = new Envelope(x1, x2, y1, y2);
				cell = shape.getFactory().toGeometry(env);
				
				//System.out.println("cell: "+i++);
				if (shape.intersects(cell)) {
					id = this.getCellId(row, col);
					al.add(id);
				}		
			}
		if (simpl) System.out.println("#cell: "+i);
		
		Long[] a = new Long[al.size()];
		return al.toArray(a);
	}
	
	/**
	 * Il metodo verifica l'intersezione tra una cella della griglia
	 * e la figura passata come parametro. Restituisce un vettore 
	 * per poter essere usato in Map/Reduce. Copiato dalla classe 
	 * GridPartitioner.
	 * 
	 * NOTA: The rectangle is considered open-ended. This means that
	 * the right and top edge are outside the rectangle.
	 * 
	 * @param shape figura
	 * @param l lista delle celle intersecate
	 * @return array con indice id della cella e valore il numero di
	 * sovrapposizioni.
	 
	public int[] overlapPartitionsBis(Shape shape) {
		if (shape == null)
			return null;
		Rectangle shapeMBR =shape.getMBR();
		if (shapeMBR == null)
			return null;
		int col1, col2, row1, row2;
		col1 = (int)Math.floor((shapeMBR.x1 - x) / tileSide);
		col2 = (int)Math.ceil((shapeMBR.x2 - x) / tileSide);
		row1 = (int)Math.floor((shapeMBR.y1 - y) / tileSide);
		row2 = (int)Math.ceil((shapeMBR.y2 - y) / tileSide);
		if (col1 < 0) col1 = 0;
		if (row1 < 0) row1 = 0;
		for (int col = col1; col < col2; col++)
			for (int row = row1; row < row2; row++)
				cells[getCellNumber(col, row)] += 1;
		
		return cells;
	}
	*/

	public long getPartitionCount() {
		return numTiles;
	}

	/*
	 * Calcola il numero di colonna.
	 */
	public long getCoordX(long cellID)
	{
		return (cellID % this.numColumns);
	}

	/*
	 * Calcola il numero di riga.
	 */
	public long getCoordY(long cellID)
	{
		return (cellID / this.numColumns);
	}
	
	/*
	 * Calcola l'identificativo della cella.
	 * 
	 */
	public long getCellId(long row, long col) {
		return ((row-1) * numColumns) + (col-1);
	}

	/*
	 * Calcola la riga dato l'identificativo della cella.
	 */
	public long getRow(long id) {
		return (id/numColumns + 1);
	}
	
	/*
	 * Calcola la colonna dato l'identificativo della cella.
	 */
	public long getCol(long id) {
		return (id%numColumns + 1);
	}
	
	/*
	 * Calcola la distanza tra due celle.
	 */
	public long getDistance(long cell1, long cell2) {
		long r1, r2, c1, c2;
		r1 = getRow(cell1); r2 = getRow(cell2);
		c1 = getCol(cell1); c2 = getCol(cell2);
		if (r1==r2)
			return (long) (c1==c2?0:Math.abs(c1-c2)-1);
		else 
			return (long) ((c1==c2?0:Math.abs(c1-c2)-1)+Math.abs(r1-r2)-1);
	}
	
	/**
	 * @param args
	 */
	@SuppressWarnings("javadoc")
	public static void main(String[] args) {
		
		String mbr = "Rectangle: (-0.01,0.0)-(100.0,86.6125)"; 
		double cs = 100.0;
		Grid griglia = new Grid(mbr, cs);
		System.out.println("Parametri griglia:");
		System.out.println("num_tile: " + griglia.numTiles);
		System.out.println("num_columns: " + griglia.numColumns);
		System.out.println("num_rows: " + griglia.numRows);
		System.out.println("width: " + griglia.width );
		System.out.println("height: " + griglia.height );
		
		int col = 0;
		int row = 0;
		for (; col < griglia.numColumns; col++)
			for (row = 0; row < griglia.numRows; row++)
				System.out.println("col: " + col + " row: " + row );
    	return;
	}

}
