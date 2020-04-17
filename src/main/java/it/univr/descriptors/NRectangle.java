package it.univr.descriptors;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class NRectangle {
	protected ArrayList<Double> coordMin;
	protected ArrayList<Double> coordMax;
	protected ArrayList<Double> size;
	protected int dim;
	protected boolean isValid;
	
	public int getDim () {
		return dim;
	}
	
	public Double getCoordMin (int i) {
		if (i>dim) return null; else return coordMin.get(i);
	}

	public void setCoordMin (int i, double d) {
		if (i<dim) coordMin.set(i, d);
	}
	
	public Double getCoordMax (int i) {
		if (i>dim) return null; else return coordMax.get(i);
	}
	
	public void setCoordMax (int i, double d) {
		if (i<dim) coordMax.set(i, d);
	}
	
	public Double getSize (int i) {
		if (i>dim) return null; else return size.get(i);
	}
	
	public Boolean isInside2D (double xmin, double ymin, double xmax, double ymax) {
		if (coordMin.get(0) < xmin || coordMin.get(1) < ymin ||
				coordMax.get(0) > xmax || coordMax.get(1) > ymax)
			return false;
		else
			return true;
	}
	
	public Boolean isInside3D (double xmin, double ymin, double zmin, double xmax, double ymax, double zmax) {
		if (coordMin.get(0) < xmin || coordMin.get(1) < ymin || coordMin.get(2) < zmin ||
				coordMax.get(0) > xmax || coordMax.get(1) > ymax || coordMax.get(2) > zmax)
			return false;
		else
			return true;
	}
	
	public String print() {
		if (dim==2)
			return coordMin.get(0).toString()+","+coordMin.get(1).toString()+","+
				   coordMax.get(0).toString()+","+coordMax.get(1).toString();
		else {
			String s = new String();
			for (int i=0; i<dim; i++) 
				s += coordMin.get(i).toString()+",";
			for (int i=0; i<dim; i++) 
				s += coordMax.get(i).toString()+(i==(dim-1)?"":",");
			return s;
		}
	}
	
	public String printWKT2D() {
		if (dim==2)
			return "POLYGON(("+coordMin.get(0).toString()+" "+coordMin.get(1).toString()+", "+
				   			   coordMax.get(0).toString()+" "+coordMin.get(1).toString()+", "+
				   			   coordMax.get(0).toString()+" "+coordMax.get(1).toString()+", "+
				   			   coordMin.get(0).toString()+" "+coordMax.get(1).toString()+", "+
				   			coordMin.get(0).toString()+" "+coordMin.get(1).toString()+"))";
		else
			return "";
	}
	
	public NRectangle(String value) {
		//Pattern p = Pattern.compile("(-?\\d+[.|,]\\d+)"); //originale
		StringTokenizer st = new StringTokenizer(value,",");
		
		ArrayList<Double> al = new ArrayList<Double>();
		//Matcher m = p.matcher(value);
		while (st.hasMoreTokens()) {
			String tmp = st.nextToken();
			//tmp = tmp.replace(',', '.');
			al.add(Double.parseDouble(tmp));
		}
		
		if (al.size() % 2 != 0) {
			this.isValid = false;
			return;
		}
		
		this.dim = al.size()/2;
		this.coordMin = new ArrayList<Double>();
		this.coordMax = new ArrayList<Double>();
		this.size = new ArrayList<Double>();
		Double diff;
		
		for(int i=0; i<this.dim; i++) {
			this.coordMin.add(al.get(i));
			this.coordMax.add(al.get(i+this.dim));
			diff = al.get(i+this.dim)-al.get(i);
			if (diff < 0) {
				this.isValid = false;
				return;
			}
			this.size.add(diff);
		}
		
		this.isValid = true;
	}

	public double getNVol() {
		double a = 1.0;
		for (int i=0; i<dim; i++)
			a *= (this.coordMax.get(i) - this.coordMin.get(i));
		return a;
	}
}