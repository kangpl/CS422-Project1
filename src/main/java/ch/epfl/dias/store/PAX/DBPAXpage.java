package ch.epfl.dias.store.PAX;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

	// TODO: Implement
	public DBColumn[] PAXpage;
	public DataType[] types;

	public DBPAXpage(DBColumn[] PAXpage, DataType[] types) {
		this.PAXpage = PAXpage;
		this.types = types;
	}
	
	public DBTuple getTuple(int rowNumber) {
		Object[] fields = new Object[PAXpage.length];
		//if the content is null means
		//rowNumber out of bound 
		//return eof DBTuple
		if(PAXpage[0].fields[rowNumber] == null) {
			return new DBTuple();
		}
		for(int i = 0; i < PAXpage.length; i++) {
			fields[i] = PAXpage[i].fields[rowNumber];
		}
		return new DBTuple(fields, types);
	}
}
