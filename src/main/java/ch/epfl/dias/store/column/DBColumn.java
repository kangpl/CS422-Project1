package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// TODO: Implement
	public Object[] fields;
	public DataType type;

	public DBColumn(Object[] fields, DataType type) {
		this.fields = fields;
		this.type = type;
	}
	
	public Integer[] getAsInteger() {
		// TODO: Implement
		return null;
	}
}
