package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	// TODO: Add required structures
	public Store store;
	public int vectorsize;
	public int vectorIndex;
	public DBColumn[] wholeTable;
	

	public Scan(Store store, int vectorsize) {
		// TODO: Implement
		this.store = store;
		this.vectorsize = vectorsize;	
	}
	
	@Override
	public void open() {
		// TODO: Implement
		vectorIndex = 0;	
		wholeTable = store.getColumns(new int[] {});	
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		if(vectorIndex >= wholeTable[0].fields.length) {
			return new DBColumn[] {new DBColumn(new Object[] {}, DataType.INT)};
		}
		else {
			DBColumn[] vectorColumn = new DBColumn[wholeTable.length];
			for(int i = 0; i < wholeTable.length; i++) {
				DBColumn column = wholeTable[i];
				Object[] columnData = column.fields;
				Object[] vectorColumnData;
				if(vectorIndex + vectorsize > columnData.length) {
					vectorColumnData = new Object[columnData.length - vectorIndex];
				}
				else {
					vectorColumnData = new Object[vectorsize];
				}
				for(int j = 0; j < vectorColumnData.length; j++) {
					vectorColumnData[j] = columnData[j + vectorIndex];
				}
				vectorColumn[i] = new DBColumn(vectorColumnData, column.type);
			}
			vectorIndex += vectorsize;
			return vectorColumn;
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		vectorIndex = 0;
		wholeTable = null;
	}
}
