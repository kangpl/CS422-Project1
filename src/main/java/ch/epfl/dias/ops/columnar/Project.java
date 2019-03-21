package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnId;

public class Project implements ColumnarOperator {

	// TODO: Add required structures
	public ColumnarOperator child;
	public int[] columns;

	public Project(ColumnarOperator child, int[] columns) {
		// TODO: Implement
		this.child  = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] oldTable = child.execute();
		
		//check about materialization
		if(oldTable[0].lateMaterialization == false) {
			//materialize data
			DBColumn[] newTable = new DBColumn[columns.length];
			int index = 0;
			for(int elem : columns) {
				newTable[index] = oldTable[elem];
				index++;
			}
			return newTable;
		}
		else {
			//late materialization return ids Table
			DBColumnId[] newTable = new DBColumnId[columns.length];
			int index = 0;
			for(int elem : columns) {
				newTable[index] = (DBColumnId) oldTable[elem];
				index++;
			}
			return newTable;
		}
		
	}
}
