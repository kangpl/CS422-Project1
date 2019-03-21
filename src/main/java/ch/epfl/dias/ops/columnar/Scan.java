package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnId;

public class Scan implements ColumnarOperator {

	// TODO: Add required structures
	public ColumnStore store;

	public Scan(ColumnStore store) {
		// TODO: Implement
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] wholeTable = store.tableColumn.toArray(new DBColumn[0]);
		if(store.lateMaterialization == false) {
			return wholeTable;	
		}
		
		//do late materializtion return idsTable
		else {
			//initialize an id table
			DBColumnId[] idsWholeTable = new DBColumnId[wholeTable.length];
			
			//create ids
			int[] ids = new int[wholeTable[0].fields.length];
			for(int i = 0; i < ids.length; i++) {
				ids[i] = i;
			}
			
			for(int i = 0; i < idsWholeTable.length; i++) {
				DBColumnId idColumn = new DBColumnId(wholeTable[i].fields, wholeTable[i].type, true, ids);
				idsWholeTable[i] = idColumn;
			}
			
			return idsWholeTable;
		}
	}
}