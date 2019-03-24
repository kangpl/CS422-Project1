package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	// TODO: Add required structures
	public VectorOperator child;
	public int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		// TODO: Implement
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] vectorColumn = child.next();
		if(vectorColumn[0].fields.length == 0) {
			return vectorColumn;
		}else {
			DBColumn[] newVectorColumn = new DBColumn[fieldNo.length];
			for(int i = 0; i < fieldNo.length; i++) {
				newVectorColumn[i] = vectorColumn[fieldNo[i]];
			}
			
			return newVectorColumn;
		}	
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
}
