package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
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
	public DBTuple next() {
		// TODO: Implement
		DBTuple tuple = child.next();
		Object[] newFields = new Object[fieldNo.length];
		DataType[] newTypes = new DataType[fieldNo.length];
		if (!tuple.eof) {
			for(int i = 0; i < fieldNo.length; i++) {
				newFields[i] = tuple.fields[fieldNo[i]];
				newTypes[i] = tuple.types[fieldNo[i]];
			}
			return new DBTuple(newFields, newTypes);
			
		} else {
			return tuple;
		}		
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
}
