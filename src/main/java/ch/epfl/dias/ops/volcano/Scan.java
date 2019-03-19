package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures
	private Store store;
	private int tupleIndex;

	public Scan(Store store) {
		// TODO: Implement
		this.store = store;
	}

	@Override
	public void open() {
		// TODO: Implement
		tupleIndex = 0;
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		DBTuple tuple = store.getRow(tupleIndex);
		tupleIndex++;
		return tuple;
	}

	@Override
	public void close() {
		// TODO: Implement
		tupleIndex = 0;
	}
}