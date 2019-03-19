package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
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
		while (!tuple.eof) {
			Integer tupleFieldValue = tuple.getFieldAsInt(fieldNo);
			boolean comResult = compare(tupleFieldValue);
			if(comResult) {
				return tuple;
			}
			tuple = child.next();
		}
		return tuple;
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
	
	private boolean compare(Integer tupleFieldValue) {
		switch(op) {
		case LT:
			return (tupleFieldValue < value) ? true : false;
		case LE:
			return (tupleFieldValue <= value) ? true : false;
		case EQ:
			return (tupleFieldValue == value) ? true : false;
		case NE:
			return (tupleFieldValue != value) ? true : false;
		case GT:
			return (tupleFieldValue > value) ? true : false;
		case GE:
			return (tupleFieldValue >= value) ? true : false;
		default:
			return false;
		}
	}
}
