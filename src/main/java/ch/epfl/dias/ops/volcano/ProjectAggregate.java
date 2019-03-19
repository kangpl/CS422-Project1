package ch.epfl.dias.ops.volcano;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	
	private int intSum;
	private int intMin;
	private int intMax;
	private double doubleSum;
	private double doubleMin;
	private double doubleMax;
	private int count;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
		
		intSum = 0;
		intMin = Integer.MAX_VALUE;
		intMax = Integer.MIN_VALUE;
		doubleSum = 0.0;
		doubleMin = Double.MAX_VALUE;
		doubleMax = Double.MIN_VALUE;
		count = 0;
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
			
		calAggregate();
		
		return getAggregate();
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}

	private void calAggregate() {
		DBTuple tuple = child.next();
		while(!tuple.eof) {
			switch(agg) {
			case COUNT:
				count += 1;
				break;
			case SUM:
				switch(dt) {
				case INT:
					int intValue = tuple.getFieldAsInt(fieldNo);
					intSum += intValue;
					break;
				case DOUBLE:
					double doubleValue = tuple.getFieldAsDouble(fieldNo);
					doubleSum += doubleValue;
					break;
				default:
					break;
				}
				break;
			case MIN:
				switch(dt) {
				case INT:
					int intValue = tuple.getFieldAsInt(fieldNo);
					intMin = intValue < intMin ? intValue : intMin;
					break;
				case DOUBLE:
					double doubleValue = tuple.getFieldAsDouble(fieldNo);
					doubleMin = doubleValue < doubleMin ? doubleValue : doubleMin;
					break;
				default:
					break;
				}
				break;
			case MAX:
				switch(dt) {
				case INT:
					int intValue = tuple.getFieldAsInt(fieldNo);
					intMax = intValue > intMax ? intValue : intMax;
					break;
				case DOUBLE:
					double doubleValue = tuple.getFieldAsDouble(fieldNo);
					doubleMax = doubleValue > doubleMax ? doubleValue : doubleMax;
					break;
				default:
					break;
				}
				break;
			case AVG:
				count += 1;
				switch(dt) {
				case INT:
					int intValue = tuple.getFieldAsInt(fieldNo);
					intSum += intValue;
					break;
				case DOUBLE:
					double doubleValue = tuple.getFieldAsDouble(fieldNo);
					doubleSum += doubleValue;
					break;
				default:
					break;
				}
				break;
			default:
				break;
			}
			tuple = child.next();
		}
	}
	
	private DBTuple getAggregate() {
		switch(agg) {
		case COUNT:
			return new DBTuple(new Object[] {count}, new DataType[] {dt});
		case SUM:
			switch(dt) {
			case INT:
				return new DBTuple(new Object[] {intSum}, new DataType[] {dt});
			case DOUBLE:
				return new DBTuple(new Object[] {doubleSum}, new DataType[] {dt});
			}
		case MIN:
			switch(dt) {
			case INT:
				return new DBTuple(new Object[] {intMin}, new DataType[] {dt});
			case DOUBLE:
				return new DBTuple(new Object[] {doubleMin}, new DataType[] {dt});
			}
		case MAX:
			switch(dt) {
			case INT:
				return new DBTuple(new Object[] {intMax}, new DataType[] {dt});
			case DOUBLE:
				return new DBTuple(new Object[] {doubleMax}, new DataType[] {dt});
			}
		case AVG:
			switch(dt) {
			case INT:
				return new DBTuple(new Object[] {(double) intSum / (double) count}, new DataType[] {dt});
			case DOUBLE:
				return new DBTuple(new Object[] {(double) doubleSum / (double) count}, new DataType[] {dt});
			}
		default:
			return new DBTuple();	
		}
	}
}
