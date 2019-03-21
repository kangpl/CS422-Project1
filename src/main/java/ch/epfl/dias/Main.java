package ch.epfl.dias;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnId;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

public class Main {

	public static void main(String[] args) throws IOException {

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };
		
		DataType[] lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
//		for(DataType type : DataType.values()) {
//			System.out.println(type);
//		}

//		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
//		rowstore.load();
//		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstore);
//		DBTuple tuple = scanOrder.next();
//		int count = 0;
//		while(!tuple.eof) {
//			for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//			tuple = scanOrder.next();
//			count++;
//		}
//		System.out.println(count);
		
//		PAXStore rowstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 1);
//		rowstore.load();
//		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstore);
//		DBTuple tuple = scanOrder.next();
//		int count = 0;
//		while(!tuple.eof) {
//			for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//			System.out.println("========");
//			tuple = scanOrder.next();
//			count++;
//		}
//		System.out.println(count);
		
		
//		RowStore rowstoreData = new RowStore(schema, "input/data.csv", ",");
//        rowstoreData.load();
//        
//        RowStore rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
//        rowstoreOrder.load();
//        
//	    ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
//	    ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//	    scan.open();
//	    DBTuple tuple = scan.next();
//	    for(int i = 0; i < tuple.fields.length; i++) {
//			System.out.println(tuple.fields[i]);
//		}
//	    tuple = scan.next();
//	    for(int i = 0; i < tuple.fields.length; i++) {
//			System.out.println(tuple.fields[i]);
//		}
//	    scan.close();
//	    ch.epfl.dias.ops.volcano.Select select = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 32);
//	    select.open();
//	    tuple = select.next();
//	    if(tuple.eof) {
//	    	System.out.println("this is the end of tableTuple");
//	    }else {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    }
//	    tuple = select.next();
//	    if(tuple.eof) {
//	    	System.out.println("this is the end of tableTuple");
//	    }else {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    }
//	    select.close();
	  
	    
//	    int[] fieldNo = {0, 1, 8};
//	    ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(scan, fieldNo);
//	    project.open();
//	    DBTuple tuple = project.next();
//	    while(!tuple.eof) {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    	tuple = project.next();
//	    }
//		project.close();
	   
	    
//	    ch.epfl.dias.ops.volcano.HashJoin hashJoin = new ch.epfl.dias.ops.volcano.HashJoin(scanOrder, scanData, 0, 0);
//		hashJoin.open();
//	    DBTuple tuple = hashJoin.next();
//	    while(!tuple.eof) {
////	    	for(int i = 0; i < tuple.fields.length; i++) {
////				System.out.println(tuple.fields[i]);
////			}
//	    	tuple = hashJoin.next();
//	    }
//		hashJoin.close();
//		
//		hashJoin.open();
//		tuple = hashJoin.next();
//	    while(!tuple.eof) {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    	tuple = hashJoin.next();
//	    }
//		hashJoin.close();
	    
//	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scanOrder, Aggregate.AVG, DataType.INT, 0);
//	    agg.open();  
//	    DBTuple tuple = agg.next();
//	    if(!tuple.eof) {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    }
//		agg.close();
//	    ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",", true);
//		columnstoreData.load();
		
		
		
//		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",", false);
//		
//		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
//		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
//		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
//				DataType.INT, 2);
//
//		DBColumn[] result = agg.execute();
//		for(DBColumn ids : result) {
//		Integer[] idsColumn = ids.getAsInteger();
//		for(int i : idsColumn) {
//			System.out.println(i);
//		}
//		System.out.println("========");
//		}
		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",", true);
		columnstoreData.load();
		
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

//		DBColumn[] result = sel.execute();

//		// This query should return only one result
//		int output = result[0].getAsInteger()[0];
//		System.out.println(output);
		
//		ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|", true);
//		columnstoreData.load();
//		columnstoreOrder.load();
//		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
//		ch.epfl.dias.ops.columnar.Scan scanData = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
//		
//		ch.epfl.dias.ops.columnar.Select selectOrder = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.GT, 0, 5);
//		int[] columnsToGet = {0, 2, 3};
//		ch.epfl.dias.ops.columnar.Project project = new ch.epfl.dias.ops.columnar.Project(scanOrder, columnsToGet);
//		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scanData, scanOrder, 3, 0);
//		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scanOrder, Aggregate.COUNT, DataType.INT, 0);
////		DBColumnId[] idsSet = (DBColumnId[]) selectOrder.execute();
//		
////		DBColumnId[] idsSet = (DBColumnId[])project.execute();
		DBColumnId[] idsSet = (DBColumnId[])agg.execute();
		for(DBColumnId ids : idsSet) {
			int[] idsColumn = ids.ids;
			for(int i : idsColumn) {
				System.out.println(i);
			}
			System.out.println("========");
		}
		System.out.println("**************************");
		DBColumn[] idsSet2 = agg.execute();
		for(DBColumn ids : idsSet2) {
			Object[] idsColumn = ids.getAsInteger();
			for(Object i : idsColumn) {
				System.out.println(i);
			}
			System.out.println("========");
		}
		
//		int[] columnsToGet = {0, 2, 3};
//		DBColumn[] columnSet = columnstoreOrder.getColumns(columnsToGet);
//		for(int i = 0; i < columnSet.length; i++) {
//			Object[] column = columnSet[i].fields;
//			DataType type = columnSet[i].type;
//			System.out.println(type);
//			for(int j = 0; j < column.length; j++) {
//				System.out.println(column[j]);
//			}
//		}
//		
//		int[] idsToGet = {0, 2, 3};
//		DBColumn[] idsSet = columnstoreOrder.getIdsOfColumn(idsToGet);
//		for(int i = 0; i < idsSet.length; i++) {
//			Object[] column = idsSet[i].ids;
//			for(int j = 0; j < column.length; j++) {
//				System.out.println(column[j]);
//			}
//		}
		
//		int[] columnsToGet = {0, 2, 3};
//		ch.epfl.dias.ops.columnar.Scan scanData = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
//		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
//		DBColumn[] idsSet = scanOrder.execute();
//		for(int i = 0; i < idsSet.length; i++) {
//			Object[] column = idsSet[i].fields;
//			for(int j = 0; j < column.length; j++) {
//				System.out.println(column[j]);
//			}
//			System.out.println("=======");
//		}
//		ch.epfl.dias.ops.columnar.Select select = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.GT, 0, 32);
//		ch.epfl.dias.ops.columnar.Project project = new ch.epfl.dias.ops.columnar.Project(scanOrder, columnsToGet);
//		DBColumn[] columnSet = project.execute();
//
//		for(int i = 0; i < columnSet.length; i++) {
//			Object[] column = columnSet[i].fields;
//			DataType type = columnSet[i].type;
////			System.out.println(type);
//			for(int j = 0; j < column.length; j++) {
//				System.out.println(column[j]);
//			}
//		}
//		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scanOrder, scanData, 0, 3);
//		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scanOrder, Aggregate.AVG, DataType.DOUBLE, 3);
//		DBColumn[] columnSet = agg.execute();
//
//		for(int i = 0; i < columnSet.length; i++) {
//			Object[] column = columnSet[i].fields;
//			DataType type = columnSet[i].type;
//			System.out.println(type);
//			for(int j = 0; j < column.length; j++) {
//				System.out.println(column[j]);
//			}
//		}


		// PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		// paxstore.load();
		 
		// ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore);
		// DBTuple currentTuple = scan.next();
		// while (!currentTuple.eof) {
		// 	System.out.println(currentTuple.getFieldAsInt(1));
		// 	currentTuple = scan.next();
		// }

		// ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		// columnstoreData.load();
		//
		// ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		// ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
		// ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		// DBColumn[] result = agg.execute();
		// int output = result[0].getAsInteger()[0];
		// System.out.println(output);
	}
}
