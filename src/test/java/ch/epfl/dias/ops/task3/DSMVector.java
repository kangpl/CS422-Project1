package ch.epfl.dias.ops.task3;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class DSMVector {
	DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;
    
    @Before
    public void init() throws IOException  {
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
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
        
        columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
        columnstoreOrder.load();
        
        columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        columnstoreLineItem.load();        
    }
    
	@Test
	public void query1(){
		/* SELECT L_ORDERKEY L_SHIPDATE FROM lineitem L WHERE L_ORDERKEY > 666666*/    
	    ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 10000);
	    ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GT, 0, 666666);
	    ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[] {0, 10});
		
		// This query should return only one result
	    proj.open();
	    int outputField0 = 0;
	    String outputField1 = null;
		DBColumn[] result = proj.next();
		while(result[0].fields.length != 0) {
			outputField0 = result[0].getAsInteger()[result[0].getAsInteger().length - 1];
			outputField1 = result[1].getAsString()[result[1].getAsString().length -1];
			result = proj.next();
		}
		proj.close();
        System.out.println(outputField0);
        System.out.println(outputField1);
        assertTrue(outputField0 == 999939);
	}
	
	@Test
    public void query2(){
        /* SELECT L_PARTKEY, O_CUSTKEY FROM lineitem L, order O WHERE (L_ORDERKEY=O_ORDERKEY)*/
		
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 10000);
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 10000);
    
        // Projection
        ch.epfl.dias.ops.vector.Project projLineItem = new ch.epfl.dias.ops.vector.Project(scanLineitem, new int[]{0,1});
        ch.epfl.dias.ops.vector.Project projOrder = new ch.epfl.dias.ops.vector.Project(scanOrder, new int[]{0,1});

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(projLineItem ,projOrder, 0, 0);
    
        ch.epfl.dias.ops.vector.Project projFinal = new ch.epfl.dias.ops.vector.Project(join, new int[]{1,3});

        projFinal.open();
        DBColumn[] result = projFinal.next();
        int outputLineitem = 0;
        int outputOrder = 0;
		while(result[0].fields.length != 0) {
			outputLineitem = result[0].getAsInteger()[result[0].getAsInteger().length - 1];
			outputOrder = result[1].getAsInteger()[result[1].getAsInteger().length -1];
			result = projFinal.next();
		}
		projFinal.close();
        System.out.println(outputLineitem);
        System.out.println(outputOrder);
        assertTrue(outputLineitem == 118515);
        assertTrue(outputOrder == 41275);
    }
	
	@Test
    public void query3(){
        /* SELECT AVG(L_DISCOUNT) FROM lineitem */	    
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 10000);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 6);
        
        agg.open();
        // This query should return only one result
        DBColumn[] result = agg.next();
        double output = result[0].getAsDouble()[0];
        agg.close();
        System.out.println(output);
        output = 0.05006691999986567;
        assertTrue(output == 0.05006691999986567);
    }
}
