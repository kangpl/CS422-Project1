package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;


public class RowStore extends Store {

	// TODO: Add required structures
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private ArrayList<DBTuple> tableTuple;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tableTuple = new ArrayList<DBTuple>();
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement
		Path pathToFile = Paths.get(filename);
		
		// create an instance of BufferedReader
		try (BufferedReader br = Files.newBufferedReader(pathToFile)){ 
			
			// read the first line from the text file 
			String line = br.readLine(); 
			
			// loop until all lines are read 
			while (line != null) { 
				
				// use string.split to load a string array with the values from 
				String[] attributes = line.split(delimiter); 
				DBTuple tuple = createTuple(attributes, schema); 
				
				// adding tuple into ArrayList
				tableTuple.add(tuple);
				
				// read next line before looping 
				// if end of file reached, line would be null 
				line = br.readLine(); 
			}
		}catch (IOException ioe) {
			System.err.println(ioe);
		}
		
	}
	
	private static DBTuple createTuple(String[] attributes, DataType[] schema) {
		Object[] fields = new Object[schema.length];
		
		//Change from string to different dataType according to schema
		for(int i = 0; i < attributes.length; i++) {
			switch(schema[i]) {
			case INT:
				fields[i] = Integer.parseInt(attributes[i]);
				break;
			case DOUBLE:
				fields[i] = Double.parseDouble(attributes[i]);
				break;
			case BOOLEAN:
				fields[i] = Boolean.parseBoolean(attributes[i]);
				break;
			case STRING:
				fields[i] = attributes[i];
				break;
			default:
				fields[i] = attributes[i];
				System.out.println("Something wrong with the datatype schema!");
			}				
			
		}
//		for(Object elem : fields) {
//			System.out.println(elem);
//		}
		//Create new tuple according to fields and schema
		DBTuple tuple = new DBTuple(fields, schema);
		return tuple;
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		try {
			DBTuple tuple = tableTuple.get(rownumber);
			return tuple;
		} catch (IndexOutOfBoundsException e) {
			System.err.println(e);
			return new DBTuple();
		}
		
	}
}
