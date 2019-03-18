package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ColumnStore extends Store {

	// TODO: Add required structures
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private boolean lateMaterialization;
	private List<DBColumn> tableColumn = new ArrayList<DBColumn>();

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.lateMaterialization = lateMaterialization;	
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement
		
		//initialize an two-dimensional array to load primary data
		List<List<Object>> primaryData = new ArrayList<List<Object>>(schema.length);
		for(int i = 0; i < schema.length; i++) {
			primaryData.add(new ArrayList<Object>());
		}
		
		//get the path of the dataset file
		Path pathToFile = Paths.get(filename);
		
		
		// create an instance of BufferedReader
		try (BufferedReader br = Files.newBufferedReader(pathToFile)){ 
			
			// read the first line from the text file 
			String line = br.readLine(); 
			
			// loop until all lines are read 
			while (line != null) { 
				
				// use string.split to load a string array
				String[] attributes = line.split(delimiter); 
				
				//change from string to dataType according to schema
				for(int i = 0; i < attributes.length; i++) {
					primaryData.get(i).add(changeType(attributes[i], schema[i]));
					
				}
				
				// read next line before looping 
				// if end of file reached, line would be null 
				line = br.readLine(); 
			}
		}catch (IOException ioe) {
			System.err.println(ioe);
		}
		
		//convert from two-dimensional ArrayList to tableColumn
		convertToTable(primaryData);
		
		for(int i = 0; i < tableColumn.size(); i++) {
			Object[] column = tableColumn.get(i).fields;
			DataType type = tableColumn.get(i).type;
			System.out.println(type);
			for(int j = 0; j < column.length; j++) {
				System.out.println(column[j]);
			}
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// TODO: Implement
		DBColumn[] columnSet = new DBColumn[columnsToGet.length];
		int index = 0;
		for(int columnNum : columnsToGet) {
			columnSet[index] = tableColumn.get(columnNum);
			index ++;
		}
		
		return columnSet;
	}
	
	private Object changeType(String attribute, DataType type) {
		
		Object field = null;
		//Change from string to different dataType according to parameter type
		switch(type) {
		case INT:
			field = Integer.parseInt(attribute);
			break;
		case DOUBLE:
			field = Double.parseDouble(attribute);
			break;
		case BOOLEAN:
			field = Boolean.parseBoolean(attribute);
			break;
		case STRING:
			field = attribute;
			break;
		default:
			field = attribute;
			System.out.println("Something wrong with the datatype!");			
		
		}

		return field;
	}
	
	private void convertToTable(List<List<Object>> primaryData) {
		for(int i = 0; i < primaryData.size(); i++) {
			Object[] column = primaryData.get(i).toArray();
			DBColumn perDBColumn  = new DBColumn(column, schema[i]);
			tableColumn.add(perDBColumn);
		}
	}
}
