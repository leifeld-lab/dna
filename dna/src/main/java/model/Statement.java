package model;

import java.time.LocalDateTime;
import java.util.ArrayList;

/**
 * Represents a statement. There are two constructors: one for a basic statement
 * version that contains only the data necessary for inserting an empty
 * statement into the database and one with all data necessary for opening a
 * statement popup window.
 */
public class Statement implements Comparable<Statement> {
	private int id;
	private int start, stop;
	private int statementTypeId;
	private String statementTypeLabel;
	private Color statementTypeColor;
	private int coderId;
	private String coderName;
	private Color coderColor;
	private ArrayList<Value> values;
	private int documentId;
	private String text;
	private LocalDateTime dateTime;
	
	/**
	 * Create a statement. Full constructor with all fields.
	 * 
	 * @param id                  The ID of the statement.
	 * @param start               Start position in the text.
	 * @param stop                End position in the text.
	 * @param statementTypeId     Statement type ID.
	 * @param statementTypeLabel  State type label.
	 * @param statementTypeColor  Statement type color.
	 * @param coderId             ID of the coder.
	 * @param coderName           Name of the coder.
	 * @param coderColor          Color of the coder.
	 * @param values              An array list with variable contents.
	 * @param documentId          The document ID.
	 * @param text                The selected text.
	 * @param dateTime            The date and time of the statement.
	 */
	public Statement(int id,
			int start,
			int stop,
			int statementTypeId,
			String statementTypeLabel,
			Color statementTypeColor,
			int coderId,
			String coderName,
			Color coderColor,
			ArrayList<Value> values,
			int documentId,
			String text,
			LocalDateTime dateTime) {
		this.id = id;
		this.start = start;
		this.stop = stop;
		this.statementTypeId = statementTypeId;
		this.statementTypeLabel = statementTypeLabel;
		this.statementTypeColor = statementTypeColor;
		this.coderId = coderId;
		this.coderName = coderName;
		this.coderColor = coderColor;
		this.values = values;
		this.documentId = documentId;
		this.text = text;
		this.dateTime = dateTime;
	}

	/**
	 * Create a new statement. Light version for inserting an empty statement
	 * into the database.
	 * 
	 * @param start            Start position in the text.
	 * @param stop             End position in the text.
	 * @param statementTypeId  Statement type ID.
	 * @param coderId          The ID of the coder who owns the statement.
	 * @param values           An array list with variable contents.
	 * @param documentId       The document ID.
	 */
	public Statement(int start, int stop, int statementTypeId, int coderId, ArrayList<Value> values, int documentId) {
		this.start = start;
		this.stop = stop;
		this.statementTypeId = statementTypeId;
		this.coderId = coderId;
		this.values = values;
		this.documentId = documentId;
	}
	
	/**
	 * Copy constructor. Creates a deep copy/clone of another statement.
	 * 
	 * @param statement  The other statement to clone.
	 */
	public Statement(Statement statement) {
		this.id = statement.getId();
		this.start = statement.getStart();
		this.stop = statement.getStop();
		this.statementTypeId = statement.getStatementTypeId();
		this.statementTypeLabel = statement.getStatementTypeLabel();
		this.statementTypeColor = statement.getStatementTypeColor();
		this.coderId = statement.getCoderId();
		this.coderName = statement.getCoderName();
		this.coderColor = statement.getCoderColor();
		this.values = new ArrayList<Value>();
		for (int i = 0; i < statement.getValues().size(); i++) {
			values.add(new Value(statement.getValues().get(i))); // use copy constructor of Value class
		}
		this.documentId = statement.getDocumentId();
		this.text = statement.getText();
		this.dateTime = statement.getDateTime();
	}

	/**
	 * Get the coder name.
	 * 
	 * @return The coder name.
	 */
	public String getCoderName() {
		return coderName;
	}

	/**
	 * Set the coder name.
	 * 
	 * @param coderName The coder name to set.
	 */
	public void setCoderName(String coderName) {
		this.coderName = coderName;
	}

	/**
	 * Get the coder color.
	 * 
	 * @return The coder color.
	 */
	public Color getCoderColor() {
		return coderColor;
	}

	/**
	 * Set the coder color.
	 * 
	 * @param coderColor The coder color to set.
	 */
	public void setCoderColor(Color coderColor) {
		this.coderColor = coderColor;
	}

	/**
	 * Get the document ID.
	 * 
	 * @return The ID of the document the statement is nested in.
	 */
	public int getDocumentId() {
		return documentId;
	}

	/**
	 * Set the document ID.
	 * 
	 * @param documentId The document ID to set.
	 */
	public void setDocumentId(int documentId) {
		this.documentId = documentId;
	}

	/**
	 * Get the document text portion highlighted by the statement.
	 * 
	 * @return A text portion from the document as a String.
	 */
	public String getText() {
		return text;
	}

	/**
	 * Set the statement text.
	 * 
	 * @param text The text to set.
	 */
	public void setText(String text) {
		this.text = text;
	}

	/**
	 * Get the statement type label.
	 * 
	 * @return The name or label of the statement type associated with the
	 *   statement.
	 */
	public String getStatementTypeLabel() {
		return statementTypeLabel;
	}

	/**
	 * Set the statement type label.
	 * 
	 * @param statementTypeLabel The statement type label to set.
	 */
	public void setStatementTypeLabel(String statementTypeLabel) {
		this.statementTypeLabel = statementTypeLabel;
	}

	/**
	 * Get the color of the statement type associated with the statement.
	 * 
	 * @return The statement type color of the statement.
	 */
	public Color getStatementTypeColor() {
		return statementTypeColor;
	}

	/**
	 * Set the statement type color.
	 * 
	 * @param statementTypeColor The statement type color to set.
	 */
	public void setStatementTypeColor(Color statementTypeColor) {
		this.statementTypeColor = statementTypeColor;
	}

	/**
	 * Get the statement ID.
	 * 
	 * @return The ID of the statement.
	 */
	public int getId() {
		return id;
	}
	
	/**
	 * Set the statement ID.
	 * 
	 * @param id The ID to set.
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * Get the ID of the coder who owns the statement.
	 * 
	 * @return The coder ID of the statement.
	 */
	public int getCoderId() {
		return coderId;
	}
	
	/**
	 * Set the coder ID of the statement.
	 * 
	 * @param coderId The new coder ID for the statement.
	 */
	public void setCoderId(int coderId) {
		this.coderId = coderId;
	}
	
	/**
	 * Get the start coordinate of the statement in the document.
	 * 
	 * @return The character start position in the text.
	 */
	public int getStart() {
		return start;
	}
	
	/**
	 * Set the start coordinate of the statement in the document.
	 * 
	 * @param start The new character start position in the text.
	 */
	public void setStart(int start) {
		this.start = start;
	}

	/**
	 * Get the end coordinate of the statement in the document.
	 * 
	 * @return The character end position in the text.
	 */
	public int getStop() {
		return stop;
	}

	/**
	 * Set the end coordinate of the statement in the document.
	 * 
	 * @param stop The new character end position in the text.
	 */
	public void setStop(int stop) {
		this.stop = stop;
	}
	
	/**
	 * Get the statement type ID of the statement.
	 * 
	 * @return The statement type ID.
	 */
	public int getStatementTypeId() {
		return statementTypeId;
	}
	
	/**
	 * Get the date/time stored in the statement.
	 * 
	 * @return The date/time as a {@link LocalDateTime} object.
	 */
	public LocalDateTime getDateTime() {
		return dateTime;
	}

	/**
	 * Set the date/time of the corresponding document.
	 * 
	 * @param dateTime The new date/time to set.
	 */
	public void setDateTime(LocalDateTime dateTime) {
		this.dateTime = dateTime;
	}
	
	/**
	 * Get the values of the variables stored in the statement.
	 * 
	 * @return An array list of {@link model.Value Value} objects
	 *   corresponding to the variables specified in the corresponding statement
	 *   type.
	 */
	public ArrayList<Value> getValues() {
		return values;
	}
	
	/**
	 * Set the values of the variables stored in the statement.
	 * 
	 * @param values An array list of {@link model.Value Value} objects
	 *   corresponding to the variables specified in the corresponding statement
	 *   type.
	 */
	public void setValues(ArrayList<Value> values) {
		this.values = values;
	}

	/**
	 * Get a value from the array list of values by reference to its variable name.
	 *
	 * @param key Name of the variable for which to retrieve the {@link Value} object.
	 * @return The value.
	 */
	public Value getValueByKey(String key) {
		return this.values.stream().filter(v -> v.getKey().equals(key)).findFirst().get();
	}

	/**
	 * Get a value from the array list of values by reference to its variable ID.
	 *
	 * @param variableId ID of the variable for which to retrieve the {@link Value} object.
	 * @return The value.
	 */
	public Value getValueByVariableId(int variableId) {
		return this.values.stream().filter(v -> v.getVariableId() == variableId).findFirst().get();
	}

	/**
	 * Set the value of a {@link Value} object contained in the values array list. The correct item in the list is
	 * identified by matching the variable ID. The value slot stored in the new value is used to replace the value in
	 * the object stored in the list.
	 *
	 * @param value The value to replace.
	 */
	public void setValue(Value value) {
		this.values.stream().filter(v -> v.getVariableId() == value.getVariableId()).findFirst().get().setValue(value.getValue());
	}

	/**
	 * Implementation of the {@link java.lang.Comparable Comparable} interface
	 * to sort statements in the statement table and possibly elsewhere.
	 */
	@Override
	public int compareTo(Statement s) {
		if (this.dateTime != null) {
			if (this.getDateTime().isBefore(s.getDateTime())) {
				return -1;
			} else if (this.getDateTime().isAfter(s.getDateTime())) {
				return 1;
			}
		}
		if (this.getDocumentId() < s.getDocumentId()) {
			return -1;
		} else if (this.getDocumentId() > s.getDocumentId()) {
			return 1;
		}
		if (this.getStart() < s.getStart()) {
			return -1;
		} else if (this.getStart() > s.getStart()) {
			return 1;
		}
		if (this.getStop() < s.getStop()) {
			return -1;
		} else if (this.getStop() > s.getStop()) {
			return 1;
		}
		if (this.getId() < s.getId()) {
			return -1;
		} else {
			return 1;
		}
	}
}