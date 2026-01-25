package model;

import java.util.HashMap;

/**
 * Represents an entity, including a hash map containing the attribute values of
 * the entity. An entity is a value including a color and attributes. Entities
 * are displayed in short text fields in the statement popup windows in DNA.
 */
public class Entity implements Comparable<Entity> {
	/**
	 * The ID of the entity.
	 */
	private int id;
	
	/**
	 * The ID of another entity of which this current entity is a child. This
	 * field is currently not in use.
	 */
	private int childOf;
	
	/**
	 * The ID of the variable of which the entity is an instance.
	 */
	private int variableId;
	
	/**
	 * The String value for the entity. This is the value that is displayed in
	 * the statement popup window for the variable.
	 */
	private String value;
	
	/**
	 * The color of the entity.
	 */
	private Color color;
	
	/**
	 * Is the entity used in any statement in the database (true), or is this an
	 * unused entity, which is shown in red in the attribute manager (false)?
	 */
	private boolean inDatabase;
	
	/**
	 * A hash map of attribute variable names with their attribute values.
	 */
	private HashMap<String, String> attributeValues;

	/**
	 * The full constructor for this class. Creates a new Entity.
	 * 
	 * @param id               The ID of the entity.
	 * @param variableId       The ID of the variable.
	 * @param value            The value of the entity.
	 * @param color            The color of the entity.
	 * @param childOf          Another Entity ID of which this one is a child.
	 * @param inDatabase       Is the entity currently in use in a statement?
	 * @param attributeValues  Hash map of attribute variables (key) and values.
	 */
	public Entity(int id, int variableId, String value, Color color, int childOf, boolean inDatabase, HashMap<String, String> attributeValues) {
		this.id = id;
		this.variableId = variableId;
		this.childOf = childOf;
		this.value = value;
		this.color = color;
		this.inDatabase = inDatabase;
		this.attributeValues = (attributeValues == null) ? new HashMap<>() : new HashMap<>(attributeValues);
	}

	/**
	 * A minimal constructor for this class to create a new entity.
	 * 
	 * @param value  The value of the entity.
	 */
	public Entity(int id, int variableId, String value, Color color) {
		this.value = value;
		this.id = id;
		this.variableId = variableId;
		this.color = color;
		this.inDatabase = true;
		this.attributeValues = new HashMap<>();
	}

	/**
	 * A minimal constructor for this class to create a new entity.
	 * 
	 * @param value  The value of the entity.
	 */
	public Entity(String value) {
		this.value = value;
		this.id = -1;
		this.variableId = -1;
		this.color = new Color(0, 0, 0);
		this.inDatabase = false;
		this.attributeValues = new HashMap<>();
	}

	/**
	 * Is the entity in use in at least one statement in the database?
	 * 
	 * @return  A boolean indicating if the entity is in use in the database.
	 */
	public boolean isInDatabase() {
		return inDatabase;
	}

	/**
	 * Set the inDatabase field.
	 * 
	 * @param inDatabase  A boolean indicating if the entity is in use.
	 */
	public void setInDatabase(boolean inDatabase) {
		this.inDatabase = inDatabase;
	}

	/**
	 * Get the entity ID.
	 * 
	 * @return  The ID of the entity.
	 */
	public int getId() {
		return id;
	}

	/**
	 * Set the entity ID.
	 * 
	 * @param id The ID of the entity.
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * Get the variable ID.
	 * 
	 * @return  The ID of the variable associated with the entity.
	 */
	public int getVariableId() {
		return this.variableId;
	}

	/**
	 * Set the variable ID.
	 * 
	 * @param variableId  The variable ID.
	 */
	public void setVariableId(int variableId) {
		this.variableId = variableId;
	}

	/**
	 * Get the childOf field of the entity.
	 * 
	 * @return  The ID of the entity of which the current entity is a child.
	 */
	public int getChildOf() {
		return childOf;
	}

	/**
	 * Set the childOf field of the entity.
	 * 
	 * @param childOf  The ID of the other entity of which this one is a child.
	 */
	public void setChildOf(int childOf) {
		this.childOf = childOf;
	}

	/**
	 * Get the value of the entity.
	 * 
	 * @return  A String value.
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Set the value of the entity.
	 * 
	 * @param value  A String value.
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Get the color of the entity.
	 * 
	 * @return  The color of the entity.
	 */
	public Color getColor() {
		return color;
	}

	/**
	 * Set the color of the entity.
	 * 
	 * @param color  The color of the entity.
	 */
	public void setColor(Color color) {
		this.color = color;
	}
	
	/**
	 * Get the hash map of attribute values.
	 * 
	 * @return A hash map containing the attribute variable names as keys and
	 *   their corresponding attribute values as values.
	 */
	public HashMap<String, String> getAttributeValues() {
		return attributeValues;
	}

	/**
	 * Set the hash map of attribute values.
	 * 
	 * @param attributeValues A hash map containing attribute variable names as
	 *   keys and their corresponding attribute values as values.
	 */
	public void setAttributeValues(HashMap<String, String> attributeValues) {
		this.attributeValues = attributeValues;
	}

	/**
	 * Get a String representation of the Entity. This returns the entity value.
	 */
	public String toString() {
		return this.value;
	}

	/**
	 * Compare the entity to another entity to establish a sorting order.
	 */
	@Override
	public int compareTo(Entity a) {
		return this.getValue().compareTo(a.getValue());
	}

	/**
	 * Is this entity equal to another object?
	 *
	 * @param o An object for comparison.
	 * @return  A boolean indicator of whether the other entry is identical.
	 */
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null) return false;
		if (getClass() != o.getClass())	return false;
		Entity e = (Entity) o;
		if (this.id != e.getId()) {
			return false;
		}
		if (this.variableId != e.getVariableId()) {
			return false;
		}
		if (this.childOf != e.getChildOf()) {
			return false;
		}
		if ((this.value == null) != (e.getValue() == null)) {
			return false;
		}
		if (this.value != null && e.getValue() != null && !this.value.equals(e.getValue())) {
			return false;
		}
		if ((this.color == null) != (e.getColor() == null)) {
			return false;
		}
		if (this.color != null && e.getColor() != null && !this.color.equals(e.getColor())) {
			return false;
		}
		if (this.inDatabase != e.isInDatabase()) {
			return false;
		}
		if ((this.attributeValues == null) != (e.getAttributeValues() == null)) {
			return false;
		}
		if (this.attributeValues != null && e.getAttributeValues() != null && !this.attributeValues.equals(e.getAttributeValues())) {
			return false;
		}
		return true;
	}
}