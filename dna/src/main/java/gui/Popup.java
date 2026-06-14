package gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.LineBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import org.jdesktop.swingx.autocomplete.AutoCompleteDecorator;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import model.Entity;
import model.Coder;
import model.Statement;
import model.Value;

/**
 * Show a small popup window to display and/or edit the variables of a statement.
 */
class Popup extends JDialog {
	private static final long serialVersionUID = -4955213646188753456L;
	private Container c;
	private Statement statement;
	private Point los;
	private model.Color color;
	private boolean windowDecoration, editable;
	private JPanel gridBagPanel;
	private int textFieldWidth;
	private ArrayList<Value> variables;
	private JButton duplicate, remove;
	private Coder coder;
	private JComboBox<Coder> coderComboBox;
	private JButton cancelButton, saveButton;
	
	/**
	 * Popup dialog window to display the contents of a statements. The user can
	 * edit the values of each variable.
	 * 
	 * @param X              Horizontal coordinate for the window.
	 * @param Y              Vertical coordinate for the window.
	 * @param statement      The {@link Statement} to be edited.
	 * @param location       Location of the DNA text panel on screen.
	 * @param coder          The current coder who is viewing the statement.
	 * @param eligibleCoders A list of coders with the permission to edit the statement.
	 */
	Popup(double X, double Y, Statement statement, Point location, Coder coder, ArrayList<Coder> eligibleCoders) {
		this.statement = statement;
		this.variables = statement.getValues();
		this.coder = new Coder(statement.getCoderId(), statement.getCoderName(), statement.getCoderColor());
		int statementId = statement.getId();
		this.los = location;
		this.textFieldWidth = coder.getPopupWidth();
		this.color = statement.getStatementTypeColor();
		if (coder.isPopupDecoration() == true) {
			this.windowDecoration = true;
		} else {
			this.windowDecoration = false;
			this.setUndecorated(true);
		}
		
		// should the changes in the statements be saved? check permissions...
		editable = true;
		if (statement.getCoderId() != coder.getId() && coder.isPermissionEditOthersStatements() == false) {
			editable = false;
		}
		if (coder.isPermissionEditStatements() == false) {
			editable = false;
		}
		if (statement.getCoderId() != coder.getId() &&
				coder.isPermissionEditOthersStatements(statement.getCoderId()) == false) {
			editable = false;
		}
		
		this.setTitle("Statement details");
		this.setAlwaysOnTop(true);
		this.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		
		ImageIcon statementIcon = new SvgIcon("/icons/tabler_message_2.svg", 32).getImageIcon();
		this.setIconImage(statementIcon.getImage());
		
		c = getContentPane();
		
		JPanel contentsPanel = new JPanel(new BorderLayout());
		contentsPanel.setBorder(new LineBorder(Color.BLACK));
		JPanel titleDecorationPanel = new JPanel(new BorderLayout());
		JPanel idAndPositionPanel = new JPanel();
		
		gridBagPanel = new JPanel(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		
		JLabel sPosLabel = new JLabel("start");
		JTextField startPos = new JTextField(Integer.toString(statement.getStart()));
		int h = 20; // getting the text field height does not work properly on MacOS, so need to hard-code
		startPos.setPreferredSize(new Dimension(startPos.getPreferredSize().width, h));
		startPos.setEditable(false);
		
		JLabel ePosLabel = new JLabel("end");
		JTextField endPos = new JTextField(Integer.toString(statement.getStop()));
		endPos.setPreferredSize(new Dimension(endPos.getPreferredSize().width, h));
		endPos.setEditable(false);

		JLabel idLabel = new JLabel(" ID");
		JTextField idField = new JTextField(Integer.toString(statementId));
		idField.setPreferredSize(new Dimension(idField.getPreferredSize().width, h));
		idField.setEditable(false);

		String type = statement.getStatementTypeLabel();
		JLabel typeLabel = new JLabel(" " + type);
		
		JSeparator sep = new JSeparator();
		
		JPanel colorPanel = new JPanel();
		colorPanel.setBackground(color.toAWTColor());
		colorPanel.setPreferredSize(new Dimension(4, 4));
		
		ImageIcon duplicateIcon = new SvgIcon("/icons/tabler_copy.svg", h).getImageIcon();
		duplicate = new JButton(duplicateIcon);
		duplicate.setToolTipText("create a copy of this statement at the same location");
		duplicate.setPreferredSize(new Dimension(h, h));
		if (coder.isPermissionAddStatements() == false) {
			duplicate.setEnabled(false);
		}
		
		ImageIcon removeIcon = new SvgIcon("/icons/tabler_trash.svg", h).getImageIcon();
		remove = new JButton(removeIcon);
		remove.setToolTipText("completely remove the whole statement (but keep the text)");
		remove.setPreferredSize(new Dimension(h, h));
		remove.setEnabled(true);
		if (coder.isPermissionDeleteStatements() == false) {
			remove.setEnabled(false);
		}
		if (this.statement.getCoderId() != coder.getId() &&
				Dna.sql.getActiveCoder().isPermissionEditOthersStatements() == false) {
			remove.setEnabled(false);
		}
		if (this.statement.getCoderId() != coder.getId() &&
				coder.isPermissionEditOthersStatements(statement.getCoderId()) == false) {
			remove.setEnabled(false);
		}
		
		idAndPositionPanel.add(idLabel);
		idAndPositionPanel.add(idField);
		idAndPositionPanel.add(sPosLabel);
		idAndPositionPanel.add(startPos);
		idAndPositionPanel.add(ePosLabel);
		idAndPositionPanel.add(endPos);

		if (eligibleCoders == null || eligibleCoders.size() == 1) {
			CoderBadgePanel cbp = new CoderBadgePanel(this.coder);
			idAndPositionPanel.add(cbp);
		} else {
			int selectedIndex = -1;
			for (int i = 0; i < eligibleCoders.size(); i++) {
				if (eligibleCoders.get(i).getId() == this.coder.getId()) {
					selectedIndex = i;
				}
			}
			coderComboBox = new JComboBox<Coder>();
			CoderComboBoxModel comboBoxModel = new CoderComboBoxModel(eligibleCoders);
			coderComboBox.setModel(comboBoxModel);
			coderComboBox.setRenderer(new CoderComboBoxRenderer(9, 0, 22));
			coderComboBox.setSelectedIndex(selectedIndex);
			coderComboBox.setPreferredSize(new Dimension(coderComboBox.getPreferredSize().width, h)); // need to hard-code height because of MacOS
			idAndPositionPanel.add(coderComboBox);
		}
		
		idAndPositionPanel.add(duplicate);
		idAndPositionPanel.add(remove);
		
		titleDecorationPanel.add(idAndPositionPanel, BorderLayout.EAST);
		titleDecorationPanel.add(typeLabel, BorderLayout.CENTER);
		titleDecorationPanel.add(sep, BorderLayout.SOUTH);
		titleDecorationPanel.add(colorPanel, BorderLayout.WEST);
		contentsPanel.add(titleDecorationPanel, BorderLayout.NORTH);
		
		gbc.insets = new Insets(3, 3, 3, 3);
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.anchor = GridBagConstraints.EAST;
		
		// add fields for the values
		ArrayList<Integer> variableIds = new ArrayList<Integer>();
		HashMap<Integer, Integer> indexMap = new HashMap<Integer, Integer>();
		int counter = 0;
		for (int i = 0; i < variables.size(); i++) {
			if (variables.get(i).getDataType().equals("short text")) {
				indexMap.put(i, counter);
				counter++;
				variableIds.add(variables.get(i).getVariableId());
			}
		}
		ArrayList<ArrayList<Entity>> entities = Dna.sql.getEntities(variableIds, false); // switch to true to color unused entities in red; but it takes much longer with large databases

		// create boxes with values
		for (int i = 0; i < variables.size(); i++) {
			String key = variables.get(i).getKey();
			String dataType = variables.get(i).getDataType();
			JLabel label = new JLabel(key, JLabel.TRAILING);
			if (dataType.equals("short text")) {
				Entity[] entitiesArray = new Entity[entities.get(indexMap.get(i)).size()];
				entitiesArray = entities.get(indexMap.get(i)).toArray(entitiesArray);
				JComboBox<Entity> box = new JComboBox<Entity>(entitiesArray);
				box.setRenderer(new AttributeComboBoxRenderer());
				box.setEditable(true);
				
				// paint the selected value in the attribute color
				String s = ((JTextField) box.getEditor().getEditorComponent()).getText();
				Color fg = javax.swing.UIManager.getColor("TextField.foreground"); // default unselected foreground color of JTextField
				for (int j = 0; j < box.getModel().getSize(); j++) {
					if (s.equals(box.getModel().getElementAt(j).getValue())) {
						fg = box.getModel().getElementAt(j).getColor().toAWTColor();
					}
				}
				((JTextField) box.getEditor().getEditorComponent()).setSelectedTextColor(fg);
				((JTextField) box.getEditor().getEditorComponent()).setForeground(fg);
				
				// add a document listener to the combobox to paint the selected value in the attribute color, despite being highlighted
				((JTextField) box.getEditor().getEditorComponent()).getDocument().addDocumentListener(new DocumentListener() {
					@Override
					public void changedUpdate(DocumentEvent arg0) {
						formatEntry();
					}
					@Override
					public void insertUpdate(DocumentEvent arg0) {
						formatEntry();
					}
					@Override
					public void removeUpdate(DocumentEvent arg0) {
						formatEntry();
					}
					private void formatEntry() {
						Color fg = javax.swing.UIManager.getColor("TextField.foreground"); // default unselected foreground color of JTextField
						for (int i = 0; i < box.getModel().getSize(); i++) {
							if (((JTextField) box.getEditor().getEditorComponent()).getText().equals(box.getModel().getElementAt(i).getValue())) {
								fg = box.getModel().getElementAt(i).getColor().toAWTColor();
							}
						}
						((JTextField) box.getEditor().getEditorComponent()).setSelectedTextColor(fg);
						((JTextField) box.getEditor().getEditorComponent()).setForeground(fg);
					}
				});
				
				if (coder.isPopupAutoComplete() == true) {
					AutoCompleteDecorator.decorate(box); // auto-complete short text values; part of SwingX
				}
				if (editable == true) {
					box.setEnabled(true);
				} else {
					box.setEnabled(false);
				}
    			box.setPreferredSize(new Dimension(this.textFieldWidth, 20));
    			box.setSelectedItem((Entity) variables.get(i).getValue());
    			
    			// need to add a focus listener to save the contents; otherwise without auto-completion the last edited box will not be saved
    			box.getEditor().getEditorComponent().addFocusListener(new FocusListener() {
    				
					@Override
					public void focusGained(FocusEvent e) {
						// no action needed when focus is gained
					}
					
					@Override
					public void focusLost(FocusEvent e) {
						box.setSelectedItem(box.getEditor().getItem());
					}
    			});
    			
				gbc.anchor = GridBagConstraints.EAST;
	    		gridBagPanel.add(label, gbc);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.gridx++;
				gridBagPanel.add(box, gbc);
				gbc.gridx--;
				gbc.gridy++;
			} else if (dataType.equals("long text")) {
				String entry = (String) variables.get(i).getValue();
    			JTextArea box = new JTextArea();
    			box.setEditable(true);
				if (editable == true) {
					box.setEnabled(true);
				} else {
					box.setEnabled(false);
				}
    			box.setWrapStyleWord(true);
    			box.setLineWrap(true);
    			box.setText(entry);
    			JScrollPane boxScroller = new JScrollPane(box);
    			boxScroller.setPreferredSize(new Dimension(this.textFieldWidth, 100));
    			boxScroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
    			
				gbc.anchor = GridBagConstraints.NORTHEAST;
	    		gridBagPanel.add(label, gbc);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.gridx++;
				gridBagPanel.add(boxScroller, gbc);
				gbc.gridx--;
				gbc.gridy++;
			} else if (dataType.equals("boolean")) {
				int entry = (Integer) variables.get(i).getValue();
				boolean val;
				if (entry == 0) {
					val = false;
				} else {
					val = true;
				}
				BooleanButtonPanel buttons = new BooleanButtonPanel();
				if (val == true) {
					buttons.setYes(true);
				} else {
					buttons.setYes(false);
				}
				if (editable == true) {
					buttons.setEnabled(true);
				} else {
					buttons.setEnabled(false);
				}
    			
				gbc.anchor = GridBagConstraints.EAST;
	    		gridBagPanel.add(label, gbc);
				gbc.insets = new Insets(0,0,0,0);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.gridx++;
				gridBagPanel.add(buttons, gbc);
				gbc.insets = new Insets(3,3,3,3);
				gbc.gridx--;
				gbc.gridy++;
			} else if (dataType.equals("integer")) {
				int entry = (Integer) variables.get(i).getValue();
				JSpinner jsp = new JSpinner();
				jsp.setValue(entry);
    			jsp.setPreferredSize(new Dimension(70, 20));
    			jsp.setEnabled(true);
    			JPanel jp = new JPanel(new FlowLayout(FlowLayout.LEFT));
    			jp.add(jsp);
				if (editable == true) {
					jsp.setEnabled(true);
				} else {
					jsp.setEnabled(false);
				}
    			
				gbc.anchor = GridBagConstraints.EAST;
	    		gridBagPanel.add(label, gbc);
				gbc.insets = new Insets(0, 0, 0, 0);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.gridx++;
				gridBagPanel.add(jp, gbc);
				gbc.insets = new Insets(3, 3, 3, 3);
				gbc.gridx--;
				gbc.gridy++;
			}
		}
		
		contentsPanel.add(gridBagPanel, BorderLayout.CENTER);
		
		// add buttons if window decoration is true
		if (windowDecoration == true) {
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
			ImageIcon cancelIcon = new SvgIcon("/icons/google_device_reset.svg", h).getImageIcon();
			cancelButton = new JButton("Cancel", cancelIcon);
			cancelButton.setToolTipText("close this window without making any changes");
			buttonPanel.add(cancelButton);
			ImageIcon saveIcon = new SvgIcon("/icons/google_round_save_black_48dp.svg", h).getImageIcon();
			saveButton = new JButton("Save", saveIcon);
			saveButton.setToolTipText("save each variable into the database and close this window");
			buttonPanel.add(saveButton);
			if (editable == false) {
				saveButton.setEnabled(false);
			}
			contentsPanel.add(buttonPanel, BorderLayout.SOUTH);
		}
		
		c.add(contentsPanel);
		
		this.pack();
		double xDouble = los.getX() + X;
		double yDouble = los.getY() + Y;
		int x = (int) xDouble + 6;
		int y = (int) yDouble + 13;
		this.setLocation(x, y);
	}
	
	/**
	 * Get a reference to the duplicate button.
	 * 
	 * @return The duplicate button.
	 */
	JButton getDuplicateButton() {
		return duplicate;
	}
	
	/**
	 * Get a reference to the remove button.
	 * 
	 * @return The remove button.
	 */
	JButton getRemoveButton() {
		return remove;
	}

	/**
	 * Is the popup window editable?
	 * 
	 * @return True if the values can be changed and false otherwise.
	 */
	boolean isEditable() {
		return this.editable;
	}
	
	/**
	 * Does the popup window have window decoration?
	 * 
	 * @return True if the popup has window decoration and false otherwise.
	 */
	boolean hasWindowDecoration() {
		return this.windowDecoration;
	}

	/**
	 * Get a reference to the cancel button.
	 * 
	 * @return The save button.
	 */
	JButton getCancelButton() {
		return this.cancelButton;
	}
	
	/**
	 * Get a reference to the save button.
	 * 
	 * @return The save button.
	 */
	JButton getSaveButton() {
		return this.saveButton;
	}
	
	/**
	 * Check if the coder ID has been changed.
	 * 
	 * @return Indicator of statement coder ID change.
	 */
	boolean isCoderChanged() {
		if (this.coder.getId() != this.statement.getCoderId()) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Get an updated copy of the statement.
	 * 
	 * @return The statement.
	 */
	Statement getStatementCopy() {
		Statement s = new Statement(this.statement);
		s.setCoderColor(this.coder.getColor());
		s.setCoderName(this.coder.getName());
		s.setCoderId(this.coder.getId());
		s.setValues(this.variables);
		return s;
	}
	
	/**
	 * In a statement popup window, read the contents from all combo boxes and
	 * save them into the database.
	 * 
	 * @param simulate  If true, do not actually write the changes.
	 * @return          True if at least one of the values has changed.
	 */
	boolean saveContents(boolean simulate) {
		boolean changed = false;
		Component[] com = this.gridBagPanel.getComponents();
		int i, j;
		try {
			for (i = 0; i < com.length; i++) {
				Object content = null; // the value of a variable, e.g., "EPA"
				String variableName; // the name of the variable, e.g., "organization"
				if (com[i].getClass().getName().equals("javax.swing.JComboBox")) { // short text
					variableName = ((JLabel) com[i - 1]).getText();
					@SuppressWarnings("unchecked")
					JComboBox<Entity> box = (JComboBox<Entity>) com[i]; // save the combo box
					if (!((String) box.getSelectedItem().toString()).equals(box.getEditor().getItem().toString())) {
						box.setSelectedItem(box.getEditor().getItem()); // make sure combo box edits are saved even if the editor has not lost its focus yet
					}
					Object object = box.getSelectedItem();
					Entity entity;
					if (object.getClass().getName().endsWith("String")) { // if not an existing entity, the editor returns a String
						String s = (String) object;
						if (s.length() > 0 && s.matches("^\\s+$")) { // replace a (multiple) whitespace string by an empty string
							s = "";
						}
						s = s.substring(0, Math.min(190, s.length()));
						entity = new Entity(s); // the new entity has an ID of -1; the SQL class needs to take care of this when writing into the database
					} else {
						entity = (Entity) box.getSelectedItem();
					}
					for (j = 0; j < this.variables.size(); j++) { // update the variable corresponding to the variable name identified
						if (this.variables.get(j).getKey().equals(variableName)) {
							if (!((Entity) this.variables.get(j).getValue()).getValue().equals(entity.getValue())) {
								if (simulate == false) {
									this.variables.get(j).setValue(entity);
								}
								changed = true;
							}
						}
					}
				} else if (com[i].getClass().getName().equals("javax.swing.JScrollPane")) { // long text
					variableName = ((JLabel) com[i - 1]).getText();
					JScrollPane jsp = ((JScrollPane) com[i]);
					JTextArea jta = (JTextArea) jsp.getViewport().getView();
					content = jta.getText();
					if (content == null) {
						content = "";
					}
					for (j = 0; j < this.variables.size(); j++) {
						if (this.variables.get(j).getKey().equals(variableName)) {
							if (!this.variables.get(j).getValue().equals(content)) {
								if (simulate == false) {
									this.variables.get(j).setValue(content);
								}
								changed = true;
							}
						}
					}
				} else if (com[i].getClass().getName().equals("javax.swing.JPanel")) { // integer
					variableName = ((JLabel) com[i - 1]).getText();
					JPanel jp = (JPanel) com[i];
					JSpinner jsp = (JSpinner) jp.getComponent(0);
					content = jsp.getValue();
					for (j = 0; j < this.variables.size(); j++) {
						if (this.variables.get(j).getKey().equals(variableName)) {
							if ((Integer) this.variables.get(j).getValue() != content) {
								if (simulate == false) {
									this.variables.get(j).setValue(content);
								}
								changed = true;
							}
						}
					}
				} else if (com[i].getClass().getName().endsWith("BooleanButtonPanel")) { // boolean
					variableName = ((JLabel) com[i - 1]).getText();
					content = ((BooleanButtonPanel) com[i]).isYes();
					int intBool;
					if ((Boolean) content == false) {
						intBool = 0;
					} else {
						intBool = 1;
					}
					for (j = 0; j < this.variables.size(); j++) {
						if (this.variables.get(j).getKey().equals(variableName)) {
							if ((Integer) this.variables.get(j).getValue() != intBool) {
								if (simulate == false) {
									this.variables.get(j).setValue(intBool);
								}
								changed = true;
							}
						}
					}
				}
			}
			if (this.coderComboBox != null && this.coder.getId() != ((Coder) coderComboBox.getSelectedItem()).getId()) {
				if (simulate == false) {
					this.coder = (Coder) coderComboBox.getSelectedItem();
				}
				changed = true;
			}
			if (changed == true && simulate == false) {
				Dna.sql.updateStatement(this.statement.getId(), this.variables, this.coder.getId()); // write changes into the database
			}
		} catch (Exception e) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Could not update statement contents in the database.",
					"Read contents from popup and tried to save them in the SQL database, but something went wrong. Changes are being reverted. See exception for details.",
					e);
			Dna.logger.log(l);
		}
		return changed;
	}
}