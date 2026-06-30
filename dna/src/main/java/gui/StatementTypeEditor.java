package gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.DefaultListSelectionModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JColorChooser;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListCellRenderer;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;

import org.jdesktop.swingx.JXTextField;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import model.StatementType;
import model.Value;

/**
 * Statement type editor. A dialog window for adding new statement types,
 * deleting existing statement types, changing the name or color of a statement
 * type, and adding, renaming, or deleting variables of statement types.
 */
public class StatementTypeEditor extends JDialog {
	private static final long serialVersionUID = 3646305292547200629L;
	private JList<StatementType> statementTypeList;
	private ArrayList<StatementType> statementTypes;
	private JTextField idField, nameField;
	private JLabel idLabel, nameLabel, colorLabel;
	private ColorButton colorButton;
	private JTable variableTable;
	private VariableTableModel variableTableModel;
	private JButton resetDetailsButton, applyDetailsButton;
	private JButton addVariableButton, deleteVariableButton, renameVariableButton, attributeButton;
	private JButton addStatementTypeButton, deleteStatementTypeButton;

	/**
	 * Create a new instance of a statement type editor dialog window.
	 */
	public StatementTypeEditor(JFrame parent) {
		super(parent, "Statement type editor", true);
		this.setModal(true);
		this.setTitle("Edit statement types");
		ImageIcon statementTypeIcon = new SvgIcon("/icons/tabler_message_2.svg", 32).getImageIcon();
		this.setIconImage(statementTypeIcon.getImage());
		this.setLayout(new BorderLayout());

		// get statement types from the database
		statementTypes = Dna.sql.getStatementTypes();
		StatementType[] st = new StatementType[statementTypes.size()];
		st = statementTypes.toArray(st);

		// statement type list
		JPanel listPanel = new JPanel(new BorderLayout());
		statementTypeList = new JList<StatementType>(st);
		statementTypeList.setCellRenderer(new StatementTypeRenderer());
		statementTypeList.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		JScrollPane listScroller = new JScrollPane(statementTypeList);
		listScroller.setPreferredSize(new Dimension(200, 500));
		statementTypeList.addListSelectionListener(new ListSelectionListener() {
			@Override
			public void valueChanged(ListSelectionEvent e) {
				if (e.getValueIsAdjusting() == false) {
					updateUI(e.getSource());
				}
			}
		});
		listPanel.add(listScroller, BorderLayout.CENTER);
		
		JPanel listButtonPanel = new JPanel(new GridLayout(0, 2));
		ImageIcon addStatementTypeIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-circle-plus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		addStatementTypeButton = new JButton("Add", addStatementTypeIcon);
		addStatementTypeButton.setToolTipText("Add new statement type to the database");
		addStatementTypeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int dialog = JOptionPane.showConfirmDialog(StatementTypeEditor.this, "Add a new statement type to the database?\nThis action will be written to the database now.", "Confirmation", JOptionPane.YES_NO_OPTION);
				if (dialog == 0) {
					int id = Dna.sql.addStatementType("(New statement type)", new model.Color(181, 255, 0));
					repopulateStatementTypeListFromDatabase(id);
					updateUI(e.getSource());
				}
			}
		});
		listButtonPanel.add(addStatementTypeButton);

		ImageIcon deleteStatementTypeIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-circle-minus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		deleteStatementTypeButton = new JButton("Delete", deleteStatementTypeIcon);
		deleteStatementTypeButton.setToolTipText("Delete the selected statement type");
		deleteStatementTypeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int id = statementTypeList.getSelectedValue().getId();
				if (id > 0) {
					int count = Dna.sql.countStatements(id);
					int dialog = JOptionPane.showConfirmDialog(StatementTypeEditor.this, "Delete statement type " + id + " permanently from the database?\nThis will also delete " + count + " statements of this type and all associated entities/attributes now.\nWarning: There is no option to undo this action.", "Confirmation", JOptionPane.YES_NO_OPTION);
					if (dialog == 0) {
						boolean success = Dna.sql.deleteStatementType(id);
						repopulateStatementTypeListFromDatabase(-1);
						updateUI(e.getSource());
						if (success) {
							JOptionPane.showMessageDialog(StatementTypeEditor.this, "Statement type " + id + " was successfully deleted from the database.");
						} else {
							JOptionPane.showMessageDialog(StatementTypeEditor.this, "Statement type " + id + " could not be deleted. Check the message log for details.");
						}
					}
				}
			}
		});
		deleteStatementTypeButton.setEnabled(false);
		listButtonPanel.add(deleteStatementTypeButton);
		
		listPanel.add(listButtonPanel, BorderLayout.SOUTH);
		
		CompoundBorder borderList;
		borderList = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Statement types"));
		listPanel.setBorder(borderList);
		
		this.add(listPanel, BorderLayout.WEST);

		// details panel
		idField = new JTextField(2);
		idField.setEditable(false);
		idLabel = new JLabel("ID", JLabel.TRAILING);
		idLabel.setLabelFor(idField);
		
		nameField = new JTextField(20);
		nameField.setEnabled(false);
		nameLabel = new JLabel("Name", JLabel.TRAILING);
		nameLabel.setLabelFor(nameField);
		nameLabel.setEnabled(false);

		DocumentListener documentListener = new DocumentListener() {
			@Override
			public void changedUpdate(DocumentEvent e) {
				updateUI(e);
			}
			@Override
			public void insertUpdate(DocumentEvent e) {
				updateUI(e);
			}
			@Override
			public void removeUpdate(DocumentEvent e) {
				updateUI(e);
			}
		};
		nameField.getDocument().addDocumentListener(documentListener);
		
		colorButton = new ColorButton();
		colorButton.setBorder(BorderFactory.createLineBorder(Color.BLACK, 1));
		colorButton.setEnabled(false);
		colorLabel = new JLabel("Color", JLabel.TRAILING);
		colorLabel.setLabelFor(colorButton);
		colorLabel.setEnabled(false);
		colorButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Color newColor = JColorChooser.showDialog(StatementTypeEditor.this, "Choose color...", colorButton.getColor().toAWTColor());
				if (newColor != null) {
					colorButton.setColor(new model.Color(newColor.getRed(), newColor.getGreen(), newColor.getBlue()));
				}
				updateUI(e.getSource());
			}
		});

		JPanel detailsButtonPanel = new JPanel();
		ImageIcon resetDetailsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-rotate-clockwise.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		resetDetailsButton = new JButton("Reset", resetDetailsIcon);
		resetDetailsButton.setToolTipText("Reload the statement type name/label and color from the database and undo any changes made here");
		resetDetailsButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (!statementTypeList.getSelectedValue().getLabel().equals(nameField.getText()) ||
						!statementTypeList.getSelectedValue().getColor().equals(colorButton.getColor())) {
					Dna.sql.updateStatementType(statementTypeList.getSelectedValue().getId(), nameField.getText(), colorButton.getColor());
					colorButton.setColor(statementTypeList.getSelectedValue().getColor());
					nameField.setText(statementTypeList.getSelectedValue().getLabel()); // triggers button check
				}
			}
		});
		resetDetailsButton.setEnabled(false);
		detailsButtonPanel.add(resetDetailsButton);
		
		ImageIcon applyDetailsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-device-floppy.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		applyDetailsButton = new JButton("Apply / Save", applyDetailsIcon);
		applyDetailsButton.setToolTipText("Update the statement type name/label and color in the database, making them permanent");
		applyDetailsButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (!statementTypeList.getSelectedValue().getLabel().equals(nameField.getText()) ||
						!statementTypeList.getSelectedValue().getColor().equals(colorButton.getColor())) {
					Dna.sql.updateStatementType(statementTypeList.getSelectedValue().getId(), nameField.getText(), colorButton.getColor());
					repopulateStatementTypeListFromDatabase(statementTypeList.getSelectedValue().getId());
					updateUI(e.getSource());
				}
			}
		});
		applyDetailsButton.setEnabled(false);
		detailsButtonPanel.add(applyDetailsButton);

		JPanel detailsPanel = new JPanel(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(5, 5, 0, 5);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.gridx = 0;
		gbc.gridy = 0;
		detailsPanel.add(idLabel, gbc);
		gbc.gridx = 1;
		detailsPanel.add(idField, gbc);
		gbc.gridx = 0;
		gbc.gridy = 1;
		detailsPanel.add(nameLabel, gbc);
		gbc.gridx = 1;
		detailsPanel.add(nameField, gbc);
		gbc.gridx = 0;
		gbc.gridy = 2;
		detailsPanel.add(colorLabel, gbc);
		gbc.gridx = 1;
		detailsPanel.add(colorButton, gbc);
		gbc.gridy = 3;
		gbc.insets = new Insets(5, 0, 5, 5);
		gbc.weightx = 1.0;
		detailsPanel.add(detailsButtonPanel, gbc);
		
		CompoundBorder borderDetails;
		borderDetails = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Statement type details"));
		detailsPanel.setBorder(borderDetails);
		
		// variable panel
		JPanel variablePanel = new JPanel(new BorderLayout());
        variableTableModel = new VariableTableModel();
        variableTable = new JTable(variableTableModel);
		variableTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		variableTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			@Override
			public void valueChanged(ListSelectionEvent e) {
				updateUI(e.getSource());
			}
		});
		JScrollPane variableScrollPane = new JScrollPane(variableTable);
        variableScrollPane.setPreferredSize(new Dimension(500, 300));
        variablePanel.add(variableScrollPane, BorderLayout.CENTER);

        JPanel variableButtonPanel = new JPanel();

		ImageIcon addVariableIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-code-plus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		addVariableButton = new JButton("Add...", addVariableIcon);
		addVariableButton.setToolTipText("Add new variable to the statement type");
		addVariableButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(StatementTypeEditor.this);
				VariableDialog vd = new VariableDialog(frame, null, null);
				if (!vd.canceled) {
					String name = vd.getVariableName();
					String type = vd.getDataType();
					int sid = statementTypeList.getSelectedValue().getId();
					int vid = Dna.sql.addVariable(sid, name, type);
					if (vid > -1) {
						repopulateStatementTypeListFromDatabase(statementTypeList.getSelectedValue().getId());
						updateUI(e.getSource());
						JOptionPane.showMessageDialog(StatementTypeEditor.this, "Variable " + vid + " was successfully added to statement type " + sid + ".");
					} else {
						JOptionPane.showMessageDialog(StatementTypeEditor.this, "Variable could not be added to statement type " + sid + ".\nCheck the message log for details.");
					}
				}
			}
		});
		addVariableButton.setEnabled(false);
		variableButtonPanel.add(addVariableButton);

		ImageIcon deleteVariableIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-code-minus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		deleteVariableButton = new JButton("Delete", deleteVariableIcon);
		deleteVariableButton.setToolTipText("Delete selected variable from the statement type");
		deleteVariableButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int sid = statementTypeList.getSelectedValue().getId();
				int vid = (int) variableTable.getValueAt(variableTable.getSelectedRow(), 0);
				int count = Dna.sql.countStatements(sid);
				int dialog = JOptionPane.showConfirmDialog(StatementTypeEditor.this, "Delete Variable " + vid + " permanently from the database?\nThis operation affects " + count + " statements of this type.\nAll associated values/entities/attributes will be deleted.\nWarning: There is no option to undo this action.", "Confirmation", JOptionPane.YES_NO_OPTION);
				if (dialog == 0) {
					boolean success = Dna.sql.deleteVariable(vid);
					repopulateStatementTypeListFromDatabase(statementTypeList.getSelectedValue().getId());
					updateUI(e.getSource());
					if (success) {
						JOptionPane.showMessageDialog(StatementTypeEditor.this, "Variable " + vid + " was successfully deleted from statement type " + sid + ".");
					} else {
						JOptionPane.showMessageDialog(StatementTypeEditor.this, "Variable " + vid + " could not be deleted from statement type " + sid + ".\nCheck the message log for details.");
					}
				}
			}
		});
		deleteVariableButton.setEnabled(false);
		variableButtonPanel.add(deleteVariableButton);

		ImageIcon renameVariableIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-forms.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		renameVariableButton = new JButton("Rename", renameVariableIcon);
		renameVariableButton.setToolTipText("Rename selected variable");
		renameVariableButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int vid = (int) variableTable.getValueAt(variableTable.getSelectedRow(), 0);
				String oldName = (String) variableTable.getValueAt(variableTable.getSelectedRow(), 1);
				String oldType = (String) variableTable.getValueAt(variableTable.getSelectedRow(), 2);
				JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(StatementTypeEditor.this);
				VariableDialog vd = new VariableDialog(frame, oldName, oldType);
				if (!vd.canceled()) {
					String name = vd.getVariableName();
					int sid = statementTypeList.getSelectedValue().getId();
					boolean success = Dna.sql.updateVariableName(vid, name);
					if (success) {
						repopulateStatementTypeListFromDatabase(statementTypeList.getSelectedValue().getId());
						updateUI(e.getSource());
						JOptionPane.showMessageDialog(StatementTypeEditor.this, "Variable " + vid + " in statement type " + sid + " was successfully renamed.");
					} else {
						JOptionPane.showMessageDialog(StatementTypeEditor.this, "Variable " + vid + " in statement type " + sid + " could not be renamed.\\nCheck the message log for details.");
					}
				}
			}
		});
		renameVariableButton.setEnabled(false);
		variableButtonPanel.add(renameVariableButton);
		
		ImageIcon attributeIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-list.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		attributeButton = new JButton("Attribute variables", attributeIcon);
		attributeButton.setToolTipText("Edit attribute variables associated with the selected short text variable.");
		attributeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int vid = (int) variableTable.getValueAt(variableTable.getSelectedRow(), 0);
				String vn = (String) variableTable.getValueAt(variableTable.getSelectedRow(), 1);
				JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(StatementTypeEditor.this);
				new AttributeDialog(frame, vid, vn);
			}
		});
		attributeButton.setEnabled(false);
		variableButtonPanel.add(attributeButton);
		variablePanel.add(variableButtonPanel, BorderLayout.SOUTH);

		CompoundBorder borderVariables;
		borderVariables = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Variables"));
		variablePanel.setBorder(borderVariables);
		
		// content panel: details, variables, and links
		JPanel contentPanel = new JPanel(new BorderLayout());
		contentPanel.add(detailsPanel, BorderLayout.NORTH);
		contentPanel.add(variablePanel, BorderLayout.CENTER);
		this.add(contentPanel, BorderLayout.CENTER);
		
		this.pack();
		this.setLocationRelativeTo(null);
		this.setVisible(true);
	}

	/**
	 * Load all statement types from the database and put them into the GUI. A
	 * specified statement type can be selected afterwards. If this is not
	 * desired, a value of {@code -1} can be provided.
	 * 
	 * @param selectStatementTypeId  ID of the statement type to be selected.
	 *   Can be {@code -1}.
	 */
	private void repopulateStatementTypeListFromDatabase(int selectStatementTypeId) {
		DefaultListModel<StatementType> model = new DefaultListModel<StatementType>();
		statementTypes = Dna.sql.getStatementTypes();
		int statementTypeIndex = -1;
		for (int i = 0; i < statementTypes.size(); i++) {
			model.addElement(statementTypes.get(i));
			if (statementTypes.get(i).getId() == selectStatementTypeId) {
				statementTypeIndex = i;
			}
		}
		statementTypeList.setModel(model);
		if (statementTypeIndex > -1) {
			statementTypeList.setSelectedIndex(statementTypeIndex);
		}
	}
	
	/**
	 * (Re)load a statement type from the array list after changes to details or
	 * variables have been made or after the list has been reloaded from the
	 * database. Adjust the controls after selecting the statement type.
	 */
	private void updateUI(Object source) {
		// statement type delete button
		if (statementTypeList.isSelectionEmpty()) {
			deleteStatementTypeButton.setEnabled(false);
		} else {
			deleteStatementTypeButton.setEnabled(true);
		}
		
		// ID, name, and color details
		if (statementTypeList.isSelectionEmpty()) {
			idLabel.setEnabled(false);
			idField.setEnabled(false);
			nameLabel.setEnabled(false);
			nameField.setEnabled(false);
			colorLabel.setEnabled(false);
			colorButton.setEnabled(false);
			idField.setText("");
			if (!(source instanceof DocumentEvent)) {
				nameField.setText("");
			}
			colorButton.setColor(new model.Color(0, 0, 0));
		} else {
			idField.setEnabled(true);
			nameField.setEnabled(true);
			colorButton.setEnabled(true);
			idLabel.setEnabled(true);
			nameLabel.setEnabled(true);
			colorLabel.setEnabled(true);
			idField.setText(statementTypeList.getSelectedValue().getId() + "");
			if (!(source instanceof DocumentEvent || source instanceof DefaultListSelectionModel || source == colorButton)) {
				nameField.setText(statementTypeList.getSelectedValue().getLabel());
			}
			if (source == statementTypeList ||
					source == addStatementTypeButton ||
					source == deleteStatementTypeButton ||
					source == applyDetailsButton ||
					source == resetDetailsButton) {
				colorButton.setColor(statementTypeList.getSelectedValue().getColor());
			}
		}
		
		// apply details button
		if (statementTypeList.isSelectionEmpty() ||
				nameField.getText().matches("^\\s*$") ||
				(statementTypeList.getSelectedValue().getLabel().equals(nameField.getText()) &&
				statementTypeList.getSelectedValue().getColor().equals(colorButton.getColor()))) {
			applyDetailsButton.setEnabled(false);
		} else {
			applyDetailsButton.setEnabled(true);
		}
		
		// reset details button
		if (!statementTypeList.isSelectionEmpty() &&
				(!statementTypeList.getSelectedValue().getLabel().equals(nameField.getText()) ||
				!statementTypeList.getSelectedValue().getColor().equals(colorButton.getColor()))) {
			resetDetailsButton.setEnabled(true);
		} else {
			resetDetailsButton.setEnabled(false);
		}
		
		// variable table
		if (!(source instanceof DefaultListSelectionModel)) {
			variableTableModel.clear();
			if (statementTypeList.isSelectionEmpty()) {
				variableTable.setEnabled(false);
			} else {
				for (int i = 0; i < statementTypeList.getSelectedValue().getVariables().size(); i++) {
					variableTableModel.addRow(statementTypeList.getSelectedValue().getVariables().get(i));
				}
				variableTable.setEnabled(true);
			}
		}
		
		// add variable button
		if (statementTypeList.isSelectionEmpty()) {
			addVariableButton.setEnabled(false);
		} else {
			addVariableButton.setEnabled(true);
		}
		
		// delete and rename variable buttons
		if (variableTable.getSelectedRowCount() < 1 || statementTypeList.isSelectionEmpty()) {
			deleteVariableButton.setEnabled(false);
			renameVariableButton.setEnabled(false);
			attributeButton.setEnabled(false);
		} else {
			deleteVariableButton.setEnabled(true);
			renameVariableButton.setEnabled(true);
			if (variableTable.getValueAt(variableTable.getSelectedRow(), 2).equals("short text")) {
				attributeButton.setEnabled(true);
			} else {
				attributeButton.setEnabled(false);
			}
		}
	}

	/**
	 * List cell renderer for statement types. Displays just the label of the
	 * statement type.
	 */
	class StatementTypeRenderer implements ListCellRenderer<Object> {
		@Override
		public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
			StatementType statementType = (StatementType) value;
			JLabel label = new JLabel(statementType.getLabel());
			if (isSelected == true) {
				label.setBackground(javax.swing.UIManager.getColor("List.selectionBackground"));
			} else {
				label.setBackground(javax.swing.UIManager.getColor("List.background"));
			}
			label.setOpaque(true);
			label.setBorder(new EmptyBorder(5, 5, 5, 5));
			return label;
		}
	}

	/**
	 * A table model for variables.
	 */
	private class VariableTableModel extends AbstractTableModel {
		private static final long serialVersionUID = 6268706192843972473L;
		private ArrayList<Value> variables;

		/**
		 * Create a new variable table model.
		 */
		VariableTableModel() {
			this.variables = new ArrayList<Value>();
		}
		
		@Override
		public int getColumnCount() {
			return 3;
		}

		@Override
		public int getRowCount() {
			return this.variables.size();
		}

		@Override
		public Object getValueAt(int row, int col) {
			if (row > -1 && row < variables.size()) {
				if (col == 0) {
					return variables.get(row).getVariableId();
				} else if (col == 1) {
					return variables.get(row).getKey();
				} else if (col == 2) {
					return variables.get(row).getDataType();
				}
			}
			return null;
		}
		
		/**
		 * Set the value for an arbitrary table cell.
		 * 
		 * @param object The object to save for the cell.
		 * @param row    The table row.
		 * @param col    The table column.
		 */
		public void setValueAt(Object object, int row, int col) {
			if (row > -1 && row < variables.size() && col > 0) {
				if (col == 1) {
					variables.get(row).setKey((String) object); 
				} else if (col == 2) {
					variables.get(row).setDataType((String) object); 
				}
			}
			fireTableCellUpdated(row, col);
		}
		
		/**
		 * Is the cell editable?
		 * 
		 * @param row The table row.
		 * @param col The table column.
		 */
		public boolean isCellEditable(int row, int col) {
			return false;
		}

		/**
		 * Return the name of a column.
		 * 
		 * @param column The column index of the table for which the name should
		 *   be returned, starting at 0.
		 */
		public String getColumnName(int column) {
			switch(column) {
				case 0: return "Variable ID";
				case 1: return "Name";
				case 2: return "Data type";
				default: return null;
			}
		}

		/**
		 * Which type of object (i.e., class) shall be shown in the columns?
		 * 
		 * @param col The column index of the table for which the class type
		 *   should be returned, starting at 0.
		 */
		public Class<?> getColumnClass(int col) {
			switch (col) {
				case 0: return int.class;
				case 1: return String.class;
				case 2: return String.class;
				default: return null;
			}
		}
		
		/**
		 * Remove all variables from the model.
		 */
		public void clear() {
			variables.clear();
			fireTableDataChanged();
		}
		
		/**
		 * Add a new variable to the model.
		 * 
		 * @param variable A new {@link model.Value Value} object.
		 */
		public void addRow(Value variable) {
			variables.add(variable);
			fireTableRowsInserted(variables.size() - 1, variables.size() - 1);
		}
	}
	
	/**
	 * A dialog window for adding a new variable or renaming an existing
	 * variable. It contains a simple form for the name and data type of the
	 * variable as well as two buttons.
	 */
	private class VariableDialog extends JDialog {
		private static final long serialVersionUID = -3845428777468313134L;
		private JButton okButton, cancelButton;
		private JTextField variableNameField;
		private JComboBox<String> box;
		private String name, type;
		private boolean canceled = true;
		
		/**
		 * Create a new instance of a variable dialog.
		 */
		VariableDialog(Frame parent, String name, String type) {
			super(parent, "Variable", true);
			this.setModal(true);
			if (name == null || type == null) {
				this.setTitle("Add new variable");
			} else {
				this.setTitle("Rename variable");
			}
			this.setLayout(new BorderLayout());

			JPanel buttonPanel = new JPanel();
			ImageIcon cancelIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-x.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
			cancelButton = new JButton("Cancel", cancelIcon);
			cancelButton.setToolTipText("Close without saving.");
			cancelButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					canceled = true;
					VariableDialog.this.dispose();
				}
			});
			buttonPanel.add(cancelButton);

			ImageIcon okIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-check.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
			okButton = new JButton("OK", okIcon);
			okButton.setToolTipText("Add or rename variable and save changes to the database.");
			okButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					VariableDialog.this.name = variableNameField.getText();
					VariableDialog.this.type = (String) box.getSelectedItem();
					canceled = false;
					VariableDialog.this.dispose();
				}
			});
			okButton.setEnabled(false);
			buttonPanel.add(okButton);

			this.add(buttonPanel, BorderLayout.SOUTH);
			
			JLabel variableNameLabel = new JLabel("Name");
			variableNameField = new JTextField(20);
			variableNameLabel.setLabelFor(variableNameField);
			variableNameLabel.setToolTipText("Enter the name of the variable here.");
			variableNameField.setToolTipText("Enter the name of the variable here.");
			variableNameField.getDocument().addDocumentListener(new DocumentListener() {
				@Override
				public void changedUpdate(DocumentEvent e) {
					if (variableNameField.getText().matches("^\\s*$")) {
						okButton.setEnabled(false);
					} else {
						okButton.setEnabled(true);
					}
				}
				@Override
				public void insertUpdate(DocumentEvent e) {
					if (variableNameField.getText().matches("^\\s*$")) {
						okButton.setEnabled(false);
					} else {
						okButton.setEnabled(true);
					}
				}
				@Override
				public void removeUpdate(DocumentEvent e) {
					if (variableNameField.getText().matches("^\\s*$")) {
						okButton.setEnabled(false);
					} else {
						okButton.setEnabled(true);
					}
				}
			});
			if (name != null) {
				this.name = name;
				variableNameField.setText(name);
			} else {
				this.name = "";
				variableNameField.setText("");
			}
			
			String[] dataTypes = new String[] {"short text", "long text", "integer", "boolean"};
			box = new JComboBox<String>(dataTypes);
			JLabel boxLabel = new JLabel("Data type");
			boxLabel.setLabelFor(box);
			boxLabel.setToolTipText("Select the data type of the variable here.");
			box.setToolTipText("Select the data type of the variable here.");
			if (type != null) {
				this.type = type;
				box.setSelectedItem(type);
				box.setVisible(false);
				boxLabel.setVisible(false);
			} else {
				this.type = "short text";
				box.setSelectedItem("short text");
				box.setVisible(true);
				boxLabel.setVisible(true);
			}
			
			JPanel formPanel = new JPanel(new GridBagLayout());
			GridBagConstraints gbc = new GridBagConstraints();
			gbc.weightx = 1.0;
			gbc.insets = new Insets(5, 5, 0, 5);
			gbc.anchor = GridBagConstraints.WEST;
			gbc.gridx = 0;
			gbc.gridy = 0;
			formPanel.add(variableNameLabel, gbc);
			gbc.gridx = 1;
			formPanel.add(variableNameField, gbc);
			gbc.gridx = 0;
			gbc.gridy = 1;
			formPanel.add(boxLabel, gbc);
			gbc.gridx = 1;
			formPanel.add(box, gbc);
			this.add(formPanel, BorderLayout.CENTER);
			
			this.pack();
			this.setLocationRelativeTo(null);
			this.setVisible(true);
		}
		
		/**
		 * Retrieve the variable name. Accessible from outside the dialog.
		 * 
		 * @return Name of the variable.
		 */
		String getVariableName() {
			return this.name;
		}
		
		/**
		 * Retrieve the selected data type. Accessible from outside the dialog.
		 * 
		 * @return The selected data type.
		 */
		String getDataType() {
			return this.type;
		}
		
		/**
		 * Was the dialog closed without saving?
		 * 
		 * @return Boolean indicator of whether the dialog was canceled.
		 */
		boolean canceled() {
			return this.canceled;
		}
	}
	
	/**
	 * A dialog window for editing attribute variables belonging to a short text
	 * variable.
	 */
	private class AttributeDialog extends JDialog {
		private static final long serialVersionUID = 7380782015282865541L;
		JButton attributeAddButton, attributeRenameButton, attributeDeleteButton, closeButton;
		
		/**
		 * Create a new instance of an attribute dialog.
		 */
		AttributeDialog(Frame parent, int variableId, String variableName) {
			super(parent, "Attributes", true);
			this.setModal(true);
			this.setTitle("Edit attributes");
			JPanel contentsPanel = new JPanel(new BorderLayout());

			// list of attributes
			ArrayList<String> attributeVariables = Dna.sql.getAttributeVariables(variableId);
			DefaultListModel<String> attributeListModel = new DefaultListModel<String>();
			attributeListModel.addAll(attributeVariables);
			JList<String> attributeList = new JList<String>(attributeListModel);
			attributeList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
			JScrollPane listScroller = new JScrollPane(attributeList);
			listScroller.setPreferredSize(new Dimension(100, 100));
			attributeList.addListSelectionListener(new ListSelectionListener() {
				@Override
				public void valueChanged(ListSelectionEvent e) {
					if (e.getValueIsAdjusting() == false) {
						if (attributeList.getSelectedIndices().length == 0) {
							attributeRenameButton.setEnabled(false);
							attributeDeleteButton.setEnabled(false);
						} else {
							attributeRenameButton.setEnabled(true);
							attributeDeleteButton.setEnabled(true);
						}
					}
				}
			});
			contentsPanel.add(listScroller, BorderLayout.CENTER);
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
			JXTextField newField = new JXTextField("New attribute variable...");
			newField.setColumns(15);
			newField.setPreferredSize(new Dimension(newField.getPreferredSize().width, newField.getPreferredSize().height + 4));
			buttonPanel.add(newField);
			
			// add entity button
			ImageIcon attributeAddIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-circle-plus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
			attributeAddButton = new JButton("Add", attributeAddIcon);
			attributeAddButton.setToolTipText("Add a new attribute variable.");
			attributeAddButton.setPreferredSize(new Dimension(attributeAddButton.getPreferredSize().width, newField.getPreferredSize().height));
			attributeAddButton.setEnabled(false);
			buttonPanel.add(attributeAddButton);
			
			// document listener to react to changes in the new entity field
			newField.getDocument().addDocumentListener(new DocumentListener() {
				@Override
				public void changedUpdate(DocumentEvent e) {
					checkButton();
				}
				@Override
				public void insertUpdate(DocumentEvent e) {
					checkButton();
				}
				@Override
				public void removeUpdate(DocumentEvent e) {
					checkButton();
				}
				public void checkButton() {
					String newValue = newField.getText();
					if (newValue.matches("^\\s*$") || attributeListModel.contains(newValue) || newValue.length() > 190) {
						attributeAddButton.setEnabled(false);
					} else {
						attributeAddButton.setEnabled(true);
					}
				}
			});
			
			// action listener for the add attribute button
			attributeAddButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					try {
						Dna.sql.addAttributeVariable(variableId, newField.getText());
						attributeListModel.addElement(newField.getText());
						newField.setText("");
					} catch(Exception ex) {
						LogEvent le = new LogEvent(Logger.WARNING,
								"Could not add attribute variable.",
								"Could not add attribute variable to the database or list.",
								ex);
						Dna.logger.log(le);
					}
				}
			});

			// rename attribute button and action listener
			ImageIcon attributeRenameIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-forms.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
			attributeRenameButton = new JButton("Rename...", attributeRenameIcon);
			attributeRenameButton.setToolTipText("Rename the selected attribute variable.");
			attributeRenameButton.setPreferredSize(new Dimension(attributeRenameButton.getPreferredSize().width, newField.getPreferredSize().height));
			attributeRenameButton.setEnabled(false);
			buttonPanel.add(attributeRenameButton);
			
			attributeRenameButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					String oldAttributeVariableName = attributeList.getSelectedValue();
					JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(StatementTypeEditor.this);
					VariableDialog vd = new VariableDialog(frame, oldAttributeVariableName, "rename");
					if (!vd.canceled()) {
						String newAttributeVariableName = vd.getVariableName();
						boolean success = Dna.sql.updateAttributeVariableName(variableId, oldAttributeVariableName, newAttributeVariableName);
						if (success) {
							for (int i = 0; i < attributeList.getModel().getSize(); i++) {
								if (attributeListModel.get(i).equals(oldAttributeVariableName)) {
									attributeListModel.set(i, newAttributeVariableName);
									break;
								}
							}
						}
					}
				}
			});

			// delete attribute button and action listener
			ImageIcon deleteIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-circle-minus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
			attributeDeleteButton = new JButton("Delete", deleteIcon);
			attributeDeleteButton.setToolTipText("Delete the selected attribute variable (including all stored values).");
			attributeDeleteButton.setPreferredSize(new Dimension(attributeDeleteButton.getPreferredSize().width, newField.getPreferredSize().height));
			attributeDeleteButton.setEnabled(false);
			buttonPanel.add(attributeDeleteButton);
			
			attributeDeleteButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					int dialog = JOptionPane.showConfirmDialog(StatementTypeEditor.this, "Delete attribute variable \"" + attributeList.getSelectedValue() + "\" with all its data from the database?", "Confirmation", JOptionPane.YES_NO_OPTION);
					if (dialog == 0) {
						Dna.sql.deleteAttributeVariable(variableId, attributeList.getSelectedValue());
						attributeListModel.removeElement(attributeList.getSelectedValue());
					}
				}
			});

			// close button and action listener
			ImageIcon closeIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-x.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
			closeButton = new JButton("Close", closeIcon);
			closeButton.setToolTipText("Close this window.");
			closeButton.setPreferredSize(new Dimension(closeButton.getPreferredSize().width, newField.getPreferredSize().height));
			buttonPanel.add(closeButton);
			
			closeButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					AttributeDialog.this.dispose();
				}
			});
			contentsPanel.add(buttonPanel, BorderLayout.SOUTH);
			
			CompoundBorder border = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Attributes for: " + variableName));
			contentsPanel.setBorder(border);
			this.add(contentsPanel);
			
			this.pack();
			this.setLocationRelativeTo(null);
			this.setVisible(true);
		}
	}
}