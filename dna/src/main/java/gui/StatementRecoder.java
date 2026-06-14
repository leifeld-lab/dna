package gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.swing.BorderFactory;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.ScrollPaneConstants;
import javax.swing.UIDefaults;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;

import org.jdesktop.swingx.autocomplete.AutoCompleteDecorator;

import dna.Dna;
import model.Coder;
import model.Entity;
import model.Statement;
import model.StatementType;
import model.Value;

/**
 * Statement recoder. This class represents a dialog window in which the user
 * can recode or edit multiple statements, with a preview.
 */
public class StatementRecoder extends JDialog {
	private static final long serialVersionUID = -6224715694254322483L;
	private Container c;
	private int currentIndex = 0;
	private StatementType statementType;
	private StatementRecodeTableModel tableModel;
	private JComboBox<Coder> coderComboBox;
	private CoderComboBoxModel comboBoxModel;
	private JButton coderRevertButton;
	private JTextField idField, indexField;
	private JButton previousButton, nextButton;
	private ArrayList<Integer> variableIds;
	private HashMap<Integer, Integer> indexMap;
	private ArrayList<Component> components = new ArrayList<Component>();
	private ArrayList<JButton> revertButtons = new ArrayList<JButton>();
	private boolean changesApplied = false; // has the save button been pressed and confirmed?
	private ArrayList<Statement> changedStatements = null; // subset of edited statements after save or reset button pressed

	/**
	 * Constructor to create a new instance of the StatementRecoder dialog.
	 * 
	 * @param parent         The parent frame.
	 * @param statementIds   The IDs of the statements to edit.
	 * @param statementType  The statement type common to the statements.
	 */
	public StatementRecoder(Frame parent, int[] statementIds, StatementType statementType) {
		super(parent, "StatementRecorder", true);
		this.statementType = statementType;

		this.setTitle("Edit/recode multiple statements");
		this.setModal(true);
		ImageIcon statementIcon = new ImageIcon(getClass().getResource("/icons/tabler-icon-pencil.png"));
		this.setIconImage(statementIcon.getImage());
		c = getContentPane();

		JPanel dialogPanel = new JPanel(new BorderLayout());
		JPanel contentsPanel = new JPanel(new BorderLayout());
		
		/* Dialog layout:
		 * 
		 * NORTH of dialog panel: contents panel
		 * 
		 * - NORTH of contents panel: control panel
		 *   - west: navigation panel with previous/next buttons and ID
		 *   - east: button panel with reset, cancel, and save buttons
		 * 
		 * - SOUTH of contents panel: statement details panel
		 *   - coder combo box with apply all button
		 *   - combo boxes and other contents, with apply all buttons
		 * 
		 * CENTER of dialog panel: preview table panel
		 */
		
		// dialog panel CENTER: preview table panel
		ArrayList<Statement> statements = Dna.sql.getStatements(statementIds, this.statementType.getId(), null, null, null, false, null, false, null, false, null, false);
		final ArrayList<Statement> statementsBackup = statements.stream().map(s -> new Statement(s)).collect(Collectors.toCollection(ArrayList::new));
		tableModel = new StatementRecodeTableModel(Dna.sql.getStatements(statementIds, this.statementType.getId(), null, null, null, false, null, false, null, false, null, false), statementsBackup, statementType);
		JTable table = new JTable(tableModel);
		RecoderTableCellRenderer recoderTableCellRenderer = new RecoderTableCellRenderer();
		table.setDefaultRenderer(Coder.class, recoderTableCellRenderer);
		table.setDefaultRenderer(String.class, recoderTableCellRenderer);
		table.setDefaultRenderer(int.class, recoderTableCellRenderer);
		table.setDefaultRenderer(Integer.class, recoderTableCellRenderer);
		table.setDefaultRenderer(Entity.class, recoderTableCellRenderer);
		table.getTableHeader().setReorderingAllowed(false);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setFocusable(false); // no focus border around selected cell
		JScrollPane scrollPane = new JScrollPane(table, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		scrollPane.setPreferredSize(new Dimension(800, 200));
		JPanel tablePanel = new JPanel(new BorderLayout());
		tablePanel.add(scrollPane, BorderLayout.CENTER);
		CompoundBorder tablePanelBorder = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Preview"));
		tablePanel.setBorder(tablePanelBorder);
		table.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			@Override
			public void valueChanged(ListSelectionEvent e) {
				if (!e.getValueIsAdjusting()) {
					currentIndex = table.getSelectedRow();
					updateContents(currentIndex);
				}
			}
		});
		dialogPanel.add(tablePanel, BorderLayout.CENTER);
		
		// control panel WEST: navigation panel
		JPanel controlPanel = new JPanel(new BorderLayout());
		JPanel navigationPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
		int h = 20; // getting the text field height does not work properly on MacOS, so need to hard-code

		ImageIcon previousIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-caret-left.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		previousButton = new JButton(previousIcon);
		previousButton.setPreferredSize(new Dimension(h, h));
		previousButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (currentIndex > 0) {
					table.addRowSelectionInterval(currentIndex - 1, currentIndex - 1);
				}
			}
		});
		navigationPanel.add(previousButton);
		
		ImageIcon nextIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-caret-right.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		nextButton = new JButton(nextIcon);
		nextButton.setPreferredSize(new Dimension(h, h));
		nextButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (currentIndex < statementIds.length - 1) {
					table.addRowSelectionInterval(currentIndex + 1, currentIndex + 1);
				}
			}
		});
		navigationPanel.add(nextButton);
		
		JLabel indexLabel = new JLabel(" Showing");
		navigationPanel.add(indexLabel);
		
		indexField = new JTextField(1 + " / " + statementIds.length);
		int numStatementsLength = Integer.toString(statementIds.length).length();
		int columns = 2 * numStatementsLength + 3;
		indexField.setColumns(columns);
		indexField.setPreferredSize(new Dimension(indexField.getPreferredSize().width, h));
		indexField.setEditable(false);
		navigationPanel.add(indexField);

		JLabel idLabel = new JLabel(" ID");
		navigationPanel.add(idLabel);
		
		idField = new JTextField(Integer.toString(tableModel.getRow(currentIndex).getId()));
		int digits = Integer.toString(IntStream.of(statementIds).max().getAsInt()).length();
		idField.setColumns(digits);
		idField.setPreferredSize(new Dimension(idField.getPreferredSize().width, h));
		idField.setEditable(false);
		navigationPanel.add(idField);

		controlPanel.add(navigationPanel, BorderLayout.WEST);
		
		// control panel EAST: button panel
		JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
		JButton resetButton = new JButton("Reset");
		resetButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				changedStatements = tableModel.getChangedStatements();
				if (changedStatements.size() > 0) {
					String messagePart = " modified statements to their";
					if (changedStatements.size() == 1) {
						messagePart = " modified statement to its";
					}
					int dialog = JOptionPane.showConfirmDialog(StatementRecoder.this, "Revert " + changedStatements.size() + messagePart + " original state?", "Confirmation", JOptionPane.YES_NO_OPTION);
					if (dialog == 0) {
						int oldIndex = currentIndex;
						tableModel.revertAllRows();
						currentIndex = oldIndex;
						table.setRowSelectionInterval(currentIndex, currentIndex);
					}
				}
			}
		});
		buttonPanel.add(resetButton);
		JButton cancelButton = new JButton("Cancel");
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				StatementRecoder.this.dispose();
			}
		});
		buttonPanel.add(cancelButton);
		JButton saveButton = new JButton("Save");
		buttonPanel.add(saveButton);
		saveButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				changedStatements = tableModel.getChangedStatements();
				if (changedStatements.size() > 0) {
					String messagePart = "s";
					if (changedStatements.size() == 1) {
						messagePart = "";
					}
					int dialog = JOptionPane.showConfirmDialog(StatementRecoder.this, "Save " + changedStatements.size() + " modified statement" + messagePart + " to the database?", "Confirmation", JOptionPane.YES_NO_OPTION);
					if (dialog == 0) {
						ArrayList<Integer> ids = changedStatements.stream().map(s -> Integer.valueOf(s.getId())).collect(Collectors.toCollection(ArrayList::new));
						ArrayList<Integer> coderIds = changedStatements.stream().map(s -> Integer.valueOf(s.getCoderId())).collect(Collectors.toCollection(ArrayList::new));
						ArrayList<ArrayList<Value>> values = changedStatements.stream().map(s -> s.getValues()).collect(Collectors.toCollection(ArrayList::new));
						Dna.sql.updateStatements(ids, values, coderIds);
						changesApplied = true;
						StatementRecoder.this.dispose();
					}
				}
			}
		});
		controlPanel.add(buttonPanel, BorderLayout.EAST);
		
		CompoundBorder controlBorder = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Navigation and control"));
		controlPanel.setBorder(controlBorder);
		contentsPanel.add(controlPanel, BorderLayout.NORTH);
		
		// statement details panel CENTER: statement fields and their buttons
		JPanel statementDetailsPanel = new JPanel(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		
		gbc.insets = new Insets(3, 3, 3, 3);
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.anchor = GridBagConstraints.EAST;
		gbc.weightx = 0;
		
		JLabel coderLabel = new JLabel("coder", JLabel.TRAILING);
		statementDetailsPanel.add(coderLabel, gbc);
		
		gbc.weightx = 1;
		gbc.gridx++;
		coderComboBox = new JComboBox<Coder>();
		ArrayList<Coder> coders = Dna.sql.getCoders()
				.stream()
				.filter(c -> (Dna.sql.getActiveCoder().isPermissionEditOthersStatements(c.getId()) || Dna.sql.getActiveCoder().getId() == c.getId()))
				.collect(Collectors.toCollection(ArrayList::new)); // get coders for whom active coder has permission
		comboBoxModel = new CoderComboBoxModel(coders);
		coderComboBox.setModel(comboBoxModel);
		coderComboBox.setRenderer(new CoderComboBoxRenderer(9, 0, 22));
		coderComboBox.setSelectedIndex(comboBoxModel.getIndexByCoderId(tableModel.getRow(currentIndex).getCoderId()));
		coderComboBox.setPreferredSize(new Dimension(coderComboBox.getPreferredSize().width, h));
		coderComboBox.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				// save changed coder in table model
				Coder selectedCoder = (Coder) coderComboBox.getSelectedItem();
				tableModel.updateCoder(currentIndex, selectedCoder.getId(), selectedCoder.getName(), selectedCoder.getColor());
				
				// toggle coder revert button if necessary
				if (tableModel.getRow(currentIndex).getCoderId() == tableModel.getBackupRow(currentIndex).getCoderId()) {
					coderRevertButton.setEnabled(false);
				} else {
					coderRevertButton.setEnabled(true);
				}
			}
		});
		statementDetailsPanel.add(coderComboBox, gbc);
		
		gbc.weightx = 0;
		gbc.gridx++;
		ImageIcon revertIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-rotate-clockwise.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		coderRevertButton = new JButton(revertIcon);
		coderRevertButton.setPreferredSize(new Dimension(h, h));
		coderRevertButton.setToolTipText("Revert coder to the original coder.");
		coderRevertButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				tableModel.revertCoder(currentIndex);
				coderComboBox.setSelectedIndex(comboBoxModel.getIndexByCoderId(tableModel.getRow(currentIndex).getCoderId()));
				coderComboBox.repaint();
			}
		});
		coderRevertButton.setEnabled(false);
		statementDetailsPanel.add(coderRevertButton, gbc);
		
		gbc.gridx++;
		ImageIcon applyAllIcon = new SvgIcon("/icons/tabler_copy.svg", 18).getImageIcon();
		JButton applyAllCoderButton = new JButton(applyAllIcon);
		applyAllCoderButton.setPreferredSize(new Dimension(h, h));
		applyAllCoderButton.setToolTipText("Apply this coder to all " + statementIds.length + " selected statements.");
		applyAllCoderButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int oldIndex = currentIndex;
				tableModel.applyAllCoder(currentIndex);
				currentIndex = oldIndex;
				table.setRowSelectionInterval(currentIndex, currentIndex);
			}
		});
		statementDetailsPanel.add(applyAllCoderButton, gbc);
		gbc.gridx = 0;
		gbc.gridy++;
		
		// add fields for the values
		variableIds = new ArrayList<Integer>();
		indexMap = new HashMap<Integer, Integer>();
		int counter = 0;
		for (int i = 0; i < statementType.getVariables().size(); i++) {
			if (statementType.getVariables().get(i).getDataType().equals("short text")) {
				indexMap.put(i, counter);
				counter++;
				variableIds.add(statementType.getVariables().get(i).getVariableId());
			}
		}
		ArrayList<ArrayList<Entity>> entities = Dna.sql.getEntities(variableIds, false); // switch to true to color unused entities in red; but it takes much longer with large databases

		// create boxes with values
		for (int i = 0; i < statementType.getVariables().size(); i++) {
			String key = statementType.getVariables().get(i).getKey();
			String dataType = statementType.getVariables().get(i).getDataType();
			JLabel label = new JLabel(key, JLabel.TRAILING);

			// The table model may store the variables in a different order; look up the index in the variables
			int tableModelIndex = -1;
			for (int v = 0; v < tableModel.getRow(currentIndex).getValues().size(); v++) {
				if (key.equals(tableModel.getRow(currentIndex).getValues().get(v).getKey())) {
					tableModelIndex = v;
				}
			}
			if (tableModelIndex == -1) {
				tableModelIndex = i;
			}
			final int k = tableModelIndex;

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

				JButton revertButton = new JButton(revertIcon);
				revertButton.setPreferredSize(new Dimension(h, h));
				revertButton.setToolTipText("Revert the value to the original version.");
				revertButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						Entity b = (Entity) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						box.setSelectedItem(b);
						tableModel.updateValue(currentIndex, k, b);
					}
				});
				revertButton.setEnabled(false);
				revertButtons.add(revertButton);
				
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
						for (int j = 0; j < box.getModel().getSize(); j++) {
							if (((JTextField) box.getEditor().getEditorComponent()).getText().equals(box.getModel().getElementAt(j).getValue())) {
								fg = box.getModel().getElementAt(j).getColor().toAWTColor();
							}
						}
						((JTextField) box.getEditor().getEditorComponent()).setSelectedTextColor(fg);
						((JTextField) box.getEditor().getEditorComponent()).setForeground(fg);
					}
				});
				
				if (Dna.sql.getActiveCoder().isPopupAutoComplete()) {
					AutoCompleteDecorator.decorate(box); // auto-complete short text values; part of SwingX
				}
    			box.setPreferredSize(new Dimension(Dna.sql.getActiveCoder().getPopupWidth(), 20));
    			box.setSelectedItem((Entity) tableModel.getRow(currentIndex).getValues().get(k).getValue());
    			
    			// need to add a focus listener to save the contents; otherwise without auto-completion the last edited box will not be saved
    			box.getEditor().getEditorComponent().addFocusListener(new FocusListener() {
    				
					@Override
					public void focusGained(FocusEvent e) {
						// no action needed when focus is gained
					}
					
					@Override
					public void focusLost(FocusEvent e) {
						// save value
						box.setSelectedItem(box.getEditor().getItem());
						Entity entity;
						if (box.getSelectedItem().getClass().getName().endsWith("String")) { // if not an existing entity, the editor returns a String
							String s = (String) box.getSelectedItem();
							if (s.length() > 0 && s.matches("^\\s+$")) { // replace a (multiple) whitespace string by an empty string
								s = "";
							}
							s = s.substring(0, Math.min(190, s.length()));
							entity = new Entity(s); // the new entity has an ID of -1; the SQL class needs to take care of this when writing into the database
						} else {
							entity = (Entity) box.getSelectedItem();
						}
						Entity originalEntity = (Entity) tableModel.getRow(currentIndex).getValues().get(k).getValue();
						String originalText = originalEntity.getValue();
						if (!entity.getValue().equals(originalText)) {
							tableModel.updateValue(currentIndex, k, entity);
						}

						// toggle revert button (enabled or disabled), depending on whether the value has changed
						Entity backupEntity = (Entity) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						String backupText = backupEntity.getValue();
						if (entity.getValue().equals(backupText)) {
							revertButton.setEnabled(false);
						} else {
							revertButton.setEnabled(true);
						}
					}
    			});
    			
    			components.add(box);
    			
				gbc.anchor = GridBagConstraints.EAST;
				statementDetailsPanel.add(label, gbc);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.gridx++;
				gbc.weightx = 1;
				statementDetailsPanel.add(box, gbc);

				gbc.weightx = 0;
				gbc.gridx++;
				statementDetailsPanel.add(revertButton, gbc);
				
				gbc.gridx++;
				JButton applyAllButton = new JButton(applyAllIcon);
				applyAllButton.setPreferredSize(new Dimension(h, h));
				applyAllButton.setToolTipText("Apply this entity or pattern to all " + statementIds.length + " selected statements.");
				applyAllButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						int oldIndex = currentIndex;
						tableModel.applyAll(currentIndex, k);
						currentIndex = oldIndex;
						table.setRowSelectionInterval(currentIndex, currentIndex);
					}
				});
				statementDetailsPanel.add(applyAllButton, gbc);
				gbc.gridx = 0;
				gbc.gridy++;
			} else if (dataType.equals("long text")) {
				String entry = (String) tableModel.getRow(currentIndex).getValues().get(k).getValue();
    			JTextArea box = new JTextArea();
    			box.setEditable(true);
    			box.setWrapStyleWord(true);
    			box.setLineWrap(true);
    			box.setText(entry);
    			JScrollPane boxScroller = new JScrollPane(box);
    			boxScroller.setPreferredSize(new Dimension(Dna.sql.getActiveCoder().getPopupWidth(), 100));
    			boxScroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);

    			components.add(box);

				JButton revertButton = new JButton(revertIcon);
				revertButton.setPreferredSize(new Dimension(h, h));
				revertButton.setToolTipText("Revert the value to the original version.");
				revertButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						String b = (String) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						box.setText(b);
						tableModel.updateValue(currentIndex, k, b);
					}
				});
				revertButton.setEnabled(false);
				revertButtons.add(revertButton);

				// add a document listener to the text area to toggle the revert button enabled or disabled
				box.getDocument().addDocumentListener(new DocumentListener() {
					@Override
					public void changedUpdate(DocumentEvent e) {
						toggleRevertButton();
					}
					@Override
					public void insertUpdate(DocumentEvent e) {
						toggleRevertButton();
					}
					@Override
					public void removeUpdate(DocumentEvent e) {
						toggleRevertButton();
					}
					private void toggleRevertButton() {
						// toggle revert button
						String backupText = (String) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						if (box.getText().equals(backupText)) {
							revertButton.setEnabled(false);
						} else {
							revertButton.setEnabled(true);
						}
					}
				});

    			// need to add a focus listener to save the contents
    			box.addFocusListener(new FocusListener() {
					@Override
					public void focusGained(FocusEvent e) {
						// no action needed when focus is gained
					}
					
					@Override
					public void focusLost(FocusEvent e) {
						// save value
						if (!box.getText().equals(tableModel.getRow(currentIndex).getValues().get(k).getValue())) {
							tableModel.updateValue(currentIndex, k, box.getText());
						}
					}
    			});
    			
				gbc.anchor = GridBagConstraints.NORTHEAST;
				statementDetailsPanel.add(label, gbc);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.weightx = 1;
				gbc.gridx++;
				statementDetailsPanel.add(boxScroller, gbc);

				gbc.weightx = 0;
				gbc.gridx++;
				statementDetailsPanel.add(revertButton, gbc);
				
				gbc.gridx++;
				JButton applyAllButton = new JButton(applyAllIcon);
				applyAllButton.setPreferredSize(new Dimension(h, h));
				applyAllButton.setToolTipText("Apply this text or pattern to all " + statementIds.length + " selected statements.");
				applyAllButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						int oldIndex = currentIndex;
						tableModel.applyAll(currentIndex, k);
						currentIndex = oldIndex;
						table.setRowSelectionInterval(currentIndex, currentIndex);
					}
				});
				statementDetailsPanel.add(applyAllButton, gbc);
				gbc.gridx = 0;
				gbc.gridy++;
			} else if (dataType.equals("boolean")) {
				int entry = (Integer) tableModel.getRow(currentIndex).getValues().get(k).getValue();
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

    			components.add(buttons);

				JButton revertButton = new JButton(revertIcon);
				revertButton.setPreferredSize(new Dimension(h, h));
				revertButton.setToolTipText("Revert the value to the original version.");
				revertButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						int b = (int) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						buttons.setYes(b == 1);
						tableModel.updateValue(currentIndex, k, b);
						revertButton.setEnabled(false);
					}
				});
				revertButton.setEnabled(false);
				revertButtons.add(revertButton);

    			ActionListener l = new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						// save value
						boolean value = buttons.isYes();
						int valueInt = 0;
						if (value) {
							valueInt = 1;
						}
						int originalValue = (int) tableModel.getRow(currentIndex).getValues().get(k).getValue();
						if (valueInt != originalValue) {
							tableModel.updateValue(currentIndex, k, valueInt);
						}
						
						// toggle revert button
						int b = (int) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						if (valueInt == b) {
							revertButton.setEnabled(false);
						} else {
							revertButton.setEnabled(true);
						}
					}
    			};
    			buttons.getYesButton().addActionListener(l);
    			buttons.getNoButton().addActionListener(l);
    			
				gbc.anchor = GridBagConstraints.EAST;
				statementDetailsPanel.add(label, gbc);
				gbc.insets = new Insets(0, 0, 0, 0);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.weightx = 1;
				gbc.gridx++;
				statementDetailsPanel.add(buttons, gbc);

				gbc.insets = new Insets(3,3,3,3);
				gbc.weightx = 0;
				gbc.gridx++;
				statementDetailsPanel.add(revertButton, gbc);
				
				gbc.gridx++;
				JButton applyAllButton = new JButton(applyAllIcon);
				applyAllButton.setPreferredSize(new Dimension(h, h));
				applyAllButton.setToolTipText("Apply this Boolean value to all " + statementIds.length + " selected statements.");
				applyAllButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						int oldIndex = currentIndex;
						tableModel.applyAll(currentIndex, k);
						currentIndex = oldIndex;
						table.setRowSelectionInterval(currentIndex, currentIndex);
					}
				});
				statementDetailsPanel.add(applyAllButton, gbc);
				gbc.gridx = 0;
				gbc.gridy++;
			} else if (dataType.equals("integer")) {
				int entry = (Integer) tableModel.getRow(currentIndex).getValues().get(k).getValue();
				JSpinner jsp = new JSpinner();
				jsp.setValue(entry);
    			jsp.setPreferredSize(new Dimension(70, 20));
    			jsp.setEnabled(true);
    			JPanel jp = new JPanel(new FlowLayout(FlowLayout.LEFT));
    			jp.add(jsp);

    			components.add(jsp);

				JButton revertButton = new JButton(revertIcon);
				revertButton.setPreferredSize(new Dimension(h, h));
				revertButton.setToolTipText("Revert the value to the original version.");
				revertButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						int b = (int) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						jsp.setValue(b);
						tableModel.updateValue(currentIndex, k, b);
						revertButton.setEnabled(false);
					}
				});
				revertButton.setEnabled(false);
				revertButtons.add(revertButton);

				ChangeListener l = new ChangeListener() {
					@Override
					public void stateChanged(ChangeEvent e) {
						// save value
						int value = (int) jsp.getValue();
						int originalValue = (int) tableModel.getRow(currentIndex).getValues().get(k).getValue();
						if (value != originalValue) {
							tableModel.updateValue(currentIndex, k, value);
						}
						
						// toggle revert button
						int b = (int) tableModel.getBackupRow(currentIndex).getValues().get(k).getValue();
						if (value == b) {
							revertButton.setEnabled(false);
						} else {
							revertButton.setEnabled(true);
						}
					}
				};
				jsp.addChangeListener(l);
				
				gbc.anchor = GridBagConstraints.EAST;
				statementDetailsPanel.add(label, gbc);
				gbc.insets = new Insets(0, 0, 0, 0);
				gbc.anchor = GridBagConstraints.WEST;
				gbc.weightx = 1;
				gbc.gridx++;
				statementDetailsPanel.add(jp, gbc);

				gbc.insets = new Insets(3, 3, 3, 3);
				gbc.weightx = 0;
				gbc.gridx++;
				statementDetailsPanel.add(revertButton, gbc);
				
				gbc.gridx++;
				JButton applyAllButton = new JButton(applyAllIcon);
				applyAllButton.setPreferredSize(new Dimension(h, h));
				applyAllButton.setToolTipText("Apply this integer value to all " + statementIds.length + " selected statements.");
				applyAllButton.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						int oldIndex = currentIndex;
						tableModel.applyAll(currentIndex, k);
						currentIndex = oldIndex;
						table.setRowSelectionInterval(currentIndex, currentIndex);
					}
				});
				statementDetailsPanel.add(applyAllButton, gbc);
				gbc.gridx = 0;
				gbc.gridy++;
			}
		}
		table.addRowSelectionInterval(0, 0);

		CompoundBorder statementDetailsBorder = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Statement details"));
		statementDetailsPanel.setBorder(statementDetailsBorder);
		contentsPanel.add(statementDetailsPanel, BorderLayout.SOUTH);
		dialogPanel.add(contentsPanel, BorderLayout.NORTH);
		
		c.add(dialogPanel);
		this.pack();
		this.setLocationRelativeTo(null);
		this.setVisible(true);
	}
	
	/**
	 * Update the content components when a new statement is loaded.
	 * 
	 * @param statementIndex The index of the statement that is loaded.
	 */
	@SuppressWarnings("unchecked")
	private void updateContents(int statementIndex) {
		if (statementIndex > -1) { // this can happen because fireTableDataChanged in the table model triggers a deselection of all rows
			// GUI elements in the upper part of the window, including coder combo box and controls
			idField.setText(Integer.toString(tableModel.getRow(currentIndex).getId()));
			indexField.setText((currentIndex + 1) + " / " + tableModel.getRowCount());
			coderComboBox.setSelectedIndex(comboBoxModel.getIndexByCoderId(tableModel.getRow(currentIndex).getCoderId()));
			coderComboBox.repaint();
			previousButton.setEnabled(currentIndex > 0);
			nextButton.setEnabled(currentIndex < tableModel.getRowCount() - 1);
			
			// content combo boxes
			ArrayList<ArrayList<Entity>> entities = Dna.sql.getEntities(variableIds, false); // switch to true to color unused entities in red; but it takes much longer with large databases
			for (int i = 0; i < statementType.getVariables().size(); i++) {
				String dataType = statementType.getVariables().get(i).getDataType();
				String key = statementType.getVariables().get(i).getKey();

				// The table model may store the variables in a different order; look up the index in the variables
				int tableModelIndex = -1;
				for (int v = 0; v < tableModel.getRow(currentIndex).getValues().size(); v++) {
					if (key.equals(tableModel.getRow(currentIndex).getValues().get(v).getKey())) {
						tableModelIndex = v;
					}
				}
				if (tableModelIndex == -1) {
					tableModelIndex = i;
				}
				final int k = tableModelIndex;

				if (dataType.equals("short text")) {
					// load value
					Entity[] entitiesArray = new Entity[entities.get(indexMap.get(i)).size()];
					entitiesArray = entities.get(indexMap.get(i)).toArray(entitiesArray);
					ComboBoxModel<Entity> model = new DefaultComboBoxModel<Entity>(entitiesArray);
					((JComboBox<Entity>) components.get(i)).setModel(model);
					Entity value = (Entity) tableModel.getRow(currentIndex).getValues().get(tableModelIndex).getValue();
					((JComboBox<Entity>) components.get(i)).getModel().setSelectedItem(value);
					
					// toggle revert button
					Entity backupEntity = (Entity) tableModel.getBackupRow(currentIndex).getValues().get(tableModelIndex).getValue();
					if (value.getValue().equals(backupEntity.getValue())) {
						revertButtons.get(i).setEnabled(false);
					} else {
						revertButtons.get(i).setEnabled(true);
					}
				} else if (dataType.equals("long text")) {
					// load value
					String value = (String) tableModel.getRow(currentIndex).getValues().get(tableModelIndex).getValue();
					((JTextArea) components.get(i)).setText(value);

					// toggle revert button
					String backupText = (String) tableModel.getBackupRow(currentIndex).getValues().get(tableModelIndex).getValue();
					if (value.equals(backupText)) {
						revertButtons.get(i).setEnabled(false);
					} else {
						revertButtons.get(i).setEnabled(true);
					}
				} else if (dataType.equals("boolean")) {
					// load value
					((BooleanButtonPanel) components.get(i)).setYes((Integer) tableModel.getRow(currentIndex).getValues().get(tableModelIndex).getValue() != 0);

					// toggle revert button
					boolean b = ((int) tableModel.getBackupRow(currentIndex).getValues().get(tableModelIndex).getValue()) == 1;
					if (((BooleanButtonPanel) components.get(i)).isYes() == b) {
						revertButtons.get(i).setEnabled(false);
					} else {
						revertButtons.get(i).setEnabled(true);
					}
				} else if (dataType.equals("integer")) {
					// load value
					int value = (int) tableModel.getRow(currentIndex).getValues().get(tableModelIndex).getValue();
					((JSpinner) components.get(i)).setValue(value);

					// toggle revert button
					int b = (int) tableModel.getBackupRow(currentIndex).getValues().get(tableModelIndex).getValue();
					if (value == b) {
						revertButtons.get(i).setEnabled(false);
					} else {
						revertButtons.get(i).setEnabled(true);
					}
				}
			}
		}
	}
	
	/**
	 * Has the save button been pressed and confirmed?
	 * 
	 * @return Boolean indicating if the changes should be saved.
	 */
	public boolean isChangesApplied() {
		return changesApplied;
	}
	
	/**
	 * Get the changed statements after the save or reset button was pressed.
	 * 
	 * @return Array list of the statements that were modified.
	 */
	public ArrayList<Statement> getChangedStatements() {
		return changedStatements;
	}
	
	/**
	 * Table model for the statement recode table.
	 */
	private class StatementRecodeTableModel extends AbstractTableModel {
		private static final long serialVersionUID = -2971964746106990839L;
		private ArrayList<Statement> statements;
		private final ArrayList<Statement> statementBackup;
		private StatementType statementType;
		
		public StatementRecodeTableModel(ArrayList<Statement> statements,
				final ArrayList<Statement> statementBackup,
				StatementType statementType) {
			this.statements = statements;
			this.statementBackup = statementBackup;
			this.statementType = statementType;
		}

		@Override
		public int getColumnCount() {
			return statementType.getVariables().size() + 2;
		}

		@Override
		public int getRowCount() {
			return this.statements.size();
		}

		@Override
		public Object getValueAt(int row, int col) {
			if (col == 0) {
				return this.statements.get(row).getId();
			} else if (col == statementType.getVariables().size() + 1) {
				Coder coder = new Coder(this.statements.get(row).getCoderId(),
						this.statements.get(row).getCoderName(),
						this.statements.get(row).getCoderColor());
				return coder;
			} else if (col > 0 && col < this.statements.get(row).getValues().size() + 1) {
				return this.statements.get(row).getValues()
						.stream()
						.filter(v -> v.getVariableId() == statementType.getVariables().get(col - 1).getVariableId())
						.map(v -> {
							if (v.getDataType().equals("boolean")) {
								if (((int) v.getValue()) == 1) {
									return "yes";
								} else {
									return "no";
								}
							} else {
								return v.getValue();
							}
						})
						.findFirst()
						.get();
			} else {
				return null;
			}
		}

		/**
		 * Get a statement specified by a model row index.
		 * 
		 * @param row  The row index.
		 * @return     The statement corresponding to the row index.
		 */
		public Statement getRow(int row) {
			return this.statements.get(row);
		}
		
		/**
		 * Get a statement backup specified by a model row index.
		 * 
		 * @param row  The row index.
		 * @return     The statement backup corresponding to the row index.
		 */
		public Statement getBackupRow(int row) {
			return this.statementBackup.get(row);
		}

		/**
		 * Is the cell editable?
		 * 
		 * @param row  The row index.
		 * @param col  The column index.
		 * @return     Is the cell editable?
		 */
		public boolean isCellEditable(int row, int col) {
			return false;
		}

		/**
		 * Get the name of a column.
		 * 
		 * @param col  The column index.
		 */
		public String getColumnName(int col) {
			if (col == 0) {
				return "ID";
			} else if (col == statementType.getVariables().size() + 1) {
				return "Coder";
			} else {
				return this.statementType.getVariables().get(col - 1).getKey();
			}
		}
		
		/**
		 * Get the class of a column.
		 * 
		 * @param col  The column index.
		 */
		public Class<?> getColumnClass(int col) {
			if (col == 0) {
				return Integer.class;
			} else if (col == statementType.getVariables().size() + 1) {
				return Coder.class;
			} else if (statementType.getVariables().get(col - 1).getDataType().equals("short text")) {
				return Entity.class;
			} else if (statementType.getVariables().get(col - 1).getDataType().equals("long text")) {
				return String.class;
			} else if (statementType.getVariables().get(col - 1).getDataType().equals("boolean")) {
				return String.class;
			} else if (statementType.getVariables().get(col - 1).getDataType().equals("integer")) {
				return int.class;
			} else {
				return String.class;
			}
		}
		
		/**
		 * Apply the value from a variable in a specific statement to all
		 * statements.
		 * 
		 * @param row            Statement index.
		 * @param variableIndex  Variable index.
		 */
		public void applyAll(int row, int variableIndex) {
			Object value = statements.get(row).getValues().get(variableIndex).getValue();
			statements.stream().forEach(s -> s.getValues().get(variableIndex).setValue(value));
			fireTableDataChanged();
		}
		
		/**
		 * Apply the coder from a variable in a specific statement to all
		 * statements.
		 * 
		 * @param row            Statement index from which to take the coder.
		 */
		public void applyAllCoder(int row) {
			int coderId = statements.get(row).getCoderId();
			model.Color coderColor = statements.get(row).getCoderColor();
			String coderName = statements.get(row).getCoderName();
			statements.stream().forEach(s -> {
				s.setCoderId(coderId);
				s.setCoderColor(coderColor);
				s.setCoderName(coderName);
			});
			fireTableDataChanged();
		}
		
		/**
		 * Update the coder in a statement.
		 * 
		 * @param row        Statement index in this table model.
		 * @param coderId    The new coder ID.
		 * @param coderName  The new coder name.
		 * @param coderColor The new coder color.
		 */
		public void updateCoder(int row, int coderId, String coderName, model.Color coderColor) {
			statements.get(row).setCoderId(coderId);
			statements.get(row).setCoderName(coderName);
			statements.get(row).setCoderColor(coderColor);
			this.fireTableRowsUpdated(row, row);
		}

		/**
		 * Revert the coder in a statement to its backup.
		 * 
		 * @param row  Statement index in this table model.
		 */
		public void revertCoder(int row) {
			statements.get(row).setCoderId(statementBackup.get(row).getCoderId());
			statements.get(row).setCoderName(statementBackup.get(row).getCoderName());
			statements.get(row).setCoderColor(statementBackup.get(row).getCoderColor());
			this.fireTableRowsUpdated(row, row);
		}
		
		/**
		 * Update a variable value for a specific statement.
		 * 
		 * @param row            The statement index.
		 * @param variableIndex  The index of the variable.
		 * @param object         The new value to set.
		 */
		public void updateValue(int row, int variableIndex, Object object) {
			statements.get(row).getValues().get(variableIndex).setValue(object);
			this.fireTableRowsUpdated(row, row);
		}
		
		/**
		 * Clone all backup statements and save as rows. I.e., revert any edited
		 * statements to their backup state.
		 */
		public void revertAllRows() {
			statements = statementBackup.stream().map(s -> new Statement(s)).collect(Collectors.toCollection(ArrayList::new));
			this.fireTableDataChanged();
		}
		
		/**
		 * Check if a table cell has been modified compared to the backup
		 * version.
		 * 
		 * @param row     Table row.
		 * @param column  Table column.
		 * @return        Has the cell been updated?
		 */
		public boolean isCellChanged(int row, int column) {
			if (column == 0) {
				return false;
			} else if (column == statementType.getVariables().size() + 1) {
				if (statements.get(row).getCoderId() != statementBackup.get(row).getCoderId()) {
					return true;
				} else {
					return false;
				}
			} else {
				int columnVariableId = statementType.getVariables().get(column - 1).getVariableId();
				Value v1 = this.statements.get(row).getValueByVariableId(columnVariableId);
				Value v2 = this.statementBackup.get(row).getValueByVariableId(columnVariableId);
				if (v1.equals(v2)) {
					return false;
				} else {
					return true;
				}
			}
		}
		
		/**
		 * Return an array list of statement IDs of the statements that were
		 * edited.
		 * 
		 * @return Array list of statement IDs that have been changed.
		 */
		public ArrayList<Statement> getChangedStatements() {
			ArrayList<Integer> changedIds = new ArrayList<Integer>();
			String type;
			for (int i = 0; i < statements.size(); i++) {
				for (int j = 0; j < statements.get(i).getValues().size(); j++) {
					type = statements.get(i).getValues().get(j).getDataType();
					if (type.equals("short text")) {
						Entity newEntity = (Entity) statements.get(i).getValues().get(j).getValue();
						Entity oldEntity = (Entity) statementBackup.get(i).getValues().get(j).getValue();
						if (!((String) newEntity.getValue()).equals((String) oldEntity.getValue())) {
							changedIds.add(statements.get(i).getId());
							break;
						}
					} else if (type.equals("long text")) {
						if (!((String) statements.get(i).getValues().get(j).getValue()).equals((String) statementBackup.get(i).getValues().get(j).getValue())) {
							changedIds.add(statements.get(i).getId());
							break;
						}
					} else {
						if ((int) statements.get(i).getValues().get(j).getValue() != (int) statementBackup.get(i).getValues().get(j).getValue()) {
							changedIds.add(statements.get(i).getId());
							break;
						}
					}
				}
				if (statements.get(i).getCoderId() != statementBackup.get(i).getCoderId() && !changedIds.contains(statements.get(i).getCoderId())) {
					changedIds.add(statements.get(i).getId());
				}
			}
			ArrayList<Statement> changedStatements = statements.stream().filter(s -> changedIds.contains(s.getId())).collect(Collectors.toCollection(ArrayList::new));
			return changedStatements;
		}
	}
	
	/**
	 * Table cell renderer for the statement recoder preview table. It adds
	 * a background color for modified cells.
	 */
	private class RecoderTableCellRenderer extends DefaultTableCellRenderer {
		private static final long serialVersionUID = -114843491102801089L;

		@Override
		public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
			int modelRow = table.convertRowIndexToModel(row);
			int modelColumn = table.convertColumnIndexToModel(column);
			Class<?> colClass = tableModel.getColumnClass(column);
			Color updatedColor = new Color(255, 102, 102);
			UIDefaults defaults = javax.swing.UIManager.getDefaults();
			Color selectedColor = defaults.getColor("Table.selectionBackground");
			Color defaultColor = defaults.getColor("Table.background");
			Color selectedUpdatedColor = new Color(
					(updatedColor.getRed() + selectedColor.getRed()) / 2,
					(updatedColor.getGreen() + selectedColor.getGreen()) / 2,
					(updatedColor.getBlue() + selectedColor.getBlue()) / 2,
					(updatedColor.getAlpha() + selectedColor.getAlpha()) / 2);
			DefaultTableCellRenderer renderer = (DefaultTableCellRenderer) super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (value == null) {
				return new JLabel("");
            } else if (colClass.equals(Coder.class)) {
				Coder coder = (Coder) value;
				CoderBadgePanel cbp = new CoderBadgePanel(coder, 13, 1, 22);
				if (tableModel.isCellChanged(row, column) && isSelected) {
					cbp.setBackground(selectedUpdatedColor);
				} else if (tableModel.isCellChanged(row, column)) {
					cbp.setBackground(updatedColor);
				} else if (isSelected) {
					cbp.setBackground(selectedColor);
				} else {
					cbp.setBackground(defaultColor);
				}
				return cbp;
			} else {
				if (colClass.equals(Integer.class)) {
					renderer.setHorizontalAlignment(JLabel.RIGHT);
				} else {
					renderer.setHorizontalAlignment(JLabel.LEFT);
				}
				if (column == 0) {
					if (isSelected) {
						renderer.setBackground(selectedColor);
					} else {
						renderer.setBackground(defaultColor);
					}
				} else if (tableModel.isCellChanged(modelRow, modelColumn) && isSelected) {
					renderer.setBackground(selectedUpdatedColor);
				} else if (tableModel.isCellChanged(modelRow, modelColumn)) {
					renderer.setBackground(updatedColor);
				} else if (isSelected) {
					renderer.setBackground(selectedColor);
				} else {
					renderer.setBackground(defaultColor);
				}
				return renderer;
			}
		}
	}
}