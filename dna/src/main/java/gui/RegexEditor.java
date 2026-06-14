package gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.util.ArrayList;
import java.util.Collections;

import javax.swing.AbstractListModel;
import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JColorChooser;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.jdesktop.swingx.JXTextField;

import dna.Dna;
import model.Regex;

class RegexEditor extends JDialog {
	private static final long serialVersionUID = -1587444508551756607L;
	private JButton addButton, removeButton;
	private JXTextField textField;
	private RegexListModel regexListModel;
	private JList<Regex> regexList;
	private boolean changed;

	public RegexEditor() {
		this.setModal(true);
		this.setTitle("Regex editor");
        ImageIcon regexEditorIcon = new SvgIcon("/icons/google_regular_expression.svg", 32).getImageIcon();
		this.setIconImage(regexEditorIcon.getImage());

		this.changed = false;
		
		JPanel dialogPanel = new JPanel(new BorderLayout());
		CompoundBorder border = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Regular expressions"));
		dialogPanel.setBorder(border);

		// list
		regexListModel = new RegexListModel();
		regexList = new JList<Regex>(regexListModel);
		regexList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		regexList.setLayoutOrientation(JList.VERTICAL);
		regexList.setVisibleRowCount(20);
		regexList.setCellRenderer(new RegexListRenderer());
		regexList.setBorder(BorderFactory.createLineBorder(Color.BLACK, 1));
		regexList.addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				if (regexList.getModel().getSize() == 0 || regexList.isSelectionEmpty() 
						|| !Dna.sql.getActiveCoder().isPermissionEditRegex()) {
					removeButton.setEnabled(false);
				} else {
					removeButton.setEnabled(true);
				}
			}
		});
		JScrollPane listScroller = new JScrollPane(regexList);
		listScroller.setPreferredSize(new Dimension(200, 400));
		listScroller.setBorder(new EmptyBorder(10, 10, 10, 10));
		dialogPanel.add(listScroller, BorderLayout.CENTER);
		
		// entry panel: color button
		JPanel entryPanel = new JPanel(new BorderLayout(5, 0));
		entryPanel.setBorder(new EmptyBorder(5, 10, 5, 10));
		ColorButton colorButton = new ColorButton();
		colorButton.setBorder(BorderFactory.createLineBorder(Color.BLACK, 1));
		colorButton.setColor(new model.Color(255, 0, 0));
		colorButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				model.Color currentColor = colorButton.getColor();
				Color newColor = JColorChooser.showDialog(RegexEditor.this, "Choose color...", currentColor.toAWTColor());
				if (newColor != null) {
					colorButton.setColor(new model.Color(newColor.getRed(), newColor.getGreen(), newColor.getBlue()));
				}
			}
		});
		entryPanel.add(colorButton, BorderLayout.WEST);
		
		// entry panel: text field
		textField = new JXTextField("enter new regex entry here");
		textField.setBorder(BorderFactory.createLineBorder(Color.BLACK, 1));
		textField.getDocument().addDocumentListener(new DocumentListener() {
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
				String s = textField.getText();
				boolean duplicate = false;
				if (textField.getText().equals("")) {
					duplicate = true;
				} else {
					for (int i = 0; i < regexList.getModel().getSize(); i++) {
						if (((Regex) regexList.getModel().getElementAt(i)).getLabel().equals(s)) {
							duplicate = true;
						}
					}
				}
				if (duplicate == true) {
					addButton.setEnabled(false);
					if (!s.equals("")) {
						removeButton.setEnabled(true);
					}
				} else {
					addButton.setEnabled(true);
					removeButton.setEnabled(false);
				}
			}
		});
		textField.addFocusListener(new FocusListener() {
			@Override
			public void focusGained(FocusEvent e) {
				regexList.clearSelection();
			}
			@Override
			public void focusLost(FocusEvent e) {
				// nothing to do
			}
		});
		entryPanel.add(textField, BorderLayout.CENTER);
		colorButton.setPreferredSize(new Dimension(colorButton.getPreferredSize().width, textField.getPreferredSize().height));
		
        // button panel: add button
        JPanel buttonPanel = new JPanel();
		buttonPanel.setBorder(new EmptyBorder(0, 5, 0, 5)); // border left and right
        ImageIcon addIcon = new SvgIcon("/icons/tabler_plus.svg", 16).getImageIcon();
        addButton = new JButton("Add", addIcon);
        addButton.setEnabled(false);
		addButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				model.Color cl = colorButton.getColor();
				int red = cl.getRed();
				int green = cl.getGreen();
				int blue = cl.getBlue();
				String text = textField.getText();
				boolean added = Dna.sql.addRegex(text, red, green, blue);
				if (added) {
					changed = true;
					Regex regex = new Regex(text, cl);
					regexListModel.addElement(regex);
					textField.setText("");
					colorButton.setForeground(Color.RED);
				}
			}
		});
        buttonPanel.add(addButton);

        // button panel: remove button
        ImageIcon removeIcon = new SvgIcon("/icons/tabler_minus.svg", 16).getImageIcon();
        removeButton = new JButton("Remove", removeIcon);
        removeButton.setEnabled(false);
		removeButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String label = regexList.getSelectedValue().getLabel();
				boolean deleted = Dna.sql.deleteRegex(label);
				if (deleted) {
					changed = true;
					regexList.clearSelection();
					regexListModel.removeRegex(label);
					if (regexList.getVisibleRowCount() > 0) {
						regexList.setSelectedIndex(0);
					}
				}
			}
		});
        buttonPanel.add(removeButton);

		// button panel: close button
		ImageIcon closeIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-x.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		JButton closeButton = new JButton("Close", closeIcon);
		closeButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				dispose();
			}
		});
		buttonPanel.add(closeButton);

		// lower panel for text field and buttons
		JPanel lowerPanel = new JPanel(new BorderLayout());
		lowerPanel.add(entryPanel, BorderLayout.NORTH);
		lowerPanel.add(buttonPanel, BorderLayout.SOUTH);
		dialogPanel.add(lowerPanel, BorderLayout.SOUTH);
		this.add(dialogPanel);
		
		// load from database
		regexListModel.setRegexItems(Dna.sql.getRegexes());
		
		this.pack();
		this.setLocationRelativeTo(null);
		this.setVisible(true);
	}
	
	/**
	 * Has any change been made to the regular expressions in the database?
	 * 
	 * @return Has a change been made?
	 */
	public boolean isChanged() {
		return changed;
	}
	
	/**
	 * A list renderer for displaying {@link Regex} objects as list items.
	 */
	private class RegexListRenderer extends DefaultListCellRenderer {
		private static final long serialVersionUID = -2591524720728319393L;

		public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
			JLabel label = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
			label.setText((String)((Regex)value).getLabel());
			label.setForeground(((Regex)value).getColor().toAWTColor());
			label.setOpaque(true);
			return label;
		}
	}
	
	/**
	 * A list model for regular expressions.
	 */
	private class RegexListModel extends AbstractListModel<Regex> {
		private static final long serialVersionUID = 1232262685157283074L;
		ArrayList<Regex> regex;
		
		/**
		 * Create a new regex list model.
		 */
		public RegexListModel() {
			regex = new ArrayList<Regex>();
		}
		
		/**
		 * Replace the current array list of regexes by a new array list.
		 * 
		 * @param regexItems An array list of {@link Regex} objects.
		 */
		public void setRegexItems(ArrayList<Regex> regexItems) {
			int index = Math.max(regex.size(), regexItems.size());
			index = Math.max(index, 1);
			regex = regexItems;
			Collections.sort(regex);
			this.fireContentsChanged(this, 0, index - 1);
		}
		
	    /**
	     * Add a {@link Regex} object to the list.
	     * 
	     * @param newRegex  A {@link Regex} object.
	     */
	    public int addElement(Regex newRegex) {
	    	int newIndex = -1;
			if (regex.size() > 0) {
				for (int i = 0; i < regex.size(); i++) {
					if (newRegex.compareTo(regex.get(i)) == -1) {
						newIndex = i;
						break;
					}
				}
			} else {
				newIndex = 0;
			}
			if (newIndex == -1) { // there were other regexes, but the new regex comes later than all of them
				newIndex = regex.size();
			}
			regex.add(newIndex, newRegex);
			fireIntervalAdded(this, newIndex, newIndex);
			return newIndex;
	    }
	    
	    /**
	     * Remove a regex from the list model.
	     * 
	     * @param label The label of the regex to delete.
	     */
	    public void removeRegex(String label) {
	    	for (int i = regex.size() - 1; i >= 0; i--) {
	    		if (regex.get(i).getLabel().equals(label)) {
	    			regex.remove(i);
	    			this.fireIntervalRemoved(this, i, i);
	    			break;
	    		}
	    	}
	    }

	    @Override
	    public Regex getElementAt(int index) {
	    	return regex.get(index);
	    }
	    
	    @Override
	    public int getSize() {
	    	return regex.size();
	    }
	}
}