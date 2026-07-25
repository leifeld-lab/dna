package gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ProgressMonitor;
import javax.swing.SwingWorker;
import javax.swing.ToolTipManager;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableModel;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import model.Document;
import model.Statement;

/**
 * Represents a dialog window that can import documents from a CSV file.
 * The dialog lets the user map CSV columns to DNA document fields via
 * dropdown selectors, preview the data, and import all rows as documents.
 */
public class CsvImporter extends JDialog {
	private static final long serialVersionUID = 1L;
	private File csvFile;
	private List<String> csvHeaders;
	private List<String[]> csvRows;
	
	// Column mapping dropdowns
	private JComboBox<String> titleCombo, textCombo, authorCombo, sourceCombo, sectionCombo, typeCombo, notesCombo, dateCombo;
	
	// Fixed value fields (shown when "Fixed value..." is selected in a dropdown)
	private JTextField titleFixedField, authorFixedField, sourceFixedField, sectionFixedField, typeFixedField, notesFixedField;
	
	// Date format
	private JTextField dateFormatField;
	
	// Preview table
	private JTable previewTable;
	private DefaultTableModel previewTableModel;
	
	// Text preview for selected row
	private JTextArea textPreviewArea;
	
	/** Whether documents were successfully imported. Read by ActionCsvImportDocuments to trigger refresh. */
	boolean documentsImported = false;
	
	private static final String FIXED_VALUE = "< Fixed value... >";
	private static final String IGNORE = "< ignore >";
	private static final String SELECT_COLUMN = "-- select column --";
	
	/**
	 * Create an instance of the CSV importer dialog window.
	 */
	public CsvImporter() {
		this.setModal(true);
		this.setTitle("Import CSV file...");
		ImageIcon tableAddIcon = new SvgIcon("/icons/tabler_filter_plus.svg", 32).getImageIcon();
		this.setIconImage(tableAddIcon.getImage());
		this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		this.setLayout(new BorderLayout());
		
		LogEvent l = new LogEvent(Logger.MESSAGE,
				"[GUI] Opened CSV import window.",
				"Opened a CSV import window.");
		Dna.logger.log(l);
		
		// --- Top: file selector ---
		JPanel filePanel = new JPanel(new BorderLayout(5, 0));
		TitledBorder tbFile = new TitledBorder(new EtchedBorder(), "CSV file");
		tbFile.setTitleJustification(TitledBorder.LEFT);
		filePanel.setBorder(tbFile);
		
		JTextField filePathField = new JTextField(50);
		filePathField.setEditable(false);
		filePathField.setToolTipText("Path to the selected CSV file.");
		filePanel.add(filePathField, BorderLayout.CENTER);
		
		ImageIcon folderIcon = new SvgIcon("/icons/tabler_folder.svg", 18).getImageIcon();
		JButton browseButton = new JButton("Browse...", folderIcon);
		browseButton.setToolTipText("Select a CSV file to import.");
		filePanel.add(browseButton, BorderLayout.EAST);
		
		this.add(filePanel, BorderLayout.NORTH);
		
		// Column separator selector
		JPanel separatorPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		separatorPanel.add(new JLabel("Separator:"));
		JComboBox<String> separatorCombo = new JComboBox<>(new String[]{",", ";", "\t", "|"});
		separatorCombo.setEditable(true);
		separatorCombo.setToolTipText("Column separator character used in the CSV file. Common: comma (,), semicolon (;), tab.");
		separatorPanel.add(separatorCombo);
		separatorPanel.setToolTipText("Select the character that separates columns in your CSV file.");
		
		// --- Center: mapping panel ---
		JPanel mappingPanel = new JPanel(new GridBagLayout());
		TitledBorder tbMapping = new TitledBorder(new EtchedBorder(), "Column mapping");
		tbMapping.setTitleJustification(TitledBorder.CENTER);
		mappingPanel.setBorder(tbMapping);
		
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.insets = new java.awt.Insets(2, 5, 2, 5);
		gbc.weightx = 0.0;
		
		// Headers
		gbc.gridx = 0; gbc.gridy = 0;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("DNA field"), gbc);
		gbc.gridx = 1; gbc.gridy = 0;
		gbc.weightx = 1.0;
		mappingPanel.add(new JLabel("CSV column"), gbc);
		gbc.gridx = 2; gbc.gridy = 0;
		gbc.weightx = 0.5;
		mappingPanel.add(new JLabel("Fixed value"), gbc);
		
		// Title
		gbc.gridx = 0; gbc.gridy = 1;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Title:"), gbc);
		gbc.gridx = 1; gbc.gridy = 1;
		gbc.weightx = 1.0;
		titleCombo = new JComboBox<>();
		titleCombo.setToolTipText("Map a CSV column to the document title, or use a fixed value.");
		mappingPanel.add(titleCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 1;
		gbc.weightx = 0.5;
		titleFixedField = new JTextField(20);
		titleFixedField.setEnabled(false);
		mappingPanel.add(titleFixedField, gbc);
		
		// Text (required)
		gbc.gridx = 0; gbc.gridy = 2;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Text:*"), gbc);
		gbc.gridx = 1; gbc.gridy = 2;
		gbc.weightx = 1.0;
		textCombo = new JComboBox<>();
		textCombo.setToolTipText("Map a CSV column to the document text. Required.");
		mappingPanel.add(textCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 2;
		gbc.weightx = 0.5;
		mappingPanel.add(new JLabel("(required)"), gbc);
		
		// Author
		gbc.gridx = 0; gbc.gridy = 3;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Author:"), gbc);
		gbc.gridx = 1; gbc.gridy = 3;
		gbc.weightx = 1.0;
		authorCombo = new JComboBox<>();
		authorCombo.setToolTipText("Map a CSV column to the document author, or use a fixed value.");
		mappingPanel.add(authorCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 3;
		gbc.weightx = 0.5;
		authorFixedField = new JTextField(20);
		authorFixedField.setEnabled(false);
		mappingPanel.add(authorFixedField, gbc);
		
		// Source
		gbc.gridx = 0; gbc.gridy = 4;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Source:"), gbc);
		gbc.gridx = 1; gbc.gridy = 4;
		gbc.weightx = 1.0;
		sourceCombo = new JComboBox<>();
		sourceCombo.setToolTipText("Map a CSV column to the document source, or use a fixed value.");
		mappingPanel.add(sourceCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 4;
		gbc.weightx = 0.5;
		sourceFixedField = new JTextField(20);
		sourceFixedField.setEnabled(false);
		mappingPanel.add(sourceFixedField, gbc);
		
		// Section
		gbc.gridx = 0; gbc.gridy = 5;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Section:"), gbc);
		gbc.gridx = 1; gbc.gridy = 5;
		gbc.weightx = 1.0;
		sectionCombo = new JComboBox<>();
		sectionCombo.setToolTipText("Map a CSV column to the document section, or use a fixed value.");
		mappingPanel.add(sectionCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 5;
		gbc.weightx = 0.5;
		sectionFixedField = new JTextField(20);
		sectionFixedField.setEnabled(false);
		mappingPanel.add(sectionFixedField, gbc);
		
		// Type
		gbc.gridx = 0; gbc.gridy = 6;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Type:"), gbc);
		gbc.gridx = 1; gbc.gridy = 6;
		gbc.weightx = 1.0;
		typeCombo = new JComboBox<>();
		typeCombo.setToolTipText("Map a CSV column to the document type, or use a fixed value.");
		mappingPanel.add(typeCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 6;
		gbc.weightx = 0.5;
		typeFixedField = new JTextField(20);
		typeFixedField.setEnabled(false);
		mappingPanel.add(typeFixedField, gbc);
		
		// Notes
		gbc.gridx = 0; gbc.gridy = 7;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Notes:"), gbc);
		gbc.gridx = 1; gbc.gridy = 7;
		gbc.weightx = 1.0;
		notesCombo = new JComboBox<>();
		notesCombo.setToolTipText("Map a CSV column to the document notes, or use a fixed value.");
		mappingPanel.add(notesCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 7;
		gbc.weightx = 0.5;
		notesFixedField = new JTextField(20);
		notesFixedField.setEnabled(false);
		mappingPanel.add(notesFixedField, gbc);
		
		// Date/time
		gbc.gridx = 0; gbc.gridy = 8;
		gbc.weightx = 0.0;
		mappingPanel.add(new JLabel("Date/Time:"), gbc);
		gbc.gridx = 1; gbc.gridy = 8;
		gbc.weightx = 1.0;
		dateCombo = new JComboBox<>();
		dateCombo.setToolTipText("Map a CSV column to the document date/time, or use a fixed value.");
		mappingPanel.add(dateCombo, gbc);
		gbc.gridx = 2; gbc.gridy = 8;
		gbc.weightx = 0.5;
		dateFormatField = new JTextField("yyyy-MM-dd", 20);
		dateFormatField.setToolTipText("Date format pattern, e.g. yyyy-MM-dd, dd/MM/yyyy, dd-MM-yyyy.");
		mappingPanel.add(dateFormatField, gbc);
		
		// Wrap separator + mapping panels
		JPanel centerPanel = new JPanel(new BorderLayout());
		centerPanel.add(separatorPanel, BorderLayout.NORTH);
		centerPanel.add(mappingPanel, BorderLayout.CENTER);
		this.add(centerPanel, BorderLayout.CENTER);
		
		// --- East: preview ---
		JPanel previewPanel = new JPanel(new BorderLayout());
		
		// Preview table
		previewTableModel = new DefaultTableModel();
		previewTable = new JTable(previewTableModel);
		previewTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		previewTable.setToolTipText("Preview of the first 20 rows from the CSV file. Click a row to see its text below.");
		JScrollPane tableScrollPane = new JScrollPane(previewTable);
		tableScrollPane.setPreferredSize(new Dimension(400, 200));
		TitledBorder tbPreview = new TitledBorder(new EtchedBorder(), "Preview (first 20 rows)");
		tbPreview.setTitleJustification(TitledBorder.CENTER);
		tableScrollPane.setBorder(tbPreview);
		previewPanel.add(tableScrollPane, BorderLayout.CENTER);
		
		// Text preview
		textPreviewArea = new JTextArea(8, 30);
		textPreviewArea.setWrapStyleWord(true);
		textPreviewArea.setLineWrap(true);
		textPreviewArea.setEditable(false);
		textPreviewArea.setToolTipText("Full text of the selected row in the preview table above.");
		JScrollPane textScrollPane = new JScrollPane(textPreviewArea);
		TitledBorder tbText = new TitledBorder(new EtchedBorder(), "Text preview");
		tbText.setTitleJustification(TitledBorder.CENTER);
		textScrollPane.setBorder(tbText);
		previewPanel.add(textScrollPane, BorderLayout.SOUTH);
		
		this.add(previewPanel, BorderLayout.EAST);
		
		// --- Bottom: buttons ---
		JPanel bottomPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		
		ImageIcon cancelIcon = new SvgIcon("/icons/google_close.svg", 18).getImageIcon();
		JButton cancelButton = new JButton("Cancel", cancelIcon);
		cancelButton.setToolTipText("Close this dialog without importing.");
		cancelButton.addActionListener(e -> dispose());
		bottomPanel.add(cancelButton);
		
		ImageIcon goIcon = new SvgIcon("/icons/tabler_plus.svg", 18).getImageIcon();
		JButton importButton = new JButton("Import CSV", goIcon);
		importButton.setEnabled(false);
		importButton.setToolTipText("Import all rows from the CSV file as DNA documents.");
		bottomPanel.add(importButton);
		
		// Tooltip toggle
		JPanel tooltipPanel = new JPanel();
		JCheckBox tooltipCheckBox = new JCheckBox("Show tooltips");
		tooltipCheckBox.addActionListener(e -> {
			ToolTipManager.sharedInstance().setInitialDelay(10);
			if (tooltipCheckBox.isSelected()) {
				ToolTipManager.sharedInstance().setEnabled(true);
				ToolTipManager.sharedInstance().setDismissDelay(999999);
			} else {
				ToolTipManager.sharedInstance().setEnabled(false);
				ToolTipManager.sharedInstance().setDismissDelay(0);
			}
		});
		ToolTipManager.sharedInstance().setInitialDelay(10);
		ToolTipManager.sharedInstance().setEnabled(false);
		ToolTipManager.sharedInstance().setDismissDelay(0);
		tooltipPanel.add(tooltipCheckBox);
		
		JPanel lowerPanel = new JPanel(new BorderLayout());
		lowerPanel.add(bottomPanel, BorderLayout.WEST);
		lowerPanel.add(tooltipPanel, BorderLayout.EAST);
		this.add(lowerPanel, BorderLayout.SOUTH);
		
		// --- Listeners ---
		
		// Browse button: select CSV file and populate preview
		browseButton.addActionListener(e -> {
			JFileChooser fc = new JFileChooser();
			fc.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("CSV files (*.csv)", "csv"));
			fc.setMultiSelectionEnabled(false);
			int result = fc.showOpenDialog(CsvImporter.this);
			if (result == JFileChooser.APPROVE_OPTION) {
				csvFile = fc.getSelectedFile();
				filePathField.setText(csvFile.getAbsolutePath());
				String sep = (String) separatorCombo.getSelectedItem();
				if (sep == null || sep.isEmpty()) sep = ",";
				loadCsvPreview(sep);
				populateDropdowns();
				importButton.setEnabled(csvRows != null && !csvRows.isEmpty());
			}
		});
		
		// Fixed value toggle listeners: enable/disable fixed value text fields
		addFixedValueListener(titleCombo, titleFixedField);
		addFixedValueListener(authorCombo, authorFixedField);
		addFixedValueListener(sourceCombo, sourceFixedField);
		addFixedValueListener(sectionCombo, sectionFixedField);
		addFixedValueListener(typeCombo, typeFixedField);
		addFixedValueListener(notesCombo, notesFixedField);
		
		// Preview table selection listener: update text preview
		previewTable.getSelectionModel().addListSelectionListener(e -> {
			if (!e.getValueIsAdjusting()) {
				updateTextPreview();
			}
		});
		
		// Import button
		importButton.addActionListener(e -> {
			if (csvRows != null && !csvRows.isEmpty()) {
				String textCol = getSelectedColumn(textCombo);
				if (textCol == null) {
					JOptionPane.showMessageDialog(CsvImporter.this,
							"Please select a CSV column for the Text field (required).",
							"Text required", JOptionPane.WARNING_MESSAGE);
					return;
				}
				
				// Confirmation dialog
				int confirm = JOptionPane.showConfirmDialog(CsvImporter.this,
						"Do you want to import " + csvRows.size() + " document(s) from the CSV file?",
						"Confirm import", JOptionPane.YES_NO_OPTION);
				if (confirm != JOptionPane.YES_OPTION) {
					return;
				}
				
				int textColIndex = csvHeaders.indexOf(textCol);
				String titleCol = getSelectedColumn(titleCombo);
				String authorCol = getSelectedColumn(authorCombo);
				String sourceCol = getSelectedColumn(sourceCombo);
				String sectionCol = getSelectedColumn(sectionCombo);
				String typeCol = getSelectedColumn(typeCombo);
				String notesCol = getSelectedColumn(notesCombo);
				String dateCol = getSelectedColumn(dateCombo);
				String dateFormat = dateFormatField.getText().trim();
				if (dateFormat.isEmpty()) {
					dateFormat = "yyyy-MM-dd";
				}
				String separator = (String) separatorCombo.getSelectedItem();
				if (separator == null || separator.isEmpty()) {
					separator = ",";
				}
				
				ImportWorker worker = new ImportWorker(
						CsvImporter.this,
						csvRows, csvHeaders,
						textColIndex,
						titleCol, authorCol, sourceCol, sectionCol, typeCol, notesCol, dateCol,
						dateFormat, separator,
						getFixedValue(titleCombo, titleFixedField),
						getFixedValue(authorCombo, authorFixedField),
						getFixedValue(sourceCombo, sourceFixedField),
						getFixedValue(sectionCombo, sectionFixedField),
						getFixedValue(typeCombo, typeFixedField),
						getFixedValue(notesCombo, notesFixedField));
				worker.execute();
			}
		});
		
		this.setModal(true);
		this.pack();
		this.setLocationRelativeTo(null);
		this.setVisible(true);
	}
	
	/**
	 * Load the CSV file, parse headers and rows, and populate the preview table.
	 * @param separator The column separator character (e.g., comma, semicolon).
	 */
	private void loadCsvPreview(String separator) {
		csvHeaders = new ArrayList<>();
		csvRows = new ArrayList<>();
		previewTableModel.setRowCount(0);
		previewTableModel.setColumnCount(0);
		
		if (csvFile == null) return;
		char sep = separator.isEmpty() ? ',' : separator.charAt(0);
		
		try (FileInputStream is = new FileInputStream(csvFile);
			 InputStreamReader isr = new InputStreamReader(is, "UTF-8");
			 BufferedReader br = new BufferedReader(isr)) {
			
			// Read header
			String headerLine = br.readLine();
			if (headerLine == null) return;
			String[] headers = parseCsvLine(headerLine, sep);
			for (String h : headers) {
				csvHeaders.add(h.trim());
			}
			
			// Set table columns
			previewTableModel.addColumn("#");
			for (String h : csvHeaders) {
				previewTableModel.addColumn(h);
			}
			
			// Read rows (up to 20 for preview)
			String line;
			int rowNum = 0;
			while ((line = br.readLine()) != null && rowNum < 20) {
				String[] values = parseCsvLine(line, sep);
				csvRows.add(values);
				
				Object[] rowData = new Object[values.length + 1];
				rowData[0] = String.valueOf(rowNum + 1);
				for (int i = 0; i < values.length && i < csvHeaders.size(); i++) {
					rowData[i + 1] = values[i].trim();
				}
				previewTableModel.addRow(rowData);
				rowNum++;
			}
			
			// Read remaining rows for import (beyond preview)
			while ((line = br.readLine()) != null) {
				csvRows.add(parseCsvLine(line, sep));
			}
			
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] CSV preview loaded: " + csvHeaders.size() + " columns, " + csvRows.size() + " rows.",
					"Loaded CSV preview from file: " + csvFile.getAbsolutePath());
			Dna.logger.log(l);
			
		} catch (FileNotFoundException ex) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"[GUI] CSV file not found.",
					"CSV file not found: " + csvFile.getAbsolutePath(), ex);
			Dna.logger.log(l);
		} catch (UnsupportedEncodingException ex) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"[GUI] CSV file encoding not supported.",
					"UTF-8 encoding not supported for CSV file.", ex);
			Dna.logger.log(l);
		} catch (IOException ex) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"[GUI] Failed to read CSV file.",
					"IO error while reading CSV file: " + csvFile.getAbsolutePath(), ex);
			Dna.logger.log(l);
		}
	}
	
	/**
	 * Parse a single line of CSV, handling quoted fields containing the separator.
	 * @param line The CSV line to parse.
	 * @param separator The column separator character.
	 */
	private String[] parseCsvLine(String line, char separator) {
		List<String> fields = new ArrayList<>();
		boolean inQuotes = false;
		StringBuilder current = new StringBuilder();
		
		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if (c == '"') {
				if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
					// escaped quote: "" inside quoted field → literal "
					current.append('"');
					i++;
				} else {
					inQuotes = !inQuotes;
				}
			} else if (c == separator && !inQuotes) {
				fields.add(current.toString());
				current = new StringBuilder();
			} else {
				current.append(c);
			}
		}
		fields.add(current.toString());
		return fields.toArray(new String[0]);
	}
	
	/**
	 * Populate all combo boxes with CSV column names and special options.
	 */
	private void populateDropdowns() {
		DefaultComboBoxModel<String> modelWithIgnore = new DefaultComboBoxModel<>();
		modelWithIgnore.addElement(SELECT_COLUMN);
		for (String h : csvHeaders) {
			modelWithIgnore.addElement(h);
		}
		modelWithIgnore.addElement(FIXED_VALUE);
		modelWithIgnore.addElement(IGNORE);
		
		// Text combo: no ignore option
		DefaultComboBoxModel<String> textModel = new DefaultComboBoxModel<>();
		textModel.addElement(SELECT_COLUMN);
		for (String h : csvHeaders) {
			textModel.addElement(h);
		}
		
		titleCombo.setModel(copyModel(modelWithIgnore));
		textCombo.setModel(copyModel(textModel));
		authorCombo.setModel(copyModel(modelWithIgnore));
		sourceCombo.setModel(copyModel(modelWithIgnore));
		sectionCombo.setModel(copyModel(modelWithIgnore));
		typeCombo.setModel(copyModel(modelWithIgnore));
		notesCombo.setModel(copyModel(modelWithIgnore));
		dateCombo.setModel(copyModel(modelWithIgnore));
	}
	
	private DefaultComboBoxModel<String> copyModel(DefaultComboBoxModel<String> source) {
		DefaultComboBoxModel<String> copy = new DefaultComboBoxModel<>();
		for (int i = 0; i < source.getSize(); i++) {
			copy.addElement(source.getElementAt(i));
		}
		return copy;
	}
	
	/**
	 * Add listener to toggle fixed value text field visibility.
	 */
	private void addFixedValueListener(JComboBox<String> combo, JTextField fixedField) {
		combo.addActionListener(e -> {
			fixedField.setEnabled(FIXED_VALUE.equals(combo.getSelectedItem()));
			if (!FIXED_VALUE.equals(combo.getSelectedItem())) {
				fixedField.setText("");
			}
		});
	}
	
	/**
	 * Get the selected column name from a combo box, or null if ignore/select.
	 */
	private String getSelectedColumn(JComboBox<String> combo) {
		Object selected = combo.getSelectedItem();
		if (selected == null || SELECT_COLUMN.equals(selected) || IGNORE.equals(selected) || FIXED_VALUE.equals(selected)) {
			return null;
		}
		return selected.toString();
	}
	
	/**
	 * Get fixed value if "Fixed value..." is selected, otherwise null.
	 */
	private String getFixedValue(JComboBox<String> combo, JTextField fixedField) {
		if (FIXED_VALUE.equals(combo.getSelectedItem())) {
			return fixedField.getText();
		}
		return null;
	}
	
	/**
	 * Update the text preview area based on the selected row in the preview table.
	 */
	private void updateTextPreview() {
		int selectedRow = previewTable.getSelectedRow();
		if (selectedRow < 0 || selectedRow >= csvRows.size()) {
			textPreviewArea.setText("");
			return;
		}
		
		String textColName = getSelectedColumn(textCombo);
		if (textColName != null) {
			int colIndex = csvHeaders.indexOf(textColName);
			if (colIndex >= 0 && colIndex < csvRows.get(selectedRow).length) {
				textPreviewArea.setText(csvRows.get(selectedRow)[colIndex].trim());
				textPreviewArea.setCaretPosition(0);
			}
		}
	}
	
	/**
	 * Convert a String to a date using a specific date-time format.
	 * Dates are interpreted as UTC to ensure consistent results across time zones.
	 */
	private static LocalDateTime stringToDateTime(String text, String dateTimeFormat) {
		if (text == null || text.isEmpty()) {
			return LocalDateTime.now(ZoneOffset.UTC);
		}
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimeFormat);
		try {
			return LocalDateTime.parse(text, formatter);
		} catch (DateTimeParseException e) {
			try {
				LocalDate date = LocalDate.parse(text, formatter);
				return date.atStartOfDay();
			} catch (DateTimeParseException e2) {
				LogEvent l = new LogEvent(Logger.WARNING,
						"Date/time could not be parsed.",
						"The date/time \"" + text + "\" could not be parsed with pattern \"" + dateTimeFormat + "\". Using current UTC date/time instead.", e);
				Dna.logger.log(l);
				return LocalDateTime.now(ZoneOffset.UTC);
			}
		}
	}
	
	/**
	 * Swing worker that imports CSV rows as DNA documents in the background.
	 */
	private class ImportWorker extends SwingWorker<Void, Document> {
		private JDialog dialog;
		private List<String[]> rows;
		private List<String> headers;
		private int textColIndex;
		private String titleCol, authorCol, sourceCol, sectionCol, typeCol, notesCol, dateCol;
		private String dateFormat;
		private String separator;
		private String titleFixed, authorFixed, sourceFixed, sectionFixed, typeFixed, notesFixed;
		private int coderId;
		private int good, bad, numDocumentsBefore;
		private ProgressMonitor progressMonitor;
		
		ImportWorker(JDialog dialog,
				List<String[]> rows, List<String> headers,
				int textColIndex,
				String titleCol, String authorCol, String sourceCol, String sectionCol, String typeCol, String notesCol, String dateCol,
				String dateFormat, String separator,
				String titleFixed, String authorFixed, String sourceFixed, String sectionFixed, String typeFixed, String notesFixed) {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Initializing CSV import thread: " + Thread.currentThread().getName(),
					"Initializing CSV import thread.");
			Dna.logger.log(l);
			
			this.dialog = dialog;
			this.rows = rows;
			this.headers = headers;
			this.textColIndex = textColIndex;
			this.titleCol = titleCol;
			this.authorCol = authorCol;
			this.sourceCol = sourceCol;
			this.sectionCol = sectionCol;
			this.typeCol = typeCol;
			this.notesCol = notesCol;
			this.dateCol = dateCol;
			this.dateFormat = dateFormat;
			this.separator = separator;
			this.titleFixed = titleFixed;
			this.authorFixed = authorFixed;
			this.sourceFixed = sourceFixed;
			this.sectionFixed = sectionFixed;
			this.typeFixed = typeFixed;
			this.notesFixed = notesFixed;
			this.coderId = Dna.sql.getConnectionProfile().getCoderId();
			this.good = 0;
			this.bad = 0;
			
			progressMonitor = new ProgressMonitor(null, "Importing CSV rows...", "", 0, rows.size() - 1);
			progressMonitor.setMillisToDecideToPopup(1);
			numDocumentsBefore = Dna.sql.countDocuments();
		}
		
		@Override
		protected Void doInBackground() throws Exception {
			ArrayList<Document> batch = new ArrayList<>();
			
			for (int i = 0; i < rows.size(); i++) {
				if (isCancelled() || progressMonitor.isCanceled()) {
					break;
				}
				
				String[] row = rows.get(i);
				
				try {
					// Get text (required)
					String text = "";
					if (textColIndex >= 0 && textColIndex < row.length) {
						text = row[textColIndex].trim();
					}
					
					// Get other fields
					String title = titleFixed != null ? titleFixed : (titleCol != null ? getColumnValue(row, titleCol) : "");
					String author = authorFixed != null ? authorFixed : (authorCol != null ? getColumnValue(row, authorCol) : "");
					String source = sourceFixed != null ? sourceFixed : (sourceCol != null ? getColumnValue(row, sourceCol) : "");
					String section = sectionFixed != null ? sectionFixed : (sectionCol != null ? getColumnValue(row, sectionCol) : "");
					String type = typeFixed != null ? typeFixed : (typeCol != null ? getColumnValue(row, typeCol) : "");
					String notes = notesFixed != null ? notesFixed : (notesCol != null ? getColumnValue(row, notesCol) : "");
					
					// Parse date
					String dateStr = dateCol != null ? getColumnValue(row, dateCol) : "";
					LocalDateTime date = stringToDateTime(dateStr, dateFormat);
					
					Document doc = new Document(-1, coderId, title, text, author, source, section, type, notes, date, new ArrayList<Statement>());
					batch.add(doc);
					good++;
					
					// Batch commit every 100 documents
					if (batch.size() >= 100) {
						Dna.sql.addDocuments(batch);
						batch.clear();
					}
					
					progressMonitor.setProgress(i);
					
				} catch (Exception ex) {
					bad++;
					LogEvent l = new LogEvent(Logger.WARNING,
							"[GUI] CSV import: failed to process row " + (i + 1) + ".",
							"Failed to import CSV row " + (i + 1) + ". Skipping.", ex);
					Dna.logger.log(l);
				}
			}
			
			// Commit remaining batch
			if (!batch.isEmpty()) {
				Dna.sql.addDocuments(batch);
			}
			
			return null;
		}
		
		private String getColumnValue(String[] row, String colName) {
			int idx = headers.indexOf(colName);
			if (idx >= 0 && idx < row.length) {
				return row[idx].trim();
			}
			return "";
		}
		
		@Override
		protected void done() {
			int numDocumentsAfter = Dna.sql.countDocuments();
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] CSV import: " + good + " document(s) imported, " + bad + " skipped.",
					"There were " + numDocumentsBefore + " document(s) before import, and " + numDocumentsAfter + " document(s) after.");
			Dna.logger.log(l);
			
			if (numDocumentsAfter > numDocumentsBefore) {
				documentsImported = true;
				JOptionPane.showMessageDialog(dialog, good + " documents were imported, " + bad + " skipped.");
			} else {
				JOptionPane.showMessageDialog(dialog, "No new documents were imported.");
			}
			
			l = new LogEvent(Logger.MESSAGE,
					"[GUI] Closing CSV import thread: " + Thread.currentThread().getName(),
					"Closing CSV import thread.");
			Dna.logger.log(l);
			
			dialog.dispose();
		}
	}
}
