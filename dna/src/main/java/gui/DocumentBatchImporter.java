package gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
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
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.swing.DefaultListModel;
import javax.swing.DefaultListSelectionModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ProgressMonitor;
import javax.swing.SwingWorker;
import javax.swing.ToolTipManager;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.text.DateFormatter;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import model.Document;
import model.Statement;

/**
 * Represents a dialog window that can batch-import documents from a folder.
 */
public class DocumentBatchImporter extends JDialog {
	private static final long serialVersionUID = 1156604686298665919L;
	private File folder;
	private File[] files;
	private JList<String> fileList;
	private DefaultListModel<String> listModel;
	private JTextField titlePatternField, authorPatternField, sourcePatternField, sectionPatternField, typePatternField, notesPatternField, datePatternField, dateFormatField;
	private JTextField titlePreviewField, authorPreviewField, sourcePreviewField, sectionPreviewField, typePreviewField, notesPreviewField, datePreviewField;
	private JTextField dateFormatPreview;
	private JCheckBox titleCheckBox, authorCheckBox, sectionCheckBox, sourceCheckBox, typeCheckBox, dateCheckBox, notesCheckBox;
	private JTextArea textPreviewArea;
	
	/**
	 * Create an instance of the document batch importer dialog window.
	 */
	public DocumentBatchImporter() {
		this.setModal(true);
		this.setTitle("Import text files...");
		ImageIcon tableAddIcon = new SvgIcon("/icons/tabler_filter_plus.svg", 32).getImageIcon();
        this.setIconImage(tableAddIcon.getImage());
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        this.setLayout(new BorderLayout());

		LogEvent l = new LogEvent(Logger.MESSAGE,
				"[GUI] Opened document batch import window.",
				"Opened a document batch import window.");
		Dna.logger.log(l);
		
		ImageIcon cancelIcon = new SvgIcon("/icons/google_close.svg", 18).getImageIcon();
		JButton cancelButton = new JButton("Cancel", cancelIcon);
        cancelButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				dispose();
			}
		});
        cancelButton.setToolTipText("<html><p width=\"500\">Close this window and abort batch-import.</p></html>");

		ImageIcon folderIcon = new SvgIcon("/icons/tabler_folder.svg", 18).getImageIcon();
		JButton browseButton = new JButton("Select text files", folderIcon);
        browseButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				FileChooser fc = null;
				try {
					fc = new FileChooser(DocumentBatchImporter.this, "Select text files", false, "", "Import files", true);
				} catch (NullPointerException npe) {
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI] Cancelled file selection in batch import window.",
							"Cancelled file selection in batch import window. No files were selected.");
					Dna.logger.log(l);
				} finally {
					if (fc != null && fc.getFiles() != null) {
						files = fc.getFiles();
						listModel.clear();
						Arrays.stream(fc.getFiles())
								.filter(f -> f.isFile())
								.map(f -> f.getName())
								.forEachOrdered(f -> listModel.addElement(f));
						fileList.updateUI();
						if (files.length > 0) {
							fileList.setSelectedIndex(0);
						}
					} else {
						// no files were selected or FileChooser was cancelled
						LogEvent l = new LogEvent(Logger.MESSAGE,
								"[GUI] No files were selected in the batch import window.",
								"File selection was cancelled or no files were selected.");
						Dna.logger.log(l);
					}
				}
			}
		});
        browseButton.setToolTipText("<html><p width=\"500\">Select multiple text files to import into the current database. Each file should contain one document. Metadata can be parsed from the file names.</p></html>");

		ImageIcon updateIcon = new SvgIcon("/icons/tabler_refresh.svg", 18).getImageIcon();
        JButton updateButton = new JButton("Refresh preview", updateIcon);
        updateButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String inputText = fileList.getSelectedValue();
				if (inputText == null) {
					titlePreviewField.setText("");
					datePreviewField.setText("");
					authorPreviewField.setText("");
					sourcePreviewField.setText("");
					sectionPreviewField.setText("");
					typePreviewField.setText("");
					notesPreviewField.setText("");
					datePreviewField.setText("");
					textPreviewArea.setText("");
				} else {
					if (titleCheckBox.isSelected()) {
						titlePreviewField.setText(patternToString(inputText, titlePatternField.getText()));
					} else {
						titlePreviewField.setText(titlePatternField.getText());
					}
					titlePreviewField.setCaretPosition(0);
					
					if (dateCheckBox.isSelected()) {
						datePreviewField.setText(patternToString(inputText, datePatternField.getText()));
					} else {
						datePreviewField.setText(datePatternField.getText());
					}
					datePreviewField.setCaretPosition(0);
					
					if (datePreviewField.getText().equals("")) {
						dateFormatPreview.setText("");
					} else {
						LocalDateTime dateTime = stringToDateTime(datePreviewField.getText(), dateFormatField.getText());
						if (dateTime == null) {
							dateFormatPreview.setText("");
						} else {
							dateFormatPreview.setText(dateTime.toString());
						}
					}
					dateFormatPreview.setCaretPosition(0);
					
					if (authorCheckBox.isSelected()) {
						authorPreviewField.setText(patternToString(inputText, authorPatternField.getText()));
					} else {
						authorPreviewField.setText(authorPatternField.getText());
					}
					authorPreviewField.setCaretPosition(0);
					
					if (sectionCheckBox.isSelected()) {
						sectionPreviewField.setText(patternToString(inputText, sectionPatternField.getText()));
					} else {
						sectionPreviewField.setText(sectionPatternField.getText());
					}
					sectionPreviewField.setCaretPosition(0);
					
					if (sourceCheckBox.isSelected()) {
						sourcePreviewField.setText(patternToString(inputText, sourcePatternField.getText()));
					} else {
						sourcePreviewField.setText(sourcePatternField.getText());
					}
					sourcePreviewField.setCaretPosition(0);
					
					if (typeCheckBox.isSelected()) {
						typePreviewField.setText(patternToString(inputText, typePatternField.getText()));
					} else {
						typePreviewField.setText(typePatternField.getText());
					}
					typePreviewField.setCaretPosition(0);
					
					if (notesCheckBox.isSelected()) {
						notesPreviewField.setText(patternToString(inputText, notesPatternField.getText()));
					} else {
						notesPreviewField.setText(notesPatternField.getText());
					}
					notesPreviewField.setCaretPosition(0);
					
					// text preview
					FileInputStream is = null;
					try {
						is = new FileInputStream(files[fileList.getSelectedIndex()]);
					} catch (FileNotFoundException e1) {
						LogEvent l = new LogEvent(Logger.ERROR,
								"[GUI] Input file for batch import of documents not found.",
								"Attempted to open file but failed: " + files[fileList.getSelectedIndex()].getAbsolutePath() + ".",
								e1);
						Dna.logger.log(l);
					}
					
    				InputStreamReader isr = null;
					try {
						isr = new InputStreamReader(is, "UTF-8");
					} catch (UnsupportedEncodingException e1) {
						LogEvent l = new LogEvent(Logger.ERROR,
								"[GUI] Failed to decode input document for batch import.",
								"Unsupported coding exception. Document could not be read using UTF-8 encoding: " + files[fileList.getSelectedIndex()].getAbsolutePath() + ".",
								e1);
						Dna.logger.log(l);
					}
					
    				BufferedReader br = new BufferedReader(isr);
    				String document = "";
    				String sCurrentLine;
    				try {
						while ((sCurrentLine = br.readLine()) != null) {
							document = document + sCurrentLine + "\n";
						}
					} catch (IOException e1) {
						LogEvent l = new LogEvent(Logger.ERROR,
								"[GUI] Failed to read document for batch import.",
								"Input/output exception while reading document: " + files[fileList.getSelectedIndex()].getAbsolutePath() + ".",
								e1);
						Dna.logger.log(l);
					}
    				textPreviewArea.setText(document);
    				textPreviewArea.setCaretPosition(0);
				}
			}
		});
        updateButton.setToolTipText("<html><p width=\"500\">Refresh the metadata and text preview. Display the contents that will be added to the database for the currently highlighted file from the list.</p></html>");
        
		ImageIcon goIcon = new SvgIcon("/icons/tabler_check.svg", 18).getImageIcon();
        JButton goButton = new JButton("Import files", goIcon);
        goButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (files != null) {
					ImportWorker worker = new ImportWorker(
							DocumentBatchImporter.this,
							files,
							dateCheckBox.isSelected(),
							titleCheckBox.isSelected(),
							authorCheckBox.isSelected(),
							sourceCheckBox.isSelected(),
							sectionCheckBox.isSelected(),
							typeCheckBox.isSelected(),
							notesCheckBox.isSelected(),
							datePatternField.getText(),
							dateFormatField.getText(),
							titlePatternField.getText(),
							authorPatternField.getText(),
							sourcePatternField.getText(),
							sectionPatternField.getText(),
							typePatternField.getText(),
							notesPatternField.getText());
		            worker.execute();
				}
			}
		});
        goButton.setToolTipText("<html><p width=\"500\">Start importing all text files from the list above into the current database, setting the metadata as defined on the left.</p></html>");
        
        listModel = new DefaultListModel<String>();
        fileList = new JList<String>(listModel);
        fileList.setSelectionMode(DefaultListSelectionModel.SINGLE_SELECTION);
        JScrollPane fileListScroller = new JScrollPane(fileList);
        fileListScroller.setPreferredSize(new Dimension(800, 200));
        
        JPanel filePanel = new JPanel(new BorderLayout());
		TitledBorder tb3 = new TitledBorder(new EtchedBorder(), "Files in the selected directory");
		tb3.setTitleJustification(TitledBorder.CENTER);
		filePanel.setBorder(tb3);
        filePanel.add(fileListScroller, BorderLayout.CENTER);
        filePanel.setToolTipText("<html><p width=\"500\">This list shows all files selected for import. Click on a file and then update the preview to see the contents.</p></html>");
        this.add(filePanel, BorderLayout.NORTH);
        
		JPanel patternPanel = new JPanel(new GridBagLayout());
		TitledBorder tb1 = new TitledBorder(new EtchedBorder(), "Metadata parsed from file name");
		tb1.setTitleJustification(TitledBorder.CENTER);
		patternPanel.setBorder(tb1);
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.gridx = 1;
		gbc.gridy = 0;
		gbc.anchor = GridBagConstraints.WEST;
		JLabel patternHeaderLabel = new JLabel("Pattern");
		String ttPattern = "<html><p width=\"500\">In each of these fields, "
        		+ "you can either enter metadata to be saved into the database "
        		+ "(e.g., enter a single author or source for all text documents) "
        		+ "or enter regular expressions to parse these metadata from the "
        		+ "respective file name. The check buttons next to each field "
        		+ "determine if the text is interpreted as a regular expression or "
        		+ "as contents to be saved. Regular expressions are a powerful way "
        		+ "of matching text using patterns. There are many regex "
        		+ "tutorials available on the web.</p>"
        		+ "<p width=\"500\">The date format field interprets the date/time string that was "
        		+ "parsed and uses the pattern you provide to turn this into an actual "
        		+ "date-/timestamp. For example, if the date/time preview shows "
        		+ "<it>2021-07-25 14:39</it> because this was parsed using the "
        		+ "date/time regex pattern <it>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-1][0-9]:[0-5][0-9]</it>, "
        		+ "then the format string <it>yyyy-MM-dd HH:mm</it> could tell DNA "
        		+ "how to interpret this date properly.</p></html>";
        patternPanel.setToolTipText(ttPattern);
		patternPanel.add(patternHeaderLabel, gbc);
		
		gbc.gridx = 2;
		JLabel checkLabel = new JLabel("Regex  ");
        checkLabel.setToolTipText("Check these boxes if you want the value you entered "
        		+ "on the left to be interpreted as a regular expression to parse "
        		+ "the file name. If the box is not checked, what you enter is used "
        		+ "directly as metadata for the document.");
		patternPanel.add(checkLabel, gbc);
		
		gbc.gridx = 3;
		JLabel previewHeaderLabel = new JLabel("Preview");
        previewHeaderLabel.setToolTipText("Click on the refresh button at the bottom to "
        		+ "show a preview of the respective metadata field here.");
		patternPanel.add(previewHeaderLabel, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 1;
		gbc.anchor = GridBagConstraints.EAST;
		JLabel titleLabel = new JLabel("Title: ");
		String ttTitle = "Title of the document. Either enter a regular expression "
				+ "to parse the title from the file name, or enter the title directly "
				+ "into the field (though this would create many duplicate document "
				+ "titles). The default regex parses a title starting and ending with "
				+ "a letter (\"[a-zA-Z].+[a-zA-Z]\") that is followed by a date (optional) "
				+ "and a file extension, such as \".txt\".";
        titleLabel.setToolTipText(ttTitle);
		patternPanel.add(titleLabel, gbc);

		gbc.gridy = 2;
		JLabel authorLabel = new JLabel("Author: ");
		String ttAuthor = "Author of the document. Either enter a regular expression "
				+ "to parse the author from the file name, or enter the author "
				+ "directly into the field.";
        authorLabel.setToolTipText(ttAuthor);
		patternPanel.add(authorLabel, gbc);

		gbc.gridy = 3;
		JLabel sourceLabel = new JLabel("Source: ");
		String ttSource = "Source of the document. Either enter a regular expression "
				+ "to parse the source from the file name, or enter the source "
				+ "directly into the field.";
        sourceLabel.setToolTipText(ttSource);
		patternPanel.add(sourceLabel, gbc);

		gbc.gridy = 4;
		JLabel sectionLabel = new JLabel("Section: ");
		String ttSection = "Section of the document. Either enter a regular expression "
				+ "to parse the section from the file name, or enter the section "
				+ "directly into the field.";
        sectionLabel.setToolTipText(ttSection);
		patternPanel.add(sectionLabel, gbc);

		gbc.gridy = 5;
		JLabel typeLabel = new JLabel("Type: ");
		String ttType = "Type of the document. Either enter a regular expression "
				+ "to parse the type from the file name, or enter the type "
				+ "directly into the field.";
        typeLabel.setToolTipText(ttType);
		patternPanel.add(typeLabel, gbc);

		gbc.gridy = 6;
		JLabel notesLabel = new JLabel("Notes: ");
		String ttNotes = "Notes for the document. Either enter a regular expression "
				+ "to parse the notes from the file name, or enter the notes "
				+ "directly into the field.";
        notesLabel.setToolTipText(ttNotes);
		patternPanel.add(notesLabel, gbc);

		gbc.gridy = 7;
		JLabel coderLabel = new JLabel("Coder: ");
		String ttCoder = "<html><p width=\"500\">The currently active coder, with whom the documents will be associated. You cannot change the current coder here. This needs to be done from the DNA main window.</p></html>";
        coderLabel.setToolTipText(ttCoder);
		patternPanel.add(coderLabel, gbc);
		
		gbc.gridx = 1;
		gbc.gridy = 1;
		gbc.anchor = GridBagConstraints.WEST;
		titlePatternField = new JTextField("[a-zA-Z].+[a-zA-Z](?=\\s*[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}\\s*\\.[^\\.\\s]+$)");
		titlePatternField.setColumns(35);
        titlePatternField.setToolTipText(ttTitle);
		patternPanel.add(titlePatternField, gbc);
		
		gbc.gridy = 2;
		authorPatternField = new JTextField("");
		authorPatternField.setColumns(35);
        authorPatternField.setToolTipText(ttAuthor);
		patternPanel.add(authorPatternField, gbc);

		gbc.gridy = 3;
		sourcePatternField = new JTextField("");
		sourcePatternField.setColumns(35);
        sourcePatternField.setToolTipText(ttSource);
		patternPanel.add(sourcePatternField, gbc);

		gbc.gridy = 4;
		sectionPatternField = new JTextField("");
		sectionPatternField.setColumns(35);
        sectionPatternField.setToolTipText(ttSection);
		patternPanel.add(sectionPatternField, gbc);

		gbc.gridy = 5;
		typePatternField = new JTextField("");
		typePatternField.setColumns(35);
        typePatternField.setToolTipText(ttType);
		patternPanel.add(typePatternField, gbc);

		gbc.gridy = 6;
		notesPatternField = new JTextField("");
		notesPatternField.setColumns(35);
        notesPatternField.setToolTipText(ttNotes);
		patternPanel.add(notesPatternField, gbc);

		gbc.gridy = 7;
		gbc.gridwidth = 2;
		CoderBadgePanel cbp = new CoderBadgePanel();
        cbp.setToolTipText(ttCoder);
		patternPanel.add(cbp, gbc);
		
		gbc.gridx = 2;
		gbc.gridwidth = 1;
		gbc.anchor = GridBagConstraints.CENTER;
		gbc.gridy = 1;
		titleCheckBox = new JCheckBox();
		titleCheckBox.setSelected(true);
        titleCheckBox.setToolTipText(ttTitle);
		patternPanel.add(titleCheckBox, gbc);

		gbc.gridy = 2;
		authorCheckBox = new JCheckBox();
		authorCheckBox.setSelected(false);
        authorCheckBox.setToolTipText(ttAuthor);
		patternPanel.add(authorCheckBox, gbc);

		gbc.gridy = 3;
		sourceCheckBox = new JCheckBox();
		sourceCheckBox.setSelected(false);
        sourceCheckBox.setToolTipText(ttSource);
		patternPanel.add(sourceCheckBox, gbc);

		gbc.gridy = 4;
		sectionCheckBox = new JCheckBox();
		sectionCheckBox.setSelected(false);
        sectionCheckBox.setToolTipText(ttSection);
		patternPanel.add(sectionCheckBox, gbc);

		gbc.gridy = 5;
		typeCheckBox = new JCheckBox();
		typeCheckBox.setSelected(false);
        typeCheckBox.setToolTipText(ttType);
		patternPanel.add(typeCheckBox, gbc);

		gbc.gridy = 6;
		notesCheckBox = new JCheckBox();
		notesCheckBox.setSelected(false);
        notesCheckBox.setToolTipText(ttNotes);
		patternPanel.add(notesCheckBox, gbc);

		gbc.gridy = 8;
		dateCheckBox = new JCheckBox();
		dateCheckBox.setSelected(true);
		String ttDate = "<html><p width=\"500\">The date format field interprets the date/time string that was "
        		+ "parsed and uses the pattern you provide to turn this into an actual "
        		+ "date-/timestamp. For example, if the date/time preview shows "
        		+ "<it>2021-07-25 14:39</it> because this was parsed using the "
        		+ "date/time regex pattern <it>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-1][0-9]:[0-5][0-9]</it>, "
        		+ "then the format string <it>yyyy-MM-dd HH:mm</it> could tell DNA "
        		+ "how to interpret this date properly. You can leave out the time in the format pattern "
				+ "but not the date.</p></html>";
        dateCheckBox.setToolTipText(ttDate);
		patternPanel.add(dateCheckBox, gbc);

		gbc.anchor = GridBagConstraints.WEST;
		gbc.gridx = 3;
		gbc.gridy = 1;
		titlePreviewField = new JTextField("");
		titlePreviewField.setColumns(25);
		titlePreviewField.setEditable(false);
        titlePreviewField.setToolTipText(ttTitle);
		patternPanel.add(titlePreviewField, gbc);

		gbc.gridy = 2;
		authorPreviewField = new JTextField("");
		authorPreviewField.setColumns(25);
		authorPreviewField.setEditable(false);
        authorPreviewField.setToolTipText(ttAuthor);
		patternPanel.add(authorPreviewField, gbc);

		gbc.gridy = 3;
		sourcePreviewField = new JTextField("");
		sourcePreviewField.setColumns(25);
		sourcePreviewField.setEditable(false);
        sourcePreviewField.setToolTipText(ttSource);
		patternPanel.add(sourcePreviewField, gbc);

		gbc.gridy = 4;
		sectionPreviewField = new JTextField("");
		sectionPreviewField.setColumns(25);
		sectionPreviewField.setEditable(false);
        sectionPreviewField.setToolTipText(ttSection);
		patternPanel.add(sectionPreviewField, gbc);

		gbc.gridy = 5;
		typePreviewField = new JTextField("");
		typePreviewField.setColumns(25);
		typePreviewField.setEditable(false);
        typePreviewField.setToolTipText(ttType);
		patternPanel.add(typePreviewField, gbc);

		gbc.gridy = 6;
		notesPreviewField = new JTextField("");
		notesPreviewField.setColumns(25);
		notesPreviewField.setEditable(false);
        notesPreviewField.setToolTipText(ttNotes);
		patternPanel.add(notesPreviewField, gbc);

		gbc.anchor = GridBagConstraints.EAST;
		gbc.gridy = 8;
        gbc.gridx = 0;
        JLabel dateLabel = new JLabel("Date/time: ");
        dateLabel.setToolTipText(ttDate);
        patternPanel.add(dateLabel, gbc);
        
        gbc.gridx = 1;
        datePatternField = new JTextField("[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}");
		datePatternField.setColumns(35);
        datePatternField.setToolTipText(ttDate);
        patternPanel.add(datePatternField, gbc);
        
        gbc.gridx = 3;
        datePreviewField = new JTextField("");
		datePreviewField.setColumns(25);
		datePreviewField.setEditable(false);
        datePreviewField.setToolTipText(ttDate);
        patternPanel.add(datePreviewField, gbc);
        
        gbc.gridx = 0;
        gbc.gridy = 9;
        JLabel dateFormatLabel = new JLabel("Format: ");
        dateFormatLabel.setToolTipText(ttDate);
        patternPanel.add(dateFormatLabel, gbc);
        
        gbc.gridx = 1;
        dateFormatField = new JTextField("dd.MM.yyyy");
		dateFormatField.setColumns(35);
        dateFormatField.setToolTipText(ttDate);
        patternPanel.add(dateFormatField, gbc);
		
        gbc.gridx = 3;
        dateFormatPreview = new JTextField("");
		dateFormatPreview.setColumns(25);
		dateFormatPreview.setEditable(false);
        dateFormatPreview.setToolTipText(ttDate);
        patternPanel.add(dateFormatPreview, gbc);
        
		this.add(patternPanel, BorderLayout.CENTER);
		
		BorderLayout bl = new BorderLayout();
		JPanel textPreviewPanel = new JPanel(bl);
		TitledBorder tb2 = new TitledBorder(new EtchedBorder(), "Text preview");
		tb2.setTitleJustification(TitledBorder.CENTER);
		textPreviewPanel.setBorder(tb2);
		textPreviewArea = new JTextArea(10, 20);
		textPreviewArea.setWrapStyleWord(true);
	    textPreviewArea.setLineWrap(true);
		textPreviewArea.setEditable(false);
		String ttTextPreview = "<html><p width=\"500\">This text box "
        		+ "shows a preview of the text contents of the file you selected "
        		+ "from the list at the top of the window. You can select the "
        		+ "directory from which those files are read using the button at "
        		+ "the bottom.</p></html>";
        textPreviewArea.setToolTipText(ttTextPreview);
		JScrollPane textScroller = new JScrollPane(textPreviewArea);
		textScroller.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
        textScroller.setToolTipText(ttTextPreview);
		textPreviewPanel.add(textScroller, BorderLayout.CENTER);
        textPreviewPanel.setToolTipText(ttTextPreview);
		this.add(textPreviewPanel, BorderLayout.EAST);
        
        JPanel lowerButtons = new JPanel(new FlowLayout(FlowLayout.LEFT));
        lowerButtons.add(cancelButton);
        lowerButtons.add(browseButton);
        lowerButtons.add(updateButton);
        lowerButtons.add(goButton);
        
        JPanel tooltipPanel = new JPanel();
        JCheckBox tooltipCheckBox = new JCheckBox("Display tooltips with instructions");
		tooltipCheckBox.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				ToolTipManager.sharedInstance().setInitialDelay(10);
				if (tooltipCheckBox.isSelected()) {
					ToolTipManager.sharedInstance().setEnabled(true);
					ToolTipManager.sharedInstance().setDismissDelay(999999);
				} else {
					ToolTipManager.sharedInstance().setEnabled(false);
					ToolTipManager.sharedInstance().setDismissDelay(0);
				}
			}
			
		});
		ToolTipManager.sharedInstance().setInitialDelay(10);
		ToolTipManager.sharedInstance().setEnabled(false);
		ToolTipManager.sharedInstance().setDismissDelay(0);
        tooltipPanel.add(tooltipCheckBox);
        tooltipPanel.setToolTipText("<html><p width=\"500\">Toggle whether help is displayed when you hover over different areas of this window with the mouse.</p></html>");
        JPanel lowerPanel = new JPanel(new BorderLayout());
        lowerPanel.add(lowerButtons, BorderLayout.WEST);
        lowerPanel.add(tooltipPanel, BorderLayout.EAST);
        this.add(lowerPanel, BorderLayout.SOUTH);
        
        this.setModal(true);
        this.pack();
        this.setLocationRelativeTo(null);
        this.setVisible(true);
	}
	
	/**
	 * Match a pattern in a String and return the resulting match.
	 * 
	 * @param text     The input text.
	 * @param pattern  The pattern to be found in the text.
	 * @return         The substring that matches the pattern.
	 */
	private static String patternToString(String text, String pattern) {
		Pattern p;
		try {
			p = Pattern.compile(pattern);
		} catch (PatternSyntaxException e) {
			return("");
		}
		Matcher m = p.matcher(text);
		if (m.find()) {
			try {
				String string = m.group(0);
			    return string.substring(0, Math.min(190, string.length()));
			} catch (IndexOutOfBoundsException e) {
				return("");
			}
		} else {
			return "";
		}
	}

	/**
	 * Convert a String to a date using a specific date-time format.
	 * 
	 * @param text            The input text, representing the date as a String.
	 * @param dateTimeFormat  The date/time format pattern.
	 * @return                The converted local date-time object.
	 */
	private static LocalDateTime stringToDateTime(String text, String dateTimeFormat) {
		if (text.equals("")) {
			return LocalDateTime.now();
		} else {
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
			try {
				LocalDateTime dateTime = LocalDateTime.parse(text, dateTimeFormatter);
				return dateTime;
			} catch (DateTimeParseException e) {
				try {
					LocalDate date = LocalDate.parse(text, dateTimeFormatter);
					LocalDateTime dateTime = date.atStartOfDay();
					return dateTime;
				} catch (DateTimeParseException e2) {
					LogEvent l = new LogEvent(Logger.WARNING,
							"Date/time could not be parsed.",
							"The date/time \"" + text + "\" could not be parsed with the pattern \"" + dateTimeFormat + "\". Using current date and time instead.",
							e);
					Dna.logger.log(l);
					return LocalDateTime.now();
				}
			}
		}
	}
	
	/**
	 * Represents a Swing worker for batch-importing documents into the DNA
	 * database.
	 */
	private static class ImportWorker extends SwingWorker<Void, Document> {
		JDialog dialog;
		File[] files;
		boolean parseDate, parseTitle, parseAuthor, parseSource, parseSection, parseType, parseNotes;
		String datePattern, dateFormat, titlePattern, authorPattern, sourcePattern, sectionPattern, typePattern, notesPattern;
		String fn, sCurrentLine, document, title, dateString, author, source, section, notes, type;
		FileInputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		LocalDateTime date;
		Document article;
		ArrayList<Document> documents;
		int coderId;
        int good, bad, numDocumentsBefore;
        ProgressMonitor progressMonitor;
        
        /**
         * Create a new batch document importer Swing worker.
         * 
         * @param dialog          A reference to the enclosing dialog in order
         *   to be able to dispose of the dialog from within this thread.
         * @param files           An array of files to be imported.
         * @param parseDate       Should the date field be used for parsing?
         * @param parseTitle      Should the title field be used for parsing?
         * @param parseAuthor     Should the author field be used for parsing?
         * @param parseSource     Should the source field be used for parsing?
         * @param parseSection    Should the section field be used for parsing?
         * @param parseType       Should the type field be used for parsing?
         * @param parseNotes      Should the notes field be used for parsing?
         * @param datePattern     The pattern to identify/parse the date/time.
         * @param dateFormat      The format of the date/time for conversion.
         * @param titlePattern    The pattern to parse the title.
         * @param authorPattern   The pattern to parse the author.
         * @param sourcePattern   The pattern to parse the source.
         * @param sectionPattern  The pattern to parse the section.
         * @param typePattern     The pattern to parse the type.
         * @param notesPattern    The pattern to parse the notes.
         */
        ImportWorker(
        		JDialog dialog,
        		File[] files,
        		boolean parseDate,
        		boolean parseTitle,
        		boolean parseAuthor,
        		boolean parseSource,
        		boolean parseSection,
        		boolean parseType,
        		boolean parseNotes,
        		String datePattern,
        		String dateFormat,
        		String titlePattern,
        		String authorPattern,
        		String sourcePattern,
        		String sectionPattern,
        		String typePattern,
        		String notesPattern) {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Initializing import document thread: " + Thread.currentThread().getName() + " (" + Thread.currentThread().getId() + ").",
					"Initializing import document thread: " + Thread.currentThread().getName() + " (" + Thread.currentThread().getId() + ").");
			Dna.logger.log(l);
			
        	this.dialog = dialog;
            this.files = files;
        	progressMonitor = new ProgressMonitor(null, "Importing text files...", "", 0, this.files.length - 1);
			progressMonitor.setMillisToDecideToPopup(1);
			numDocumentsBefore = Dna.sql.countDocuments();
            this.parseDate = parseDate;
            this.parseTitle = parseTitle;
            this.parseAuthor = parseAuthor;
            this.parseSource = parseSource;
            this.parseSection = parseSection;
            this.parseType = parseType;
            this.parseNotes = parseNotes;
            this.documents = new ArrayList<Document>();
            this.datePattern = datePattern;
            this.dateFormat = dateFormat;
            this.titlePattern = titlePattern;
            this.authorPattern = authorPattern;
            this.sourcePattern = sourcePattern;
            this.sectionPattern = sectionPattern;
            this.typePattern = typePattern;
            this.notesPattern = notesPattern;
            coderId = Dna.sql.getConnectionProfile().getCoderId();
            good = 0;
            bad = 0;
        }

        @Override
        protected void process(List<Document> chunks) {
        	for (Document row : chunks) {
                documents.add(row);
                progressMonitor.setProgress(documents.size() - 1);
    			if (progressMonitor.isCanceled()) {
    				this.cancel(true);
    			}
            }
        }

        @Override
        protected Void doInBackground() throws Exception {
        	for (int i = 0; i < files.length; i++) {
            	fn = this.files[i].getName();
    			try {
    				is = new FileInputStream(files[i]);
    				isr = new InputStreamReader(is, "UTF-8");
    				br = new BufferedReader(isr);
    				document = "";
    				while ((sCurrentLine = br.readLine()) != null) {
    					document = document + sCurrentLine + "\n";
    				}
    				
    				dateString = "";
    				if (parseDate == true) {
    					dateString = patternToString(fn, datePattern);
    				} else {
    					dateString = datePattern;
    				}
    				date = stringToDateTime(dateString, dateFormat);
    				
    				if (parseTitle == true) {
    					title = patternToString(fn, titlePattern);
    				} else {
    					title = titlePattern;
    				}
    				
    				if (parseAuthor == true) {
    					author = patternToString(fn, authorPattern);
    				} else {
    					author = authorPattern;
    				}

    				if (parseSource == true) {
    					source = patternToString(fn, sourcePattern);
    				} else {
    					source = sourcePattern;
    				}

    				if (parseSection == true) {
    					section = patternToString(fn, sectionPattern);
    				} else {
    					section = sectionPattern;
    				}

    				if (parseType == true) {
    					type = patternToString(fn, typePattern);
    				} else {
    					type = typePattern;
    				}

    				if (parseNotes == true) {
    					notes = patternToString(fn, notesPattern);
    				} else {
    					notes = notesPattern;
    				}
    				
    				article = new Document(
    						-1, 
    						coderId,
    						title, 
    						document, 
    						author, 
    						source, 
    						section, 
    						type, 
    						notes, 
    						date, 
    						new ArrayList<Statement>()
    				);
    				publish(article);
    				good++;
    			} catch (Exception ex) {
    				bad++;
    			}
        	}
            return null;
        }

        @Override
        protected void done() {
			Dna.sql.addDocuments(documents);
			int numDocumentsAfter = Dna.sql.countDocuments();
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Batch import: " + good + " document(s) imported, " + bad + " skipped.",
					"There were " + numDocumentsBefore + " document(s) before the batch import, and there are " + numDocumentsAfter + " document(s) after completing the import.");
			Dna.logger.log(l);
			if (numDocumentsAfter > numDocumentsBefore) {
				JOptionPane.showMessageDialog(dialog, good + " documents were imported, " + bad + " skipped.");
			} else {
				JOptionPane.showMessageDialog(dialog, "No new documents were imported.");
			}
			l = new LogEvent(Logger.MESSAGE,
					"[GUI] Closing import document thread: " + Thread.currentThread().getName() + " (" + Thread.currentThread().getId() + ").",
					"Closing import document thread: " + Thread.currentThread().getName() + " (" + Thread.currentThread().getId() + ").");
			Dna.logger.log(l);
			dialog.dispose();
        }
    }
}