package gui;

import java.awt.FlowLayout;
import java.awt.Image;

import javax.swing.ImageIcon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.JSpinner.DefaultEditor;

import dna.Dna;
import gui.MainWindow.ActionAboutWindow;
import gui.MainWindow.ActionAddDocument;
import gui.MainWindow.ActionAttributeManager;
import gui.MainWindow.ActionBackboneExporter;
import gui.MainWindow.ActionBatchImportDocuments;
import gui.MainWindow.ActionCloseDatabase;
import gui.MainWindow.ActionCoderManager;
import gui.MainWindow.ActionCoderRelationsEditor;
import gui.MainWindow.ActionCreateDatabase;
import gui.MainWindow.ActionCsvImportDocuments;
import gui.MainWindow.ActionEditDocuments;
import gui.MainWindow.ActionImporter;
import gui.MainWindow.ActionLoggerDialog;
import gui.MainWindow.ActionNetworkExporter;
import gui.MainWindow.ActionOpenDatabase;
import gui.MainWindow.ActionOpenProfile;
import gui.MainWindow.ActionQuit;
import gui.MainWindow.ActionRecodeStatements;
import gui.MainWindow.ActionRefresh;
import gui.MainWindow.ActionRegexEditor;
import gui.MainWindow.ActionRemoveDocuments;
import gui.MainWindow.ActionRemoveStatements;
import gui.MainWindow.ActionSaveProfile;
import gui.MainWindow.ActionSearchDialog;
import gui.MainWindow.ActionStatementTypeEditor;

/**
 * Menu bar with actions as menu items.
 */
public class MenuBar extends JMenuBar {
	private static final long serialVersionUID = 6631392079690346097L;
	private JLabel popupWidthIconLabel, popupWidthDescriptionLabel;
	private JSpinner popupWidthSpinner;
	private JCheckBoxMenuItem rightToLeftItem, popupAutoCompleteItem, popupDecorationItem, colorByCoderItem;
	private JLabel fontSizeIconLabel, fontSizeDescriptionLabel;
	private JSpinner fontSizeSpinner;
	private SpinnerNumberModel fontSizeModel;

	/**
	 * Create a new menu bar.
	 * 
	 * @param actionOpenDatabase          Action for opening a new database.
	 * @param actionCloseDatabase         Action for closing the database.
	 * @param actionCreateDatabase        Action for creating a new database.
	 * @param actionOpenProfile           Action for opening connection profile.
	 * @param actionSaveProfile           Action for saving connection profile.
	 * @param actionRegexEditor           Action for opening regex editor.
	 * @param actionCoderManager          Action for opening coder manager.
	 * @param actionQuit                  Action for quitting DNA.
	 * @param actionAddDocument           Action for adding a document.
	 * @param actionRemoveDocuments       Action for removing documents.
	 * @param actionEditDocuments         Action for editing documents.
	 * @param actionBatchImportDocuments  Action for batch-importing documents.
	 * @param actionCsvImportDocuments    Action for importing from CSV.
	 * @param actionImporter              Action for importing from database.
	 * @param actionSearchDialog          Action for regex text search.
	 * @param actionRefresh               Action for reloading data.
	 * @param actionRecodeStatements      Action for recoding statements.
	 * @param actionRemoveStatements      Action for removing statements.
	 * @param actionStatementTypeEditor   Action for editing statement types.
	 * @param actionAttributeManager      Action for opening attribute manager.
	 * @param actionNetworkExporter       Action for opening network exporter.
	 * @param actionBackboneExporter      Action for opening backbone exporter.
	 * @param actionCoderRelationsEditor  Action for editing coder relations.
	 * @param actionLoggerDialog          Action for opening logger dialog.
	 * @param actionAboutWindow           Action for opening DNA About window.
	 */
	public MenuBar(ActionOpenDatabase actionOpenDatabase,
			ActionCloseDatabase actionCloseDatabase,
			ActionCreateDatabase actionCreateDatabase,
			ActionOpenProfile actionOpenProfile,
			ActionSaveProfile actionSaveProfile,
			ActionRegexEditor actionRegexEditor,
			ActionCoderManager actionCoderManager,
			ActionQuit actionQuit,
			ActionAddDocument actionAddDocument,
			ActionRemoveDocuments actionRemoveDocuments,
			ActionEditDocuments actionEditDocuments,
			ActionBatchImportDocuments actionBatchImportDocuments,
			ActionCsvImportDocuments actionCsvImportDocuments,
			ActionImporter actionImporter,
			ActionSearchDialog actionSearchDialog,
			ActionRefresh actionRefresh,
			ActionRecodeStatements actionRecodeStatements,
			ActionRemoveStatements actionRemoveStatements,
			ActionStatementTypeEditor actionStatementTypeEditor,
			ActionAttributeManager actionAttributeManager,
			ActionNetworkExporter actionNetworkExporter,
			ActionBackboneExporter actionBackboneExporter,
			ActionCoderRelationsEditor actionCoderRelationsEditor,
			ActionLoggerDialog actionLoggerDialog,
			ActionAboutWindow actionAboutWindow) {

		// database menu
		JMenu databaseMenu = new JMenu("Database");
		this.add(databaseMenu);
		
		// database menu: open a database
		JMenuItem openDatabaseItem = new JMenuItem(actionOpenDatabase);
		databaseMenu.add(openDatabaseItem);

		// database menu: close database
		JMenuItem closeDatabaseItem = new JMenuItem(actionCloseDatabase);
		databaseMenu.add(closeDatabaseItem);

		// database menu: create a new database
		JMenuItem createDatabaseItem = new JMenuItem(actionCreateDatabase);
		databaseMenu.add(createDatabaseItem);

		// database menu: open a connection profile
		JMenuItem openProfileItem = new JMenuItem(actionOpenProfile);
		databaseMenu.add(openProfileItem);

		// database menu: save a connection profile
		JMenuItem saveProfileItem = new JMenuItem(actionSaveProfile);
		databaseMenu.add(saveProfileItem);

		// database menu: open regex editor
		JMenuItem regexEditorItem = new JMenuItem(actionRegexEditor);
		databaseMenu.add(regexEditorItem);

		// database menu: open coder manager
		JMenuItem coderManagerItem = new JMenuItem(actionCoderManager);
		databaseMenu.add(coderManagerItem);

		// database menu: quit DNA
		JMenuItem quitItem = new JMenuItem(actionQuit);
		databaseMenu.add(quitItem);

		// document menu
		JMenu documentMenu = new JMenu("Document");
		this.add(documentMenu);
		
		// document menu: add new document
		JMenuItem addDocumentItem = new JMenuItem(actionAddDocument);
		documentMenu.add(addDocumentItem);
		
		// document menu: remove documents
		JMenuItem removeDocumentsItem = new JMenuItem(actionRemoveDocuments);
		documentMenu.add(removeDocumentsItem);
		
		// document menu: edit documents
		JMenuItem editDocumentsItem = new JMenuItem(actionEditDocuments);
		documentMenu.add(editDocumentsItem);

		// document menu: batch import documents
		JMenuItem batchImportDocumentsItem = new JMenuItem(actionBatchImportDocuments);
		documentMenu.add(batchImportDocumentsItem);

		// document menu: import from CSV file
		JMenuItem csvImportItem = new JMenuItem(actionCsvImportDocuments);
		documentMenu.add(csvImportItem);

		// document menu: import from other DNA database
		JMenuItem importerItem = new JMenuItem(actionImporter);
		documentMenu.add(importerItem);

		// document menu: regex search dialog
		JMenuItem regexSearchItem = new JMenuItem(actionSearchDialog);
		documentMenu.add(regexSearchItem);
		
		// document menu: refresh document table
		JMenuItem refreshItem = new JMenuItem(actionRefresh);
		documentMenu.add(refreshItem);

		// statements menu
		JMenu statementsMenu = new JMenu("Statements");
		this.add(statementsMenu);

		// statements menu: recode statements
		JMenuItem recodeStatementsItem = new JMenuItem(actionRecodeStatements);
		statementsMenu.add(recodeStatementsItem);

		// statements menu: remove statements
		JMenuItem removeStatementsItem = new JMenuItem(actionRemoveStatements);
		statementsMenu.add(removeStatementsItem);

		// statements menu: open statement type editor
		JMenuItem statementTypeEditorItem = new JMenuItem(actionStatementTypeEditor);
		statementsMenu.add(statementTypeEditorItem);
		
		// statements menu: open attribute manager
		JMenuItem attributeManagerItem = new JMenuItem(actionAttributeManager);
		statementsMenu.add(attributeManagerItem);
		
		// export menu
		JMenu exportMenu = new JMenu("Export");
		this.add(exportMenu);

		// export menu: open network exporter
		JMenuItem networkExporterItem = new JMenuItem(actionNetworkExporter);
		exportMenu.add(networkExporterItem);

		// export menu: open backbone exporter
		JMenuItem backboneExporterItem = new JMenuItem(actionBackboneExporter);
		exportMenu.add(backboneExporterItem);

		// settings menu
		JMenu settingsMenu = new JMenu("Settings");
		this.add(settingsMenu);

        // settings menu: font size spinner
        ImageIcon fontSizeIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-typography.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		fontSizeIconLabel = new JLabel(fontSizeIcon);
		fontSizeIconLabel.setToolTipText("Set the font size of the text area.");
		fontSizeDescriptionLabel = new JLabel("Font size of document text");
		fontSizeDescriptionLabel.setToolTipText(fontSizeIconLabel.getToolTipText());
        fontSizeModel = new SpinnerNumberModel(14, 1, 99, 1);
		fontSizeSpinner = new JSpinner(fontSizeModel);
		((DefaultEditor) fontSizeSpinner.getEditor()).getTextField().setColumns(2);
		fontSizeSpinner.setToolTipText(fontSizeIconLabel.getToolTipText());
		fontSizeDescriptionLabel.setLabelFor(fontSizeSpinner);
		fontSizeIconLabel.setEnabled(false);
		fontSizeDescriptionLabel.setEnabled(false);
		fontSizeSpinner.setEnabled(false);
		JPanel spinnerFontPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		spinnerFontPanel.add(fontSizeIconLabel);
		spinnerFontPanel.add(fontSizeDescriptionLabel);
		spinnerFontPanel.add(fontSizeSpinner);
		settingsMenu.add(spinnerFontPanel);

		// settings menu: popup width spinner
        ImageIcon popupWidthIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-chart-arrows.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		popupWidthIconLabel = new JLabel(popupWidthIcon);
		popupWidthIconLabel.setToolTipText("Set the width of the text fields for the variables in a statement popup window (in px).");
		popupWidthDescriptionLabel = new JLabel("Popup text field width");
		popupWidthDescriptionLabel.setToolTipText(popupWidthIconLabel.getToolTipText());
        SpinnerNumberModel popupWidthModel = new SpinnerNumberModel(300, 160, 9990, 10);
		popupWidthSpinner = new JSpinner(popupWidthModel);
		((DefaultEditor) popupWidthSpinner.getEditor()).getTextField().setColumns(4);
		popupWidthSpinner.setToolTipText(popupWidthIconLabel.getToolTipText());
		popupWidthDescriptionLabel.setLabelFor(popupWidthSpinner);
		popupWidthIconLabel.setEnabled(false);
		popupWidthDescriptionLabel.setEnabled(false);
		popupWidthSpinner.setEnabled(false);
		JPanel spinnerPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		spinnerPanel.add(popupWidthIconLabel);
		spinnerPanel.add(popupWidthDescriptionLabel);
		spinnerPanel.add(popupWidthSpinner);
		settingsMenu.add(spinnerPanel);
		
		// settings menu: popup auto-completion toggle button
		ImageIcon popupAutoCompleteIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-forms.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		popupAutoCompleteItem = new JCheckBoxMenuItem("Popup field auto-completion     ", popupAutoCompleteIcon, false);
		popupAutoCompleteItem.setToolTipText("If the menu item is selected, text fields in statement popup windows will have auto-complete activated for entries.");
		popupAutoCompleteItem.setEnabled(false);
		settingsMenu.add(popupAutoCompleteItem);

		// settings menu: popup window decoration toggle button
		ImageIcon popupDecorationIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-border-outer.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		popupDecorationItem = new JCheckBoxMenuItem("Popup window frame and buttons     ", popupDecorationIcon, false);
		popupDecorationItem.setToolTipText("If the menu item is selected, statement popup windows will have buttons and a frame. If not, statements will auto-save.");
		popupDecorationItem.setEnabled(false);
		settingsMenu.add(popupDecorationItem);

		// settings menu: color statements by coder toggle button
		ImageIcon colorByCoderIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-palette.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		colorByCoderItem = new JCheckBoxMenuItem("Color statements by coder", colorByCoderIcon);
		colorByCoderItem.setToolTipText("If the menu item is selected, statements in the text are highlighted using the color of the coder who created them; otherwise using the statement type color.");
		colorByCoderItem.setEnabled(false);
		settingsMenu.add(colorByCoderItem);

		// settings menu: open coder relations editor
		JMenuItem coderRelationsEditorItem = new JMenuItem(actionCoderRelationsEditor);
		settingsMenu.add(coderRelationsEditorItem);
		
		// settings menu: display logger dialog window
		JMenuItem loggerDialogItem = new JMenuItem(actionLoggerDialog);
		settingsMenu.add(loggerDialogItem);

		// settings menu: display about DNA window
		JMenuItem aboutWindowItem = new JMenuItem(actionAboutWindow);
		settingsMenu.add(aboutWindowItem);
	}

	/**
	 * Get a reference to the font size spinner from the Settings menu.
	 * 
	 * @return  The font size spinner.
	 */
	JSpinner getFontSizeSpinner() {
		return fontSizeSpinner;
	}

	/**
	 * Get a reference to the popup width spinner from the Settings menu.
	 * 
	 * @return  The popup width spinner.
	 */
	JSpinner getPopupWidthSpinner() {
		return popupWidthSpinner;
	}

	/**
	 * Get a reference to the right to left item from the Settings menu.
	 * 
	 * @return  The right to left menu item.
	 */
	JCheckBoxMenuItem getRightToLeftItem() {
		return rightToLeftItem;
	}

	/**
	 * Get a reference to the popup autocomplete item from the Settings menu.
	 * 
	 * @return  The popup autocomplete menu item.
	 */
	JCheckBoxMenuItem getPopupAutoCompleteItem() {
		return popupAutoCompleteItem;
	}

	/**
	 * Get a reference to the popup decoration menu item from the Settings menu.
	 * 
	 * @return  The popup decoration menu item.
	 */
	JCheckBoxMenuItem getPopupDecorationItem() {
		return popupDecorationItem;
	}

	/**
	 * Get a reference to the color by coder menu item from the Settings menu.
	 * 
	 * @return  The color by coder menu item.
	 */
	JCheckBoxMenuItem getColorByCoderItem() {
		return colorByCoderItem;
	}

	/**
	 * When a new coder has been selected, make the necessary changes in the
	 * menu bar to reflect the new coder's permissions and settings.
	 */
	public void adjustToChangedCoder() {
		if (Dna.sql.getConnectionProfile() == null || Dna.sql.getActiveCoder() == null) {
			popupWidthIconLabel.setEnabled(false);
			popupWidthDescriptionLabel.setEnabled(false);
			popupWidthSpinner.setEnabled(false);

			popupAutoCompleteItem.setEnabled(false);
			popupAutoCompleteItem.setSelected(false);
			
			popupDecorationItem.setEnabled(false);
			popupDecorationItem.setSelected(false);
			
			colorByCoderItem.setEnabled(false);
			colorByCoderItem.setSelected(false);

			fontSizeIconLabel.setEnabled(false);
			fontSizeDescriptionLabel.setEnabled(false);
			fontSizeSpinner.setEnabled(false);
		} else {
			popupWidthIconLabel.setEnabled(true);
			popupWidthDescriptionLabel.setEnabled(true);
			popupWidthSpinner.setEnabled(true);
			popupWidthSpinner.setValue(Dna.sql.getActiveCoder().getPopupWidth());
			
			popupAutoCompleteItem.setEnabled(true);
			if (Dna.sql.getActiveCoder().isPopupAutoComplete() == true) {
				popupAutoCompleteItem.setSelected(true);
			} else {
				popupAutoCompleteItem.setSelected(false);
			}
			
			popupDecorationItem.setEnabled(true);
			if (Dna.sql.getActiveCoder().isPopupDecoration() == true) {
				popupDecorationItem.setSelected(true);
			} else {
				popupDecorationItem.setSelected(false);
			}
			
			colorByCoderItem.setEnabled(true);
			if (Dna.sql.getActiveCoder().isColorByCoder() == true) {
				colorByCoderItem.setSelected(true);
			} else {
				colorByCoderItem.setSelected(false);
			}

			fontSizeIconLabel.setEnabled(true);
			fontSizeDescriptionLabel.setEnabled(true);
			fontSizeSpinner.setEnabled(true);
			fontSizeSpinner.setValue(Dna.sql.getActiveCoder().getFontSize());
		}
	}
}