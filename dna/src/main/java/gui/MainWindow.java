package gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Image;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.*;
import java.awt.geom.Rectangle2D;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.text.BadLocationException;

import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import logger.LoggerDialog;
import model.Coder;
import model.Statement;
import model.StatementType;
import model.TableDocument;
import model.Value;
import sql.ConnectionProfile;
import sql.Sql;

/**
 * Main window that instantiates and plugs the different view components
 * together. It contains controls for the different actions and defines
 * listeners that are attached to the view components to interact between the
 * components. 
 */
public class MainWindow extends JFrame {
	private static final long serialVersionUID = 2740437090361841747L;
	private Container c;
	private DocumentTablePanel documentTablePanel;
	private DocumentTableModel documentTableModel;
	private StatementPanel statementPanel;
	private StatementTableModel statementTableModel;
	private TextPanel textPanel;
	private MenuBar menuBar;
	private ToolbarPanel toolbar;
	private StatusBar statusBar;
	private CoderSelectionPanel coderSelectionPanel;
	private ActionOpenDatabase actionOpenDatabase;
	private ActionCreateDatabase actionCreateDatabase;
	private ActionOpenProfile actionOpenProfile; 
	private ActionSaveProfile actionSaveProfile;
	private ActionRegexEditor actionRegexEditor;
	private ActionCoderManager actionCoderManager;
	private ActionQuit actionQuit;
	private ActionCloseDatabase actionCloseDatabase;
	private ActionAddDocument actionAddDocument;
	private ActionRemoveDocuments actionRemoveDocuments;
	private ActionEditDocuments actionEditDocuments;
	private ActionSearchDialog actionSearchDialog;
	private ActionRefresh actionRefresh;
	private ActionBatchImportDocuments actionBatchImportDocuments;
	private ActionImporter actionImporter;
	private ActionRecodeStatements actionRecodeStatements;
	private ActionRemoveStatements actionRemoveStatements;
	private ActionStatementTypeEditor actionStatementTypeEditor;
	private ActionAttributeManager actionAttributeManager;
	private ActionNetworkExporter actionNetworkExporter;
	private ActionBackboneExporter actionBackboneExporter;
	private ActionCoderRelationsEditor actionCoderRelationsEditor;
	private ActionLoggerDialog actionLoggerDialog;
	private ActionAboutWindow actionAboutWindow;
	private Popup popup = null;

	/**
	 * A document table swing worker thread.
	 */
	private DocumentTableRefreshWorker documentTableWorker;

	/**
	 * A statement table swing worker thread.
	 */
	private StatementTableRefreshWorker statementTableWorker;
	
	/**
	 * Create a new main window.
	 */
	public MainWindow() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Set system look and feel for GUI.",
					"Set system look and feel for GUI.");
			Dna.logger.log(l);
		} catch (UnsupportedLookAndFeelException e) {
			LogEvent l = new LogEvent(Logger.WARNING,
					"Unsupport look and feel. Using default theme.",
					"Unsupport look and feel. Using default theme.",
					e);
			Dna.logger.log(l);
	    } catch (ClassNotFoundException e) {
			LogEvent l = new LogEvent(Logger.WARNING,
					"Look and feel class not found. Using default theme.",
					"Look and feel class not found. Using default theme.",
					e);
			Dna.logger.log(l);
	    } catch (InstantiationException e) {
			LogEvent l = new LogEvent(Logger.WARNING,
					"Look and feel instantiation exception. Using default theme.",
					"Look and feel instantiation exception. Using default theme.",
					e);
			Dna.logger.log(l);
	    } catch (IllegalAccessException e) {
			LogEvent l = new LogEvent(Logger.WARNING,
					"Look and feel illegal access exception. Using default theme.",
					"Look and feel illegal access exception. Using default theme.",
					e);
			Dna.logger.log(l);
	    }
		
		c = getContentPane();
		this.setTitle("Discourse Network Analyzer");
		this.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		
		List<Image> dnaIcons = new ArrayList<Image>();
		dnaIcons.add(new ImageIcon(getClass().getResource("/icons/dna512.png")).getImage());
		dnaIcons.add(new ImageIcon(getClass().getResource("/icons/dna256.png")).getImage());
		dnaIcons.add(new ImageIcon(getClass().getResource("/icons/dna128.png")).getImage());
		dnaIcons.add(new ImageIcon(getClass().getResource("/icons/dna64.png")).getImage());
		dnaIcons.add(new ImageIcon(getClass().getResource("/icons/dna32.png")).getImage());
		dnaIcons.add(new ImageIcon(getClass().getResource("/icons/dna16.png")).getImage());
		this.setIconImages(dnaIcons);
		
		// close SQL connection before exit
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				if (documentTableWorker != null) {
					documentTableWorker.cancel(true);
				}
				if (statementTableWorker != null) {
					statementTableWorker.cancel(true);
				}
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"Exiting DNA from the GUI main window.",
						"Exiting DNA from the GUI main window.");
				Dna.logger.log(l);
				System.exit(0);
			}
		});
		
		// initialize actions
		ImageIcon openDatabaseIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-database.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionOpenDatabase = new ActionOpenDatabase("Open DNA database", openDatabaseIcon, "Open a dialog window to establish a connection to a remote or file-based database", KeyEvent.VK_O);

		ImageIcon closeDatabaseIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-x.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionCloseDatabase = new ActionCloseDatabase("Close database", closeDatabaseIcon, "Close the connection to the current database and reset graphical user interface", KeyEvent.VK_X);
		actionCloseDatabase.setEnabled(false);
		
		ImageIcon createDatabaseIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-plus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionCreateDatabase = new ActionCreateDatabase("Create new DNA database", createDatabaseIcon, "Open a dialog window to create a new remote or file-based database", KeyEvent.VK_C);
		
		ImageIcon openProfileIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-link.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionOpenProfile = new ActionOpenProfile("Open connection profile", openProfileIcon, "Open a connection profile, which acts as a bookmark to a database", KeyEvent.VK_P);
		
		ImageIcon saveProfileIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-download.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionSaveProfile = new ActionSaveProfile("Save connection profile", saveProfileIcon, "Save a connection profile, which acts as a bookmark to a database", KeyEvent.VK_S);
		actionSaveProfile.setEnabled(false);

		ImageIcon regexEditorIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-prescription.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionRegexEditor = new ActionRegexEditor("Open regex editor", regexEditorIcon, "Open the regular expression editor to add or delete regex search terms for in-text highlighting", KeyEvent.VK_R);
		actionRegexEditor.setEnabled(false);
		
		ImageIcon coderManagerIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-users.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionCoderManager = new ActionCoderManager("Open coder manager", coderManagerIcon, "Open the coder manager to edit coders and their permissions.", KeyEvent.VK_M);
		actionCoderManager.setEnabled(false);
		
		ImageIcon quitIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-logout.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionQuit = new ActionQuit("Exit / quit", quitIcon, "Close the Discourse Network Analyzer", KeyEvent.VK_Q);
		
		ImageIcon addDocumentIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-file-plus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionAddDocument = new ActionAddDocument("Add document", addDocumentIcon, "Open a dialog window to enter details of a new document", KeyEvent.VK_A);
		actionAddDocument.setEnabled(false);
		
		ImageIcon removeDocumentsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-file-minus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionRemoveDocuments = new ActionRemoveDocuments("Remove document(s)", removeDocumentsIcon, "Remove the document(s) currently selected in the document table", KeyEvent.VK_R);
		actionRemoveDocuments.setEnabled(false);
		
		ImageIcon editDocumentsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-edit.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionEditDocuments = new ActionEditDocuments("Edit document(s)", editDocumentsIcon, "Edit the document(s) currently selected in the document table", KeyEvent.VK_E);
		actionEditDocuments.setEnabled(false);
		
		ImageIcon documentTableRefreshIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-refresh.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionRefresh = new ActionRefresh("Refresh document table", documentTableRefreshIcon, "Fetch new documents from the database and insert them into the document table and remove deleted rows from the table", KeyEvent.VK_F);
		actionRefresh.setEnabled(false);

		ImageIcon batchImportDocumentsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-file-import.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionBatchImportDocuments = new ActionBatchImportDocuments("Import from directory", batchImportDocumentsIcon, "Batch-import all text files from a folder as new documents", KeyEvent.VK_I);
		actionBatchImportDocuments.setEnabled(false);

		ImageIcon importerIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-database-import.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionImporter = new ActionImporter("Import from DNA database", importerIcon, "Import documents, statements, entities, attributes, and regexes from another DNA database", KeyEvent.VK_D);
		actionImporter.setEnabled(false);

		ImageIcon searchDialogIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-search.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionSearchDialog = new ActionSearchDialog("Regex text search", searchDialogIcon, "Search for regular expressions in document texts and find matches", KeyEvent.VK_F);
		actionSearchDialog.setEnabled(false);
		this.getRootPane().getInputMap(JRootPane.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke("control F"),	"open search dialog");
		this.getRootPane().getActionMap().put("open search dialog", actionSearchDialog);

		ImageIcon recodeStatementsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-pencil.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionRecodeStatements = new ActionRecodeStatements("Edit multiple statements...", recodeStatementsIcon, "Recode the statements currently selected in the statement table", KeyEvent.VK_R);
		actionRecodeStatements.setEnabled(false);

		ImageIcon removeStatementsIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-square-minus.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionRemoveStatements = new ActionRemoveStatements("Remove statement(s)", removeStatementsIcon, "Remove the statement(s) currently selected in the statement table", KeyEvent.VK_D);
		actionRemoveStatements.setEnabled(false);

		ImageIcon statementTypeEditorIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-message-2.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionStatementTypeEditor = new ActionStatementTypeEditor("Edit statement types", statementTypeEditorIcon, "Open the statement type editor to edit statement types and their variables.", KeyEvent.VK_T);
		actionStatementTypeEditor.setEnabled(false);

		ImageIcon attributeManagerIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-list.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionAttributeManager = new ActionAttributeManager("Open attribute manager", attributeManagerIcon, "Open the attribute manager to edit entities and their attribute values.", KeyEvent.VK_A);
		actionAttributeManager.setEnabled(false);

		ImageIcon networkExporterIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-affiliate.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionNetworkExporter = new ActionNetworkExporter("Open network export dialog", networkExporterIcon, "Open a network export dialog window.", KeyEvent.VK_N);
		actionNetworkExporter.setEnabled(false);

		ImageIcon backboneExporterIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-arrows-split.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionBackboneExporter = new ActionBackboneExporter("Backbone and redundant concepts", backboneExporterIcon, "Open a backbone and redundant set export dialog window.", KeyEvent.VK_B);
		actionBackboneExporter.setEnabled(false);

		ImageIcon coderRelationsEditorIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-user-check.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionCoderRelationsEditor = new ActionCoderRelationsEditor("Edit coder relations", coderRelationsEditorIcon, "Open the coder relations editor define whose documents and statements you can view and edit.", KeyEvent.VK_R);
		actionCoderRelationsEditor.setEnabled(false);
		
		ImageIcon loggerIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-bug.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionLoggerDialog = new ActionLoggerDialog("Display message log", loggerIcon, "Display a log of messages, warnings, and errors in a dialog window", KeyEvent.VK_L);
		
		ImageIcon aboutIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/dna32.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		actionAboutWindow = new ActionAboutWindow("About DNA", aboutIcon, "Display information about DNA", KeyEvent.VK_B);

		// define models
		documentTableModel = new DocumentTableModel();
		statementTableModel = new StatementTableModel();
		
		// define GUI elements
		toolbar = new ToolbarPanel(documentTableModel,
				actionAddDocument,
				actionRemoveDocuments,
				actionEditDocuments,
				actionRefresh,
				actionRemoveStatements);
		documentTablePanel = new DocumentTablePanel(documentTableModel,
				actionAddDocument,
				actionRemoveDocuments,
				actionEditDocuments);
		menuBar = new MenuBar(actionOpenDatabase,
				actionCloseDatabase,
				actionCreateDatabase,
				actionOpenProfile,
				actionSaveProfile,
				actionRegexEditor,
				actionCoderManager,
				actionQuit,
				actionAddDocument,
				actionRemoveDocuments,
				actionEditDocuments,
				actionBatchImportDocuments,
				actionImporter,
				actionSearchDialog,
				actionRefresh,
				actionRecodeStatements,
				actionRemoveStatements,
				actionStatementTypeEditor,
				actionAttributeManager,
				actionNetworkExporter,
				actionBackboneExporter,
				actionCoderRelationsEditor,
				actionLoggerDialog,
				actionAboutWindow);
		statusBar = new StatusBar();
		statementPanel = new StatementPanel(statementTableModel, actionRecodeStatements, actionRemoveStatements);
		textPanel = new TextPanel();
		coderSelectionPanel = new CoderSelectionPanel();
		
		// add listeners
		Dna.logger.addListener(statusBar);
		
		// layout
		JPanel documentsAndToolBarPanel = new JPanel(new BorderLayout());
		documentsAndToolBarPanel.add(toolbar, BorderLayout.NORTH);
		documentsAndToolBarPanel.add(documentTablePanel, BorderLayout.CENTER);
		
		JPanel statementsAndCoderPanel = new JPanel(new BorderLayout());
		statementsAndCoderPanel.add(coderSelectionPanel, BorderLayout.NORTH);
		statementsAndCoderPanel.add(getStatementPanel(), BorderLayout.CENTER);
		
		JSplitPane verticalSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, documentsAndToolBarPanel, textPanel);
		verticalSplitPane.setOneTouchExpandable(true);
		JSplitPane rightSplitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, verticalSplitPane, statementsAndCoderPanel);
		rightSplitPane.setOneTouchExpandable(true);
		rightSplitPane.setBorder(new EmptyBorder(0, 5, 0, 0));
		rightSplitPane.setResizeWeight(0.9); // right pane gets 10% of any extra space if resized (e.g., maximized)

		JPanel mainPanel = new JPanel(new BorderLayout());
		mainPanel.add(menuBar, BorderLayout.NORTH);
		mainPanel.add(rightSplitPane, BorderLayout.CENTER);
		mainPanel.add(statusBar, BorderLayout.SOUTH);
		
		// selection listener for the statement table; select statement or enable recode and remove statements actions
		JTable statementTable = getStatementPanel().getStatementTable();
		statementTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				if (e.getValueIsAdjusting()) {
					return;
				}
				int rowCount = statementTable.getSelectedRowCount();
				getStatementPanel().setMenuItemStatementsSelected(rowCount + " statements selected");
				long statementTypeCount = IntStream.of(statementTable.getSelectedRows())
					.map(r -> statementTable.convertRowIndexToModel(r))
					.map(i -> statementTableModel.getRow(i).getStatementTypeId())
					.distinct()
					.count();
				getStatementPanel().setMenuItemStatementTypesSelected("of " + statementTypeCount + " statement type(s)");
				if (rowCount == 0) {
					MainWindow.this.actionRecodeStatements.setEnabled(false);
					MainWindow.this.actionRemoveStatements.setEnabled(false);
				} else if (rowCount == 1 && (popup == null || !popup.isValid())) {
					MainWindow.this.actionRecodeStatements.setEnabled(false);
					int selectedRow = statementTable.getSelectedRow();
					int selectedModelIndex = statementTable.convertRowIndexToModel(selectedRow);
					int statementId = statementTableModel.getRow(selectedModelIndex).getId();
					Statement s = Dna.sql.getStatement(statementId);
					documentTablePanel.setSelectedDocumentId(s.getDocumentId());
					if (Dna.sql.getActiveCoder().isPermissionDeleteStatements() == true &&
							(Dna.sql.getActiveCoder().isPermissionEditOthersStatements() == true ||
							Dna.sql.getActiveCoder().getId() == s.getCoderId()) &&
							(Dna.sql.getActiveCoder().getId() == s.getCoderId() ||
							Dna.sql.getActiveCoder().isPermissionEditOthersStatements(s.getCoderId()) == true)) {
						MainWindow.this.actionRemoveStatements.setEnabled(true);
					} else {
						MainWindow.this.actionRemoveStatements.setEnabled(false);
					}
					if (Dna.sql.getActiveCoder().getId() == s.getCoderId() ||
							(Dna.sql.getActiveCoder().isPermissionViewOthersStatements() == true &&
									Dna.sql.getActiveCoder().isPermissionViewOthersStatements(s.getCoderId()) == true)) {
						int documentCoderId = documentTableModel.getRow(documentTableModel.getModelRowById(s.getDocumentId())).getCoder().getId();
						if (Dna.sql.getActiveCoder().getId() != documentCoderId &&
								(Dna.sql.getActiveCoder().isPermissionViewOthersDocuments() == false ||
								!Dna.sql.getActiveCoder().isPermissionViewOthersDocuments(documentCoderId))) {
							LogEvent l = new LogEvent(Logger.MESSAGE,
									"[GUI] Statement " + s.getId() + ": Cannot open statement popup due to lack of permissions.",
									"Statement " + s.getId() + " cannot be opened in a popup window because the document in which it is contained is owned by a different coder and the current coder does not have permission to view this coder's documents.");
							Dna.logger.log(l);
						} else {
							JTextPane textWindow = getTextPanel().getTextWindow();
							JScrollPane textScrollPane = getTextPanel().getTextScrollPane();
							textWindow.grabFocus();
							textWindow.select(s.getStart(), s.getStop());
							
							// the selection is too slow, so wait for it to finish, otherwise the popup is sometimes displayed in random locations...
							SwingUtilities.invokeLater(new Runnable() {
								@SuppressWarnings("deprecation") // modelToView becomes modelToView2D in Java 9, but we still want Java 8 compliance
								public void run() {
									Rectangle2D mtv = null;
									try {
										double y = textWindow.modelToView(s.getStart()).getY();
										int l = textWindow.getText().length();
										double last = textWindow.modelToView(l).getY();
										double frac = y / last;
										double max = textScrollPane.getVerticalScrollBar().getMaximum();
										double h = textScrollPane.getHeight();
										int value = (int) Math.ceil(frac * max - (h / 2));
										textScrollPane.getVerticalScrollBar().setValue(value);
										mtv = textWindow.modelToView(s.getStart());
										Point loc = textWindow.getLocationOnScreen();
										newPopup(mtv.getX(), mtv.getY(), s, loc);
									} catch (BadLocationException e) {
										LogEvent l = new LogEvent(Logger.WARNING,
												"[GUI] Statement " + s.getId() + ": Popup window bad location exception.",
												"Statement " + s.getId() + ": Popup window cannot be opened because the location is outside the document text.");
										Dna.logger.log(l);
									}
								}
							});
						}
					}
				} else if (rowCount > 1 && (popup == null || !popup.isValid())) {
					boolean allOwned = true;
					boolean allOthersEditPermitted = true;
					boolean permitRecode = Dna.sql.getActiveCoder().isPermissionEditStatements();
					int[] selectedRows = statementTable.getSelectedRows();
					int[] modelRows = new int[selectedRows.length];
					int[] coderIds = new int[selectedRows.length];
					int statementTypeId = statementTableModel.getRow(statementTable.convertRowIndexToModel(selectedRows[0])).getStatementTypeId();
					for (int i = 0; i < selectedRows.length; i++) {
						modelRows[i] = statementTable.convertRowIndexToModel(selectedRows[i]);
						coderIds[i] = statementTableModel.getRow(modelRows[i]).getCoderId();
						if (coderIds[i] != Dna.sql.getActiveCoder().getId()) {
							allOwned = false;
						}
						if (Dna.sql.getActiveCoder().isPermissionEditOthersStatements(coderIds[i]) == false && Dna.sql.getActiveCoder().getId() != coderIds[i]) {
							allOthersEditPermitted = false;
						}
						if (!Dna.sql.getActiveCoder().isPermissionEditOthersStatements(coderIds[i]) && Dna.sql.getActiveCoder().getId() != coderIds[i]) {
							permitRecode = false;
						}
						if (!Dna.sql.getActiveCoder().isPermissionEditOthersStatements() && Dna.sql.getActiveCoder().getId() != coderIds[i]) {
							permitRecode = false;
						}
						if (statementTypeId != statementTableModel.getRow(modelRows[i]).getStatementTypeId()) {
							permitRecode = false;
						}
					}
					if (Dna.sql.getActiveCoder().isPermissionDeleteStatements() == true &&
							(allOwned == true ||
							(Dna.sql.getActiveCoder().isPermissionEditOthersStatements() == true &&	allOthersEditPermitted == true))) {
						MainWindow.this.actionRemoveStatements.setEnabled(true);
					} else {
						MainWindow.this.actionRemoveStatements.setEnabled(false);
					}
					MainWindow.this.actionRecodeStatements.setEnabled(permitRecode);
					// no particular statement to select because multiple statements highlighted in table
				} else {
					MainWindow.this.actionRecodeStatements.setEnabled(false);
					MainWindow.this.actionRemoveStatements.setEnabled(false);
				}
			}
		});
		
		// selection listener for the document table; set contents of the text panel,
		// update statement table, and enable or disable actions
		JTable documentTable = documentTablePanel.getDocumentTable();
		documentTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				if (e.getValueIsAdjusting()) {
					return;
				}
				changedDocumentTableSelection();
			}
		});

		// MouseListener for text window to show popups or context menu in text area; one method for Windows and one for Unix
		JTextPane textWindow = textPanel.getTextWindow();
		textWindow.addMouseListener(new MouseAdapter() {
			public void mouseReleased(MouseEvent me) {
				if (popup == null || !popup.isValid()) {
					try {
						mouseListenPopup(me);
					} catch (ArrayIndexOutOfBoundsException ex) {
						//no documents available
					}
				}
			}
			public void mousePressed(MouseEvent me) {
				if (popup == null || !popup.isValid()) {
					try {
						mouseListenPopup(me);
					} catch (ArrayIndexOutOfBoundsException ex) {
						//no documents available
					}
				}
			}

			public void mouseClicked(MouseEvent me) {
				if (popup == null || !popup.isValid()) {
					try {
						mouseListenSelect(me);
					} catch (ArrayIndexOutOfBoundsException ex) {
						//no documents available
					}
				}
			}

			/**
			 * Show a text popup menu upon right mouse click to insert statements.
			 * 
			 * @param comp  The AWT component on which to draw the dialog window.
			 * @param x     The horizontal coordinate where the dialog should be shown.
			 * @param y     The vertical coordinate where the dialog should be shown.
			 */
			private void popupMenu(Component comp, int x, int y) {
				JPopupMenu popmen = new JPopupMenu();
				ArrayList<StatementType> statementTypes = Dna.sql.getStatementTypes();
				for (int i = 0; i < statementTypes.size(); i++) {
					StatementType statementType = statementTypes.get(i);
					JMenuItem menuItem = new JMenuItem("Format as " + statementType.getLabel());
					menuItem.setOpaque(true);
					menuItem.setBackground(statementType.getColor().toAWTColor());
					popmen.add(menuItem);
					
					menuItem.addActionListener(new ActionListener() {
						@Override
						public void actionPerformed(ActionEvent e) {
							int documentId = documentTablePanel.getSelectedDocumentId();
							int selectionStart = textWindow.getSelectionStart();
							int selectionEnd = textWindow.getSelectionEnd();
							Statement statement = new Statement(selectionStart,
									selectionEnd,
									statementType.getId(),
									Dna.sql.getActiveCoder().getId(),
									statementType.getVariables(),
									documentId);
							int statementId = Dna.sql.addStatement(statement);
							if (statementId > 0) {
								documentTableModel.increaseFrequency(documentId);
								textPanel.paintStatements();
								textWindow.setCaretPosition(selectionEnd);
								
								// retrieve added statement, add to statement table, and open popup
								Statement s = Dna.sql.getStatement(statementId);
								statementTableModel.addRow(s);
								Point location = textWindow.getLocationOnScreen();
								textWindow.setSelectionStart(statement.getStart());
								textWindow.setSelectionEnd(statement.getStop());
								newPopup(x, y, s, location);
							}
						}
					});
					
					// disable menu items if the coder does not have the permission to add statements or edit other coders' documents (if the document belongs to another coder)
					int documentCoderId = documentTableModel.getRow(documentTable.convertRowIndexToModel(documentTable.getSelectedRow())).getCoder().getId();
					if (Dna.sql.getActiveCoder().isPermissionAddStatements() == false ||
							(documentCoderId != Dna.sql.getActiveCoder().getId() && !Dna.sql.getActiveCoder().isPermissionEditOthersDocuments()) ||
							(documentCoderId != Dna.sql.getActiveCoder().getId() &&	!Dna.sql.getActiveCoder().isPermissionEditOthersDocuments(documentCoderId))) {
						menuItem.setEnabled(false);
					}
				}
				popmen.show(comp, x, y);
			}

			/**
			 * Add a new statement and display a popup dialog window with the
			 * new statement.
			 * 
			 * @param me  A mouse event.
			 * @throws ArrayIndexOutOfBoundsException
			 */
			private void mouseListenPopup(MouseEvent me) throws ArrayIndexOutOfBoundsException {
				if (me.isPopupTrigger()) {
					if (!(textWindow.getSelectedText() == null) && Dna.sql.getActiveCoder() != null && Dna.sql.getActiveCoder().isPermissionAddStatements() == true) {
						popupMenu(me.getComponent(), me.getX(), me.getY());
					}
				}
			}

			/**
			 * Check the current position in the text for statements and display
			 * a statement popup window if there is a statement.
			 * 
			 * @param me  The mouse event that triggers the popup window,
			 *   including the location.
			 * @throws ArrayIndexOutOfBoundsException
			 */
			private void mouseListenSelect(MouseEvent me) throws ArrayIndexOutOfBoundsException {
				if (me.isPopupTrigger()) {
					if (!(textWindow.getSelectedText() == null)) {
						popupMenu(me.getComponent(), me.getX(), me.getY());
					}
				} else if (documentTable.getSelectedRowCount() > 0) {
					int pos = textWindow.getCaretPosition(); // click caret position
					Point p = me.getPoint();
					
					// filter statements from statement table using stream API
					List<Statement> currentStatements = statementTableModel.getRows().stream().filter(
							s -> s.getDocumentId() == documentTablePanel.getSelectedDocumentId() &&
							s.getStart() < pos &&
							s.getStop() > pos &&
							(s.getCoderId() == Dna.sql.getActiveCoder().getId() || Dna.sql.getActiveCoder().isPermissionViewOthersStatements()) &&
							(s.getCoderId() == Dna.sql.getActiveCoder().getId() || Dna.sql.getActiveCoder().getCoderRelations().get(s.getCoderId()).isViewStatements())).collect(Collectors.toList());
					
					// if the text selection contains a statement, get it from the database and display it
					if (currentStatements.size() > 0 && Dna.sql.getActiveCoder() != null) {
						Statement s = Dna.sql.getStatement(currentStatements.get(0).getId());
						Point location = textWindow.getLocationOnScreen();
						textWindow.setSelectionStart(s.getStart());
						textWindow.setSelectionEnd(s.getStop());
						newPopup(p.getX(), p.getY(), s, location);
					}
				}
			}
		});

		// toolbar document filter field listener
		JTextField documentFilterField = toolbar.getDocumentFilterField();
		documentFilterField.setBorder(BorderFactory.createLineBorder(Color.BLACK, 1));
		documentFilterField.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void changedUpdate(DocumentEvent arg0) {
				processFilterDocumentChanges();
			}
			@Override
			public void insertUpdate(DocumentEvent arg0) {
				processFilterDocumentChanges();
			}
			@Override
			public void removeUpdate(DocumentEvent arg0) {
				processFilterDocumentChanges();
			}
			
			private void processFilterDocumentChanges() {
				documentTablePanel.setDocumentFilterPattern(documentFilterField.getText());
				documentTableModel.fireTableDataChanged();
			}
		});
		
		// listeners for menu items
		JSpinner fontSizeSpinner = menuBar.getFontSizeSpinner();
		fontSizeSpinner.addChangeListener(new ChangeListener(){
			public void stateChanged(ChangeEvent e) {
				if (Dna.sql.getConnectionProfile() != null) {
					Dna.sql.setCoderFontSize(Dna.sql.getConnectionProfile().getCoderId(), (int) fontSizeSpinner.getValue());
					textPanel.adjustToChangedCoder();
				}
			}
		});
		JSpinner popupWidthSpinner = menuBar.getPopupWidthSpinner();
		popupWidthSpinner.addChangeListener(new ChangeListener(){
			public void stateChanged(ChangeEvent e) {
				if (Dna.sql.getConnectionProfile() != null) {
					Dna.sql.setCoderPopupWidth(Dna.sql.getConnectionProfile().getCoderId(), (int) popupWidthSpinner.getValue());
				}
			}
		});
		JCheckBoxMenuItem popupAutoCompleteItem = menuBar.getPopupAutoCompleteItem();
		popupAutoCompleteItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				if (Dna.sql.getConnectionProfile() != null) {
					Dna.sql.setCoderPopupAutoComplete(Dna.sql.getConnectionProfile().getCoderId(), popupAutoCompleteItem.isSelected());
				}
			}
		});
		JCheckBoxMenuItem popupDecorationItem = menuBar.getPopupDecorationItem();
		popupDecorationItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				if (Dna.sql.getConnectionProfile() != null) {
					Dna.sql.setCoderPopupDecoration(Dna.sql.getConnectionProfile().getCoderId(), popupDecorationItem.isSelected());
				}
			}
		});
		JCheckBoxMenuItem colorByCoderItem = menuBar.getColorByCoderItem();
		colorByCoderItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (Dna.sql.getConnectionProfile() != null) {
					Dna.sql.setColorByCoder(Dna.sql.getConnectionProfile().getCoderId(), colorByCoderItem.isSelected());
					textPanel.adjustToChangedCoder();
					statementTableModel.fireTableDataChanged();
				}
			}
		});
		
		// change coder button listener
		JButton changeCoderButton = coderSelectionPanel.getChangeCoderButton();
		changeCoderButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				boolean authenticated = false;
				int coderIdToSelect = -1;
				if (Dna.sql.getActiveCoder() != null) {
					coderIdToSelect = Dna.sql.getActiveCoder().getId();
				}
				while (!authenticated) {
					CoderPasswordCheckDialog dialog = new CoderPasswordCheckDialog(MainWindow.this, Dna.sql, true, coderIdToSelect, 3);
					Coder coder = dialog.getCoder();
					String clearPassword = dialog.getPassword();
					if (coder != null && clearPassword != null) {
						coderIdToSelect = coder.getId();
						if (Dna.sql.authenticate(coder.getId(), clearPassword)) {
							authenticated = true;
							boolean reloadDocumentTable = coder.differentViewDocumentPermissions(Dna.sql.getActiveCoder());
							boolean reloadStatementTable = coder.differentViewStatementPermissions(Dna.sql.getActiveCoder());
							boolean repaintDocument = coder.differentPaintSettings(Dna.sql.getActiveCoder());
							Dna.sql.selectCoder(coder.getId());
							if (reloadDocumentTable) {
								documentTableModel.fireTableDataChanged();
							}
							if (reloadStatementTable) {
								statementTableModel.fireTableDataChanged();
							}
							if (repaintDocument) {
								textPanel.adjustToChangedCoder();
							}
							coderSelectionPanel.changeCoderBadge();
							menuBar.adjustToChangedCoder();
							MainWindow.this.adjustToCoderSelection();
						} else {
							LogEvent l = new LogEvent(Logger.WARNING,
    								"Authentication failed. Check your password.",
    								"Tried to select coder, but a wrong password was entered for Coder " + coder.getId() + ".");
    						Dna.logger.log(l);
		    				JOptionPane.showMessageDialog(dialog,
		    						"Authentication failed. Check your password.",
		    					    "Check failed",
		    					    JOptionPane.ERROR_MESSAGE);
						}
					} else {
						authenticated = true; // user must have pressed cancel
					}
				}
			}
		});
		
		c.add(mainPanel);
		this.pack();
		this.setLocationRelativeTo(null);
		//this.setVisible(true);
	}
	
	/**
	 * Retrieve the text panel.
	 * 
	 * @return Text panel.
	 */
	private TextPanel getTextPanel() {
		return textPanel;
	}

	/**
	 * Get the statement panel.
	 * 
	 * @return The statement panel.
	 */
	private StatementPanel getStatementPanel() {
		return statementPanel;
	}

	/**
	 * Show a statement popup window.
	 * 
	 * @param x           X location on the screen.
	 * @param y           Y location on the screen.
	 * @param s           The statement to show.
	 * @param location    The location of the popup window.
	 */
	private void newPopup(double x, double y, Statement s, Point location) {
		
		// determine coders for the coder combo box in the popup window
		ArrayList<Coder> eligibleCoders = null;
		if (Dna.sql.getActiveCoder().isPermissionEditStatements() &&
				Dna.sql.getActiveCoder().isPermissionEditOthersStatements() &&
				(Dna.sql.getActiveCoder().isPermissionEditOthersStatements(s.getCoderId()) || Dna.sql.getActiveCoder().getId() == s.getCoderId())) {
			eligibleCoders = Dna.sql.getCoders();
			for (int i = eligibleCoders.size() - 1; i >= 0; i--) {
				if (Dna.sql.getActiveCoder().getId() != eligibleCoders.get(i).getId() &&
						s.getCoderId() != eligibleCoders.get(i).getId() &&
						!Dna.sql.getActiveCoder().isPermissionEditOthersStatements(eligibleCoders.get(i).getId())) {
					eligibleCoders.remove(i);
				}
			}
		}
		
		// create popup window
		this.popup = new Popup(x, y, s, location, Dna.sql.getActiveCoder(), eligibleCoders);
		
		// duplicate button action listener
		JButton duplicate = popup.getDuplicateButton();
		duplicate.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (Dna.sql.getActiveCoder().isPermissionAddStatements() == true) {
					// save popup changes
					if (popup.isEditable() == true && popup.hasWindowDecoration() == true) {
						String message = "Save any changes in Statement " + s.getId() + " before creating copy?";
						int dialog = JOptionPane.showConfirmDialog(popup, message, "Confirmation", JOptionPane.YES_NO_OPTION);
						if (dialog == 0) {
							popup.saveContents(false);
						}
					} else if (popup.isEditable() && popup.hasWindowDecoration() == false) {
						popup.saveContents(false);
					}
					
					// update statement table with changes to old statement that was saved
					statusBar.statementRefreshStart();
					Statement updatedOldStatement = popup.getStatementCopy();
					int modelRow = statementTableModel.getModelRowById(updatedOldStatement.getId());
					statementTableModel.getRow(modelRow).setCoderName(updatedOldStatement.getCoderName());
					statementTableModel.getRow(modelRow).setCoderColor(updatedOldStatement.getCoderColor());
					statementTableModel.fireTableRowsUpdated(modelRow, modelRow);
					
					// clone the statement
					int newStatementId = Dna.sql.cloneStatement(s.getId(), Dna.sql.getActiveCoder().getId());
					
					// repaint statements in text if old statement was changed or new statement successfully created
					if (newStatementId > 0 || (popup.isCoderChanged() && Dna.sql.getActiveCoder().isColorByCoder())) {
						textPanel.paintStatements();
					}
					
					// put a cloned statement into the statement table and update view, then select statement
					if (newStatementId > 0) {
						documentTableModel.increaseFrequency(updatedOldStatement.getDocumentId());
						updatedOldStatement.setId(newStatementId);
						updatedOldStatement.setCoderId(Dna.sql.getActiveCoder().getId());
						updatedOldStatement.setCoderName(Dna.sql.getActiveCoder().getName());
						updatedOldStatement.setCoderColor(Dna.sql.getActiveCoder().getColor());
						statementTableModel.addRow(updatedOldStatement);
						statementPanel.setSelectedStatementId(newStatementId);
					}
					popup.dispose();
					statusBar.statementRefreshEnd();
				}
			}
		});
		
		// remove button action listener
		JButton remove = popup.getRemoveButton();
		remove.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int question = JOptionPane.showConfirmDialog(MainWindow.this, 
						"Are you sure you want to remove this statement?", 
						"Remove?", JOptionPane.YES_NO_OPTION);
				if (question == 0) {
					statusBar.statementRefreshStart();
					boolean deleted = Dna.sql.deleteStatements(new int[] {s.getId()});
					if (deleted) {
						getTextPanel().paintStatements();
						documentTableModel.decreaseFrequency(s.getDocumentId());
						int statementModelRow = statementTableModel.getModelRowById(s.getId());
						getStatementPanel().getStatementTable().clearSelection();
						statementTableModel.removeStatements(new int[] {statementModelRow});

						// log deleted statements
						LogEvent l = new LogEvent(Logger.MESSAGE,
								"[GUI] Action executed: removed statement(s).",
								"Deleted statement(s) in the database and GUI.");
						Dna.logger.log(l);
						
						popup.dispose();
					}
					statusBar.statementRefreshEnd();
				}
			}
		});
		
		// save and close window or focus listener
		if (popup.hasWindowDecoration() == true) {
			popup.addWindowListener(new WindowAdapter() { // listener for the X button in the window decoration
				public void windowClosing(WindowEvent e) {
					if (popup.isEditable() == true) {
						if (popup.saveContents(true) == true) { // check first if there are any changes; ask to save only if necessary
							String message = "Save changes in Statement " + s.getId() + "?";
							int dialog = JOptionPane.showConfirmDialog(popup, message, "Confirmation", JOptionPane.YES_NO_OPTION);
							if (dialog == 0) {
								popupSave(popup);
							}
						}
					}
					popup.dispose();
					statementPanel.getStatementTable().clearSelection(); // clear statement table selection when popup window closed
				}
			});
			popup.getCancelButton().addActionListener(new ActionListener() { // cancel button action listener
				@Override
				public void actionPerformed(ActionEvent arg0) {
					popup.dispose();
					statementPanel.getStatementTable().clearSelection(); // clear statement table selection when popup window closed
				}
			});
			popup.getSaveButton().addActionListener(new ActionListener() { // save button action listener
				@Override
				public void actionPerformed(ActionEvent arg0) {
					popupSave(popup);
					popup.dispose();
					statementPanel.getStatementTable().clearSelection(); // clear statement table selection when popup window closed
				}
			});
			// popup.setModal(true); // disabled for now: set modal after adding controls because otherwise controls can't be added anymore while modal
		} else { // no window decoration: focus lost listener
			popup.addWindowFocusListener(new WindowAdapter() {
				public void windowLostFocus(WindowEvent e) {
					popupSave(popup);
					popup.dispose();
					statementPanel.getStatementTable().clearSelection(); // clear statement table selection when popup window closed
				}
			});
		}
		popup.setVisible(true); // needs to be called after setting modal; hence here instead of in the Popup class
	}
	
	/**
	 * Save the contents of a popup window and repaint statements in the text
	 * and statement table if the coder has changed.
	 * 
	 * @param popup  The popup window.
	 */
	private void popupSave(Popup popup) {
		popup.saveContents(false);
		if (popup.isCoderChanged()) {
			if (Dna.sql.getActiveCoder().isColorByCoder()) {
				textPanel.paintStatements();
			}
			Statement s = popup.getStatementCopy();
			int modelRow = statementTableModel.getModelRowById(s.getId());
			statementTableModel.getRow(modelRow).setCoderName(s.getCoderName());
			statementTableModel.getRow(modelRow).setCoderColor(s.getCoderColor());
			statementTableModel.fireTableRowsUpdated(modelRow, modelRow);
		}
	}

	/**
	 * Refresh the document table using a Swing worker in the background.
	 */
	private void refreshDocumentTable() {
		if (Dna.sql.getConnectionProfile() == null) {
			documentTableModel.clear();
		} else {
			if (documentTableWorker != null) {
				documentTableWorker.cancel(true);
				statusBar.documentRefreshEnd();
			}
	        documentTableWorker = new DocumentTableRefreshWorker(LocalTime.now().toString());
	        documentTableWorker.execute();
		}
	}

	/**
	 * Refresh the statement table using a Swing worker in the background and
	 * select the previously selected statement again (if applicable and
	 * available).
	 * 
	 * @param statementIds  An array of the statement IDs to refresh. Can be
	 *   of length zero to refresh all statements.
	 */
	private void refreshStatementTable(int[] statementIds) {
		if (Dna.sql.getConnectionProfile() == null) {
			statementTableModel.clear();
		} else {
			if (statementTableWorker != null) {
				statementTableWorker.cancel(true);
				statusBar.statementRefreshEnd();
			}
	        statementTableWorker = new StatementTableRefreshWorker(LocalTime.now().toString(), statementIds);
	        statementTableWorker.execute();
		}
	}

	/**
	 * Swing worker class for loading documents from the database and adding
	 * them to the document table in a background thread.
	 * 
	 * @see <a href="https://stackoverflow.com/questions/43161033/cant-add-tablerowsorter-to-jtable-produced-by-swingworker" target="_top">https://stackoverflow.com/questions/43161033/</a>
	 * @see <a href="https://stackoverflow.com/questions/68884145/how-do-i-use-a-jdbc-swing-worker-with-connection-pooling-ideally-while-separati" target="_top">https://stackoverflow.com/questions/68884145/</a>
	 */
	private class DocumentTableRefreshWorker extends SwingWorker<List<TableDocument>, TableDocument> {
		/**
		 * Time stamp to measure the duration it takes to update the table. The
		 * duration is logged when the table has been updated.
		 */
		private long time;
		
		/**
		 * ID of the selected document to reinstated the selection after
		 * repopulating the document table.
		 */
		private int selectedId;

		/**
		 * Vertical scroll position of the scroll pane in the text window before
		 * reloading the documents, in order to reinstate the scroll position
		 * after refreshing the document table.
		 */
		//private int verticalScrollLocation;
		
		/**
		 * A name for the swing worker thread to identify it in the message log.
		 */
		private String name;

		/**
		 * Create a new document table swing worker.
		 * 
		 * @param name  The name of the thread.
		 */
		private DocumentTableRefreshWorker(String name) {
			this.name = name;
			actionAddDocument.setEnabled(false);
			actionRemoveDocuments.setEnabled(false);
			actionEditDocuments.setEnabled(false);
			actionBatchImportDocuments.setEnabled(false);
			actionRefresh.setEnabled(false);
			statusBar.documentRefreshStart();
			time = System.nanoTime(); // take the time to compute later how long the updating took
			//verticalScrollLocation = textPanel.getVerticalScrollLocation();
			selectedId = documentTablePanel.getSelectedDocumentId(); // remember the document ID to select the same document when done
			documentTableModel.clear(); // remove all documents from the table model before re-populating the table
			LogEvent le = new LogEvent(Logger.MESSAGE,
					"[GUI] Initializing thread to populate document table: " + this.getName() + ".",
					"A new swing worker thread has been started to populate the document table with documents from the database in the background: " + this.getName() + ".");
			Dna.logger.log(le);
		}
		
		@Override
		protected List<TableDocument> doInBackground() {
			try (Connection conn = Dna.sql.getDataSource().getConnection();
					PreparedStatement s = conn.prepareStatement("SELECT D.ID, Title, (SELECT COUNT(ID) FROM STATEMENTS WHERE DocumentId = D.ID) AS Frequency, C.ID AS CoderId, Name AS CoderName, Red, Green, Blue, Date, Author, Source, Section, Type, Notes FROM CODERS C INNER JOIN DOCUMENTS D ON D.Coder = C.ID;");
					ResultSet rs = s.executeQuery();) {
				while (!isCancelled() && rs.next()) {
					if (isCancelled()) {
						return null;
					}
					TableDocument r = new TableDocument(
							rs.getInt("ID"),
							rs.getString("Title"),
							rs.getInt("Frequency"),
							new Coder(rs.getInt("CoderId"),
									rs.getString("CoderName"),
									new model.Color(rs.getInt("Red"), rs.getInt("Green"), rs.getInt("Blue"))),
							rs.getString("Author"),
							rs.getString("Source"),
							rs.getString("Section"),
							rs.getString("Type"),
							rs.getString("Notes"),
							LocalDateTime.ofEpochSecond(rs.getLong("Date"), 0, ZoneOffset.UTC));
					publish(r); // send the new document row out of the background thread
				}
			} catch (SQLException e) {
				if (e.getMessage().matches(".*Interrupted during connection acquisition.*")) {
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI]  ├─ Document retrieval canceled in Thread " + this.getName() + ".",
							"Refreshing the document table by reloading all documents from the database and populating the document table with them was canceled, presumably because a new swing worker to retrieve documents was initiated, which then superseded the existing thread.",
							e);
					Dna.logger.log(l);
				} else {
					LogEvent le = new LogEvent(Logger.WARNING,
							"[SQL]  ├─ Could not retrieve documents from database.",
							"The document table model swing worker tried to retrieve all documents from the database to display them in the document table, but some or all documents could not be retrieved because there was a problem while processing the result set. The document table may be incomplete.",
							e);
					Dna.logger.log(le);
				}
			}
			return null;
		}
	    
	    @Override
	    protected void process(List<TableDocument> chunks) {
	    	documentTableModel.addRows(chunks); // transfer a batch of rows to the table model
	    }

	    @Override
	    protected void done() {
        	documentTableModel.sort();
        	documentTablePanel.setSelectedDocumentId(this.selectedId);
        	//textPanel.setVerticalScrollLocation(this.verticalScrollLocation);
			long elapsed = System.nanoTime(); // measure time again for calculating difference
			LogEvent le = new LogEvent(Logger.MESSAGE,
					"[GUI]  ├─ (Re)loaded all " + documentTableModel.getRowCount() + " documents in " + (elapsed - time) / 1000000 + " milliseconds.",
					"The document table swing worker loaded the " + documentTableModel.getRowCount() + " documents from the DNA database in the "
					+ "background and stored them in the document table. This took " + (elapsed - time) / 1000000 + " milliseconds.");
			Dna.logger.log(le);
			le = new LogEvent(Logger.MESSAGE,
					"[GUI]  └─ Closing thread to populate document table: " + this.getName() + ".",
					"The document table has been populated with documents from the database. Closing thread: " + this.getName() + ".");
			Dna.logger.log(le);
			statusBar.documentRefreshEnd();
			if (!statusBar.isRefreshInProgress()) {
				if (Dna.sql.getConnectionProfile() != null) {
					actionRefresh.setEnabled(true);
				} else {
					actionRefresh.setEnabled(false);
				}
			}
			changedDocumentTableSelection();
	    }
	    
	    /**
	     * Get the name of the thread.
	     * 
	     * @return  Thread name.
	     */
	    String getName() {
	    	return this.name;
	    }
	}

	/**
	 * Swing worker class for loading statements from the database and adding
	 * them to the statement table in a background thread. The class contains
	 * SQL code right here instead of moving the SQL code to the {@link sql.Sql
	 * Sql} class because it contains nested {@code publish} calls that need
	 * to be in the SQL code but also need to remain in the Swing worker class. 
	 */
	private class StatementTableRefreshWorker extends SwingWorker<List<Statement>, Statement> {
		/**
		 * Time stamp to measure the duration it takes to update the table. The
		 * duration is logged when the table has been updated.
		 */
		private long time;

		/**
		 * A name for the swing worker thread to identify it in the message log.
		 */
		private String name;

		/**
		 * The statement IDs to refresh. Can be of length zero to reload all
		 * statements.
		 */
		private int[] statementIds;
		
		/**
		 * ID of the selected statement in the statement table, to restore it
		 * later and scroll back to the same position in the table after update.
		 */
		private int selectedId;

		/**
		 * A Swing worker that reloads all statements from the database and
		 * stores them in the table model for displaying them in the statement
		 * table. Selects the previously selected statement when done.
		 */
		private StatementTableRefreshWorker(String name, int[] statementIds) {
			this.name = name;
			this.statementIds = statementIds;
			selectedId = getStatementPanel().getSelectedStatementId();
			initialiseRefreshWorker();
		}
		
		/**
		 * A Swing worker that reloads all statements from the database and
		 * stores them in the table model for displaying them in the statement
		 * table.
		 * 
		 * @param statementIdToSelect The ID of the statement that should be
		 *   selected when done refreshing.
		 */
		private StatementTableRefreshWorker(String name, int[] statementIds, int statementIdToSelect) {
			this.name = name;
			this.statementIds = statementIds;
			selectedId = statementIdToSelect;
			initialiseRefreshWorker();
		}
		
		/**
		 * Initialise the statement table refresh worker. This common code is
		 * executed by both constructors.
		 */
		private void initialiseRefreshWorker() {
			actionRefresh.setEnabled(false);
    		time = System.nanoTime(); // take the time to compute later how long the updating took
    		statusBar.statementRefreshStart();
    		if (this.statementIds.length == 0) {
    			statementTableModel.clear(); // remove all documents from the table model before re-populating the table
    		}
			LogEvent le = new LogEvent(Logger.MESSAGE,
					"[GUI] Initializing thread to populate statement table: " + this.getName() + ".",
					"A new swing worker thread has been started to populate the statement table with statements from the database in the background: " + this.getName() + ".");
			Dna.logger.log(le);
		}
		
		@Override
		protected List<Statement> doInBackground() {
			String subString = "SUBSTRING(DOCUMENTS.Text, Start + 1, Stop - Start) AS Text ";
			if (Dna.sql.getConnectionProfile().getType().equals("postgresql")) {
				subString = "SUBSTRING(DOCUMENTS.Text, CAST(Start + 1 AS INT4), CAST(Stop - Start AS INT4)) AS Text ";
			}
			String stid = "";
			if (statementIds.length > 0) {
				stid = "WHERE STATEMENTS.ID IN ("
						+ IntStream.of(statementIds)
						.mapToObj(i -> ((Integer) i).toString()) // i is an int, not an Integer
						.collect(Collectors.joining(", "))
						.toString()
						+ ") ";
			}
			String q1 = "SELECT STATEMENTS.ID AS StatementId, "
					+ "StatementTypeId, "
					+ "STATEMENTTYPES.Label AS StatementTypeLabel, "
					+ "STATEMENTTYPES.Red AS StatementTypeRed, "
					+ "STATEMENTTYPES.Green AS StatementTypeGreen, "
					+ "STATEMENTTYPES.Blue AS StatementTypeBlue, "
					+ "Start, "
					+ "Stop, "
					+ "STATEMENTS.Coder AS CoderId, "
					+ "CODERS.Name AS CoderName, "
					+ "CODERS.Red AS CoderRed, "
					+ "CODERS.Green AS CoderGreen, "
					+ "CODERS.Blue AS CoderBlue, "
					+ "DocumentId, "
					+ "DOCUMENTS.Date AS Date, "
					+ subString
					+ "FROM STATEMENTS "
					+ "INNER JOIN CODERS ON STATEMENTS.Coder = CODERS.ID "
					+ "INNER JOIN STATEMENTTYPES ON STATEMENTS.StatementTypeId = STATEMENTTYPES.ID "
					+ "INNER JOIN DOCUMENTS ON DOCUMENTS.ID = STATEMENTS.DocumentId "
					+ stid
					+ "ORDER BY DOCUMENTS.DATE ASC;";
			
			String q2 = "SELECT ID FROM STATEMENTTYPES;";
			
			String q3 = "SELECT ID, Variable, DataType FROM VARIABLES;";
			
			String q4castBoolean = "DATABOOLEAN.Value";
			String q4castInteger = "DATAINTEGER.Value";
			if (Dna.sql.getConnectionProfile().getType().equals("postgresql")) {
				q4castBoolean = "CAST(DATABOOLEAN.Value AS TEXT)";
				q4castInteger = "CAST(DATAINTEGER.Value AS TEXT)";
			}
			
			String stidShort = "";
			String stidLong = "";
			String stidBool = "";
			String stidInt = "";
			if (statementIds.length > 0) {
				stid = IntStream.of(statementIds)
						.mapToObj(i -> ((Integer) i).toString()) // i is an int, not an Integer
						.collect(Collectors.joining(", "))
						.toString()
						+ ") ";
				stidShort = "AND DATASHORTTEXT.StatementId IN (" + stid;
				stidLong = "AND DATALONGTEXT.StatementId IN (" + stid;
				stidBool = "AND DATABOOLEAN.StatementId IN (" + stid;
				stidInt = "AND DATAINTEGER.StatementId IN (" + stid;
			}
			String q4 = "SELECT DATASHORTTEXT.StatementId, VARIABLES.ID AS VariableId, ENTITIES.Value AS Value FROM DATASHORTTEXT "
					+ "INNER JOIN VARIABLES ON VARIABLES.ID = DATASHORTTEXT.VariableId "
					+ "INNER JOIN ENTITIES ON ENTITIES.VariableId = VARIABLES.ID AND ENTITIES.ID = DATASHORTTEXT.Entity WHERE VARIABLES.StatementTypeId = ? "
					+ stidShort
					+ "UNION "
					+ "SELECT DATALONGTEXT.StatementId, VARIABLES.ID AS VariableId, DATALONGTEXT.Value FROM DATALONGTEXT "
					+ "INNER JOIN VARIABLES ON VARIABLES.ID = DATALONGTEXT.VariableId WHERE VARIABLES.StatementTypeId = ? "
					+ stidLong
					+ "UNION "
					+ "SELECT DATABOOLEAN.StatementId, VARIABLES.ID AS VariableId, " + q4castBoolean + " FROM DATABOOLEAN "
					+ "INNER JOIN VARIABLES ON VARIABLES.ID = DATABOOLEAN.VariableId WHERE VARIABLES.StatementTypeId = ? "
					+ stidBool
					+ "UNION "
					+ "SELECT DATAINTEGER.StatementId, VARIABLES.ID AS VariableId, " + q4castInteger + " FROM DATAINTEGER "
					+ "INNER JOIN VARIABLES ON VARIABLES.ID = DATAINTEGER.VariableId WHERE VARIABLES.StatementTypeId = ? "
					+ stidInt
					+ "ORDER BY 1, 2 ASC;";
			
			int statementTypeId, statementId, variableId;
			model.Color sColor, cColor;
			HashMap<Integer, String> variableNameMap = new HashMap<Integer, String>(); // variable ID to variable name
			HashMap<Integer, String> variableDataTypeMap = new HashMap<Integer, String>(); // variable ID to data type
			HashMap<Integer, Statement> statementMap = new HashMap<Integer, Statement>(); // statement ID to Statement
			ResultSet r3, r4;
			try (Connection conn = Dna.sql.getDataSource().getConnection();
					PreparedStatement s1 = conn.prepareStatement(q1);
					PreparedStatement s2 = conn.prepareStatement(q2);
					PreparedStatement s3 = conn.prepareStatement(q3);
					PreparedStatement s4 = conn.prepareStatement(q4);) {
				
				// assemble statements without values for now and save them in a hash map
				ResultSet r1 = s1.executeQuery();
				while (r1.next()) {
					statementId = r1.getInt("StatementId");
				    statementTypeId = r1.getInt("StatementTypeId");
				    sColor = new model.Color(r1.getInt("StatementTypeRed"), r1.getInt("StatementTypeGreen"), r1.getInt("StatementTypeBlue"));
				    cColor = new model.Color(r1.getInt("CoderRed"), r1.getInt("CoderGreen"), r1.getInt("CoderBlue"));
				    Statement statement = new Statement(statementId,
				    		r1.getInt("Start"),
				    		r1.getInt("Stop"),
				    		statementTypeId,
				    		r1.getString("StatementTypeLabel"),
				    		sColor,
				    		r1.getInt("CoderId"),
				    		r1.getString("CoderName"),
				    		cColor,
				    		new ArrayList<Value>(),
				    		r1.getInt("DocumentId"),
				    		r1.getString("Text"),
				    		LocalDateTime.ofEpochSecond(r1.getLong("Date"), 0, ZoneOffset.UTC));
				    statementMap.put(statementId, statement);
				}
				
				// get variables
				r3 = s3.executeQuery();
				while (r3.next()) {
					variableNameMap.put(r3.getInt("ID"), r3.getString("Variable"));
					variableDataTypeMap.put(r3.getInt("ID"), r3.getString("DataType"));
				}
				
				// get statement types
				ResultSet r2 = s2.executeQuery();
				while (r2.next()) {
					statementTypeId = r2.getInt("ID");
					
					// get values and put them into the statements
					s4.setInt(1, statementTypeId);
					s4.setInt(2, statementTypeId);
					s4.setInt(3, statementTypeId);
					s4.setInt(4, statementTypeId);
					r4 = s4.executeQuery();
					while (r4.next()) {
						variableId = r4.getInt("VariableId");
						statementMap.get(r4.getInt("StatementId")).getValues().add(new Value(variableId, variableNameMap.get(variableId), variableDataTypeMap.get(variableId), r4.getString("Value")));
					}
				}
				
				// publish all statements
				Collection<Statement> s = statementMap.values();
		        ArrayList<Statement> listOfStatements = new ArrayList<Statement>(s);
				Collections.sort(listOfStatements);
		        for (int i = 0; i < listOfStatements.size(); i++) {
		        	publish(listOfStatements.get(i));
		        }
			} catch (SQLException e) {
				if (e.getMessage().matches(".*Interrupted during connection acquisition.*")) {
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI]  ├─ Statement retrieval canceled in Thread " + this.getName() + ".",
							"Refreshing the statement table by reloading all statements from the database and populating the statement table with them was canceled, presumably because a new swing worker to retrieve statements was initiated, which then superseded the existing thread.",
							e);
					Dna.logger.log(l);
				} else {
					LogEvent l = new LogEvent(Logger.WARNING,
							"[SQL] Failed to retrieve statements.",
							"Attempted to retrieve all statements from the database, but something went wrong. You should double-check if the statements are all shown!",
							e);
					Dna.logger.log(l);
				}
			}
			return null;
		}
        
        @Override
        protected void process(List<Statement> chunks) {
        	if (statementIds.length == 0) {
        		statementTableModel.addRows(chunks); // transfer a batch of rows to the statement table model
    			getStatementPanel().setSelectedStatementId(selectedId); // select the statement from before; skipped if the statement not found in this batch
        	} else {
        		statementTableModel.updateStatements(chunks); // transfer a batch of statements to update
        	}
        }

        @Override
        protected void done() {
        	if (statementIds.length == 0) {
            	statementTableModel.sort();
        		statementTableModel.fireTableDataChanged(); // update the statement filter
    			getStatementPanel().setSelectedStatementId(selectedId);
        		long elapsed = System.nanoTime(); // measure time again for calculating difference
    			LogEvent le = new LogEvent(Logger.MESSAGE,
        				"[GUI]  ├─ (Re)loaded all " + statementTableModel.getRowCount() + " statements in " + (elapsed - time) / 1000000 + " milliseconds.",
        				"The statement table swing worker loaded the " + statementTableModel.getRowCount() + " statements from the DNA database in the "
        				+ "background and stored them in the statement table. This took " + (elapsed - time) / 1000000 + " milliseconds.");
        		Dna.logger.log(le);
    		} else {
        		long elapsed = System.nanoTime(); // measure time again for calculating difference
    			LogEvent le = new LogEvent(Logger.MESSAGE,
    					"[GUI]  ├─ Refreshed " + statementIds.length + " statement(s) in " + (elapsed - time) / 1000000 + " milliseconds.",
        				"The statement table swing worker loaded the " + statementIds.length + " statement(s) from the DNA database in the "
        				+ "background and updated them in the statement table. This took " + (elapsed - time) / 1000000 + " milliseconds.");
        		Dna.logger.log(le);
    		}
        	statusBar.statementRefreshEnd(); // stop displaying the update message in the status bar
    		
			LogEvent le = new LogEvent(Logger.MESSAGE,
					"[GUI]  └─ Closing thread to populate statement table: " + this.getName() + ".",
					"The statement table has been populated with statements from the database. Closing thread: " + this.getName() + ".");
			Dna.logger.log(le);
			if (!statusBar.isRefreshInProgress()) {
				if (Dna.sql.getConnectionProfile() != null) {
					actionRefresh.setEnabled(true);
				} else {
					actionRefresh.setEnabled(false);
				}
			}
        }
	    
	    /**
	     * Get the name of the thread.
	     * 
	     * @return  Thread name.
	     */
	    String getName() {
	    	return this.name;
	    }
    }

	/**
	 * Adjust controls to document table selection.
	 */
	private void changedDocumentTableSelection() {
		JTable documentTable = documentTablePanel.getDocumentTable();
		int rowCount = documentTable.getSelectedRowCount();
		boolean allOwned = true;
		boolean allOthersEditPermitted = true;
		int[] rows = documentTable.getSelectedRows();
		int[] documentIds = new int[rowCount];
		if (rowCount > 0) {
			for (int i = 0; i < rows.length; i++) {
				int modelRow = documentTable.convertRowIndexToModel(rows[i]);
				int coderId = documentTableModel.getRow(modelRow).getCoder().getId();
				if (coderId != Dna.sql.getActiveCoder().getId()) {
					allOwned = false; // does the active coder own all selected documents?
					if (Dna.sql.getActiveCoder().isPermissionEditOthersDocuments(coderId) == false) {
						allOthersEditPermitted = false;
					}
				}
				documentIds[i] = documentTableModel.getRow(modelRow).getId();
			}
		}

		// notify listeners (e.g., regex text search dialog windows)
		for (DocumentTablePanel.DocumentTableListener l : this.documentTablePanel.getListeners()) {
			l.documentSelected(documentIds);
		}

		// enable or disable action for deleting documents depending on selection and user rights
		if (rowCount > 0 && Dna.sql.getActiveCoder().isPermissionDeleteDocuments() == true &&
				(Dna.sql.getActiveCoder().isPermissionEditOthersDocuments() == true || allOwned == true) &&
				allOthersEditPermitted == true) {
			actionRemoveDocuments.setEnabled(true);
		} else {
			actionRemoveDocuments.setEnabled(false);
		}
		// enable or disable action for editing documents depending on selection and user rights
		if (rowCount > 0 && Dna.sql.getActiveCoder().isPermissionEditDocuments() == true &&
				(Dna.sql.getActiveCoder().isPermissionEditOthersDocuments() == true || allOwned == true) &&
				allOthersEditPermitted == true) {
			actionEditDocuments.setEnabled(true);
		} else {
			actionEditDocuments.setEnabled(false);
		}
		if (rowCount == 0 || rowCount > 1) {
			textPanel.setContents(-1, "");
			getStatementPanel().setDocumentId(-1);
			statementTableModel.fireTableDataChanged();
		} else if (rowCount == 1) {
			int selectedRow = documentTable.getSelectedRow();
			int selectedModelIndex = documentTable.convertRowIndexToModel(selectedRow);
			int id = documentTableModel.getIdByModelRow(selectedModelIndex);
			textPanel.setContents(id, Dna.sql.getDocumentText(id));
			getStatementPanel().setDocumentId(id);
			statementTableModel.fireTableDataChanged();
		} else {
			LogEvent l = new LogEvent(Logger.WARNING,
					"[GUI] Negative number of rows in the document table!",
					"When a document is selected in the document table in the DNA coding window, the text of the document is displayed in the text panel. When checking which row in the table was selected, it was found that the table contained negative numbers of documents. This is obviously an error. Please report it by submitting a bug report along with the saved log.");
			Dna.logger.log(l);
		}
		
		// enable or disable actions for adding and importing documents
		if (Dna.sql.getActiveCoder() == null || Dna.sql.getConnectionProfile() == null) {
			actionAddDocument.setEnabled(false);
			actionBatchImportDocuments.setEnabled(false);
		} else {
			if (Dna.sql.getActiveCoder().isPermissionAddDocuments() == true) {
				actionAddDocument.setEnabled(true);
				actionBatchImportDocuments.setEnabled(true);
			} else {
				actionAddDocument.setEnabled(false);
				actionBatchImportDocuments.setEnabled(false);
			}
			if (Dna.sql.getActiveCoder().isPermissionImportDocuments() == true) {
				actionImporter.setEnabled(true);
			} else {
				actionImporter.setEnabled(false);
			}
		}
	}
	
	/**
	 * React to changes in the active coder in the {@link Sql} class.
	 */
	public void adjustToCoderSelection() {
		actionCloseDatabase.setEnabled(true);
		actionCreateDatabase.setEnabled(true);
		actionOpenProfile.setEnabled(true);
		actionSaveProfile.setEnabled(true);
		if (Dna.sql.getActiveCoder() != null && Dna.sql.getActiveCoder().isPermissionEditRegex() == true) {
			actionRegexEditor.setEnabled(true);
		} else {
			actionRegexEditor.setEnabled(false);
		}
		if (Dna.sql.getActiveCoder() != null && Dna.sql.getActiveCoder().isPermissionEditCoders() == true) {
			actionCoderManager.setEnabled(true);
		} else {
			actionCoderManager.setEnabled(false);
		}
		changedDocumentTableSelection();
		actionRefresh.setEnabled(true);
		actionRecodeStatements.setEnabled(false);
		actionRemoveStatements.setEnabled(false);
		if (Dna.sql.getActiveCoder() != null && Dna.sql.getActiveCoder().isPermissionEditStatementTypes() == true) {
			actionStatementTypeEditor.setEnabled(true);
		} else {
			actionStatementTypeEditor.setEnabled(false);
		}
		if (Dna.sql.getActiveCoder() != null && Dna.sql.getActiveCoder().isPermissionEditAttributes() == true) {
			actionAttributeManager.setEnabled(true);
		} else {
			actionAttributeManager.setEnabled(false);
		}
		if (Dna.sql.getActiveCoder() != null && Dna.sql.getActiveCoder().isPermissionEditCoderRelations() == true && Dna.sql.getActiveCoder().getId() != 1) {
			actionCoderRelationsEditor.setEnabled(true);
		} else {
			actionCoderRelationsEditor.setEnabled(false);
		}
		if (Dna.sql.getActiveCoder() != null && Dna.sql.getConnectionProfile() != null) {
			actionSearchDialog.setEnabled(true);
		} else {
			actionSearchDialog.setEnabled(false);
		}
		if (Dna.sql.getConnectionProfile() != null) {
			actionNetworkExporter.setEnabled(true);
			actionBackboneExporter.setEnabled(true);
		} else {
			actionNetworkExporter.setEnabled(false);
			actionBackboneExporter.setEnabled(false);
		}
	}
	
	/**
	 * An action to display a dialog to open a database.
	 */
	class ActionOpenDatabase extends AbstractAction {
		private static final long serialVersionUID = 5076889002458881750L;
		
		public ActionOpenDatabase(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			NewDatabaseDialog n = new NewDatabaseDialog(MainWindow.this, true);
			ConnectionProfile cp = n.getConnectionProfile();
			String version = new Sql(cp, false).getVersion();
			if (!version.startsWith("3.0")) {
				LogEvent le = new LogEvent(Logger.ERROR,
						"[GUI] Tried to open an incompatible database version.",
						"You tried to open a DNA database with version " + version + ", but you can only open databases with version 3.0. Data from version 2 databases can also be imported into a new or existing DNA 3 database using the importer in the Documents menu.");
				Dna.logger.log(le);
			} else if (cp != null) {
				Dna.sql.setConnectionProfile(cp, false);
				refreshDocumentTable();
				refreshStatementTable(new int[0]);
				adjustToCoderSelection();
				
				// changes in other classes
				statusBar.updateUrl();
				toolbar.adjustToChangedConnection();
				textPanel.setContents(-1, "");
				statementPanel.adjustToChangedConnection();
				menuBar.adjustToChangedCoder();
				coderSelectionPanel.changeCoderBadge();

				if (cp == null) {
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI] Action executed: could not open database.",
							"Started opening a database connection from the GUI, but the connection was not established.");
					Dna.logger.log(l);
				} else {
					Dna.sql.setConnectionProfile(cp, false); // not a connection test, so false
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI] Action executed: opened database.",
							"Opened a database connection from the GUI.");
					Dna.logger.log(l);
				}
			}
		}
	}
	
	/**
	 * An action to close the open SQL database.
	 */
	class ActionCloseDatabase extends AbstractAction {
		private static final long serialVersionUID = -4463742124397662610L;
		
		public ActionCloseDatabase(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			Dna.sql.selectCoder(-1);
			
			// subsequent changes in main window class
			actionCloseDatabase.setEnabled(false);
			actionCreateDatabase.setEnabled(true);
			actionOpenProfile.setEnabled(true);
			actionSaveProfile.setEnabled(false);
			actionRegexEditor.setEnabled(false);
			actionCoderManager.setEnabled(false);
			actionAddDocument.setEnabled(false);
			actionRemoveDocuments.setEnabled(false);
			actionEditDocuments.setEnabled(false);
			actionBatchImportDocuments.setEnabled(false);
			actionImporter.setEnabled(false);
			actionRefresh.setEnabled(false);
			actionRemoveStatements.setEnabled(false);
			actionStatementTypeEditor.setEnabled(false);
			actionAttributeManager.setEnabled(false);
			actionCoderRelationsEditor.setEnabled(false);
			documentTableModel.clear();
			statementTableModel.clear();
			
			// changes in other classes
			statusBar.updateUrl();
			toolbar.adjustToChangedConnection();
			textPanel.setContents(-1, "");
			statementPanel.adjustToChangedConnection();
			menuBar.adjustToChangedCoder();
			coderSelectionPanel.changeCoderBadge();
			
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: closed database.",
					"Closed database connection from the GUI.");
			Dna.logger.log(l);
		}
	}
	
	/**
	 * An action to display a new database dialog to create a new DNA database.
	 */
	class ActionCreateDatabase extends AbstractAction {
		private static final long serialVersionUID = -9019267411134217476L;

		public ActionCreateDatabase(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			NewDatabaseDialog n = new NewDatabaseDialog(MainWindow.this, false);
			ConnectionProfile cp = n.getConnectionProfile();
			
			if (cp != null) {
				Dna.sql.setConnectionProfile(cp, false); // this is after creating data structures, so no test (= false)
				refreshDocumentTable();
				refreshStatementTable(new int[0]);
				adjustToCoderSelection();
				
				// changes in other classes
				statusBar.updateUrl();
				toolbar.adjustToChangedConnection();
				textPanel.setContents(-1, "");
				statementPanel.adjustToChangedConnection();
				menuBar.adjustToChangedCoder();
				coderSelectionPanel.changeCoderBadge();
			}
			
			if (cp == null) {
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: new database was not created.",
						"Started creating a new database from the GUI, but a connection was not established.");
				Dna.logger.log(l);
			} else {
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: created new database.",
						"Created a new database from the GUI.");
				Dna.logger.log(l);
			}
		}
	}
	
	/**
	 * An action to display a file chooser and open a connection profile.
	 */
	class ActionOpenProfile extends AbstractAction {
		private static final long serialVersionUID = -1945734783855268915L;
		
		public ActionOpenProfile(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			FileChooser fc = new FileChooser(MainWindow.this, "Open profile", false, ".dnc", "DNA connection profile (*.dnc)", false);
			if (fc.getFiles() != null && fc.getFiles().length > 0 && fc.getFiles()[0] != null && fc.getFiles()[0].exists()) {
				String filename = new String(fc.getFiles()[0].getPath());
				boolean validPasswordInput = false;
				while (!validPasswordInput) {
					// ask user for password (for the user in the connection profile) to decrypt the profile
					CoderPasswordCheckDialog d = new CoderPasswordCheckDialog(MainWindow.this);
					String key = d.getPassword();

					// decrypt connection profile, create SQL connection, and set as default SQL connection
					if (key == null) {
						validPasswordInput = true; // user must have pressed cancel; quit the while-loop
					} else {
						if (!key.equals("")) {
							ConnectionProfile cp = null;
							try {
								cp = new ConnectionProfile(filename, key);
							} catch (EncryptionOperationNotPossibleException e2) {
								cp = null;
							}
							if (cp != null) {
								Sql sqlTemp = new Sql(cp, true); // just for authentication purposes, so a test
								if (sqlTemp.getDataSource() == null) {
									LogEvent l = new LogEvent(Logger.ERROR,
											"[GUI] No data source available in the database connection.",
											"Tried to open database, but the coder could not be authenticated because there is no data source available. This may be due to a failed connection to the database. Look out for other error messages.");
									Dna.logger.log(l);
									JOptionPane.showMessageDialog(MainWindow.this,
											"The connection to the database failed or was denied.",
											"Connection failed",
											JOptionPane.ERROR_MESSAGE);
								} else {
									boolean authenticated = sqlTemp.authenticate(-1, key);
									if (authenticated == true) {
										validPasswordInput = true; // authenticated; quit the while-loop
										Dna.sql.setConnectionProfile(cp, false); // use the connection profile, so no test
										refreshDocumentTable();
										refreshStatementTable(new int[0]);
										adjustToCoderSelection();

										// changes in other classes
										statusBar.updateUrl();
										toolbar.adjustToChangedConnection();
										textPanel.setContents(-1, "");
										statementPanel.adjustToChangedConnection();
										menuBar.adjustToChangedCoder();
										coderSelectionPanel.changeCoderBadge();
									} else {
										cp = null;
									}
								}

							}
							if (cp == null) {
								JOptionPane.showMessageDialog(MainWindow.this,
										"Database credentials could not be decrypted.\n"
												+ "Did you enter the right password?",
										"Check failed",
										JOptionPane.ERROR_MESSAGE);
							}
						} else {
							JOptionPane.showMessageDialog(MainWindow.this,
									"Password check failed. Zero-length passwords are not permitted.",
									"Check failed",
									JOptionPane.ERROR_MESSAGE);
						}
					}
				}
			} else {
				JOptionPane.showMessageDialog(MainWindow.this, "The file name you entered does not exist. Please choose a new file.");
			}

			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: opened connection profile.",
					"Opened a connection profile from the GUI.");
			Dna.logger.log(l);
		}
	}

	/**
	 * An action to display a file chooser and save a connection profile.
	 */
	class ActionSaveProfile extends AbstractAction {
		private static final long serialVersionUID = 6515595073332160633L;
		
		public ActionSaveProfile(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			boolean fileAlreadyExists = true;
			while (fileAlreadyExists) {
				FileChooser fc = new FileChooser(MainWindow.this, "Save profile", true, ".dnc", "DNA connection profile (*.dnc)", false);
				if (fc.getFiles() != null && fc.getFiles().length > 0 && fc.getFiles()[0] != null) {
					if (!fc.getFiles()[0].exists() || Dna.operatingSystem.toLowerCase().contains("mac")) {
						fileAlreadyExists = false;
						boolean validPasswordInput = false;
						while (!validPasswordInput) {
							CoderPasswordCheckDialog d = new CoderPasswordCheckDialog(MainWindow.this, Dna.sql, false, -1, 3);
							String key = d.getPassword();
							if (key == null) { // user must have pressed cancel
								validPasswordInput = true;
							} else {
								boolean authenticated = Dna.sql.authenticate(-1, key);
								if (authenticated) {
									// write the connection profile to disk, with an encrypted version of the password
									ConnectionProfile.writeConnectionProfile(fc.getFiles()[0].getPath(), new ConnectionProfile(Dna.sql.getConnectionProfile()), key);
									validPasswordInput = true; // quit the while-loop after successful export
									JOptionPane.showMessageDialog(MainWindow.this,
											"The profile was saved as:\n" + fc.getFiles()[0].getAbsolutePath(),
											"Success",
											JOptionPane.PLAIN_MESSAGE);
								} else {
									JOptionPane.showMessageDialog(MainWindow.this,
											"Coder password could not be verified. Try again.",
											"Check failed",
											JOptionPane.ERROR_MESSAGE);
								}
							}
						}
						LogEvent l = new LogEvent(Logger.MESSAGE,
								"[GUI] Action executed: saved connection profile.",
								"Saved a connection profile from the GUI.");
						Dna.logger.log(l);
					} else {
						JOptionPane.showMessageDialog(MainWindow.this, "The file name you entered already exists. Please choose a new file.");
					}
				} else {
					fileAlreadyExists = false; // leave the while loop if file chooser cancelled
				}
			}
		}
	}

	/**
	 * An action to open the regex editor.
	 */
	class ActionRegexEditor extends AbstractAction {
		private static final long serialVersionUID = -3249565772295389092L;

		public ActionRegexEditor(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: opened regex editor.",
					"Opened the regex editor from the DNA main window menu.");
			Dna.logger.log(l);
			RegexEditor re = new RegexEditor();
			if (re.isChanged()) {
				getTextPanel().paintStatements();
			}
			re.dispose();
		}
	}
	
	/**
	 * An action to display a coder manager dialog window.
	 */
	class ActionCoderManager extends AbstractAction {
		private static final long serialVersionUID = -771898426024889217L;

		public ActionCoderManager(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			if (Dna.sql.getActiveCoder().isPermissionEditCoders() == true) {
				CoderManager cm = new CoderManager(MainWindow.this);
				if (Dna.sql.getActiveCoder().getId() != 1) {
					Coder coderCopy = Dna.sql.getCoder(Dna.sql.getConnectionProfile().getCoderId());
					
					// refresh document and statement table and repaint text if necessary; reselect the coder
					boolean updateViewDocuments = false;
					if (coderCopy.differentViewDocumentPermissions(Dna.sql.getActiveCoder())) {
						updateViewDocuments = true;
					}
					boolean updateViewStatements = false;
					if (coderCopy.differentViewStatementPermissions(Dna.sql.getActiveCoder())) {
						updateViewStatements = true;
					}
					boolean updatePaintSettings = false;
					if (coderCopy.differentPaintSettings(Dna.sql.getActiveCoder())) {
						updatePaintSettings = true;
					}
					Dna.sql.selectCoder(coderCopy);
					if (updateViewDocuments) {
						documentTableModel.fireTableDataChanged();
					}
					if (updateViewStatements) {
						statementTableModel.fireTableDataChanged();
					}
					if (updatePaintSettings) {
						textPanel.adjustToChangedCoder();
					}
					
					// enable or disable actions as necessary after update
					if (Dna.sql.getActiveCoder().isPermissionEditCoders() == true) {
						actionCoderManager.setEnabled(true);
					} else {
						actionCoderManager.setEnabled(false);
					}
					changedDocumentTableSelection();
					if (Dna.sql.getActiveCoder().isPermissionDeleteStatements() == true) {
						actionRemoveStatements.setEnabled(true);
					} else {
						actionRemoveStatements.setEnabled(false);
					}
					if (Dna.sql.getActiveCoder().isPermissionEditStatementTypes() == true) {
						actionStatementTypeEditor.setEnabled(true);
					} else {
						actionStatementTypeEditor.setEnabled(false);
					}
					if (Dna.sql.getActiveCoder().isPermissionEditAttributes() == true) {
						actionAttributeManager.setEnabled(true);
					} else {
						actionAttributeManager.setEnabled(false);
					}
					if (Dna.sql.getActiveCoder().isPermissionEditCoderRelations() == true) {
						actionCoderRelationsEditor.setEnabled(true);
					} else {
						actionCoderRelationsEditor.setEnabled(false);
					}
				}
				
				if (cm.isDeletedCoder()) {
					refreshDocumentTable();
					refreshStatementTable(new int[0]);
				}
				cm.dispose();
				Dna.sql.selectCoder(Dna.sql.getActiveCoder().getId()); // refresh permissions in case they were updated
				
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: opened coder manager.",
						"Opened a coder manager window from the GUI.");
				Dna.logger.log(l);
			} else {
				LogEvent l = new LogEvent(Logger.WARNING,
						"[GUI] Action could not be executed: insufficient permissions to open coder manager.",
						"Attempted to open a coder manager from the GUI, but the coder did not have sufficient permissions for editing coders. This message should never appear because the menu item for opening a coder manager should be grayed out when the active coder has insufficient permissions. Please report the full error log to the developers through the issue tracker on GitHub.");
				Dna.logger.log(l);
			}
		}
	}

	/**
	 * An action to quit DNA.
	 */
	class ActionQuit extends AbstractAction {
		private static final long serialVersionUID = 3334696382161923841L;

		public ActionQuit(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: quit DNA.",
					"Quit DNA from the GUI.");
			Dna.logger.log(l);
			System.exit(0);
		}
	}
	
	/**
	 * An action to open a document editor dialog and add a new document.
	 */
	class ActionAddDocument extends AbstractAction {
		private static final long serialVersionUID = -3332492885668412485L;

		public ActionAddDocument(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			DocumentEditor de = new DocumentEditor();
			if (de.isChangesApplied()) {
				int[] documentIds = de.getDocumentIds();
				ArrayList<TableDocument> d = Dna.sql.getTableDocuments(documentIds);
				if (d.size() > 0) {
					int modelRow = documentTableModel.addRow(d.get(0));
					
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI] Action executed: added a new document.",
							"Added a new document from the GUI.");
					Dna.logger.log(l);
					
					// select new document in the table and open its text
					JTable dt = documentTablePanel.getDocumentTable();
					int viewRow = dt.convertRowIndexToView(modelRow);
					dt.setRowSelectionInterval(viewRow, viewRow);
					dt.scrollRectToVisible(new Rectangle(dt.getCellRect(viewRow, 0, true)));
				}
			}
		}
	}
	
	/**
	 * An action to remove the selected documents.
	 */
	class ActionRemoveDocuments extends AbstractAction {
		private static final long serialVersionUID = -5326085918049798694L;

		public ActionRemoveDocuments(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			// gather document data
			JTable dt = documentTablePanel.getDocumentTable();
			int[] viewRows = documentTablePanel.getSelectedRows();
			int[] modelRows = new int[viewRows.length];
			int[] documentIds = new int[viewRows.length];
			for (int i = 0; i < viewRows.length; i++) {
				modelRows[i] = dt.convertRowIndexToModel(viewRows[i]);
				documentIds[i] = documentTableModel.getRow(modelRows[i]).getId();
			}
			ArrayList<Integer> documentIdArrayList = new ArrayList<Integer>();
			for (int i = 0; i < documentIds.length; i++) {
				documentIdArrayList.add(documentIds[i]);
			}
			
			// confirmation dialog, then delete documents from database and table
			String message = "Are you sure you want to delete " + viewRows.length + " document(s) including all statements?";
			int dialog = JOptionPane.showConfirmDialog(MainWindow.this, message, "Confirmation required", JOptionPane.YES_NO_OPTION);
			if (dialog == 0) {
				// delete documents and everything dependent on it (e.g., statements) in database
				boolean deleted = Dna.sql.deleteDocuments(documentIds);
				if (deleted) {
					// if successful, delete statements from statement table in GUI
					statementTableModel.removeStatementsByDocuments(documentIdArrayList);
					
					// remove table documents from document table after unselecting them
					dt.clearSelection();
					documentTableModel.removeDocuments(modelRows);
					
					// log document removal
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI] Action executed: removed document(s).",
							"Deleted one or more documents in the database from the GUI.");
					Dna.logger.log(l);
				}
			}
		}
	}
	
	/**
	 * An action to edit the selected document(s).
	 */
	class ActionEditDocuments extends AbstractAction {
		private static final long serialVersionUID = 4411902324874635324L;

		public ActionEditDocuments(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			JTable t = documentTablePanel.getDocumentTable();
			int[] selectedRows = t.getSelectedRows();
			int[] modelRows = new int[selectedRows.length];
			int[] selectedDocumentIds = new int[selectedRows.length];
			for (int i = 0; i < selectedRows.length; i++) {
				modelRows[i] = t.convertRowIndexToModel(selectedRows[i]);
				selectedDocumentIds[i] = documentTableModel.getIdByModelRow(modelRows[i]);
			}
			DocumentEditor de = new DocumentEditor(selectedDocumentIds);
			if (de.isChangesApplied()) {
				// update current document document text and repaint
				if (selectedRows.length == 1) {
					String newDocumentText = Dna.sql.getDocumentText(selectedDocumentIds[0]);
					if (!newDocumentText.equals(textPanel.getTextWindow().getText())) {
						textPanel.setContents(selectedDocumentIds[0], newDocumentText);
					}
				}
				
				// update changed table cells
				ArrayList<TableDocument> updatedDocuments = Dna.sql.getTableDocuments(selectedDocumentIds);
				for (int i = 0; i < updatedDocuments.size(); i++) {
					int modelRow = documentTableModel.getModelRowById(updatedDocuments.get(i).getId());
					int viewRow = t.convertRowIndexToView(modelRow);
					if (!documentTableModel.getRow(modelRow).getTitle().equals(updatedDocuments.get(i).getTitle())) {
						documentTableModel.getRow(modelRow).setTitle(updatedDocuments.get(i).getTitle());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(1));
					}
					if (documentTableModel.getRow(modelRow).getFrequency() != updatedDocuments.get(i).getFrequency()) {
						documentTableModel.getRow(modelRow).setFrequency(updatedDocuments.get(i).getFrequency());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(2));
					}
					if (!documentTableModel.getRow(modelRow).getDateTime().equals(updatedDocuments.get(i).getDateTime())) {
						documentTableModel.getRow(modelRow).setDateTime(updatedDocuments.get(i).getDateTime());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(3));
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(4));
					}
					if (documentTableModel.getRow(modelRow).getCoder().getId() != updatedDocuments.get(i).getCoder().getId()) {
						documentTableModel.getRow(modelRow).setCoder(updatedDocuments.get(i).getCoder());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(5));
					}
					if (!documentTableModel.getRow(modelRow).getAuthor().equals(updatedDocuments.get(i).getAuthor())) {
						documentTableModel.getRow(modelRow).setAuthor(updatedDocuments.get(i).getAuthor());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(6));
					}
					if (!documentTableModel.getRow(modelRow).getSource().equals(updatedDocuments.get(i).getSource())) {
						documentTableModel.getRow(modelRow).setSource(updatedDocuments.get(i).getSource());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(7));
					}
					if (!documentTableModel.getRow(modelRow).getSection().equals(updatedDocuments.get(i).getSection())) {
						documentTableModel.getRow(modelRow).setSection(updatedDocuments.get(i).getSection());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(8));
					}
					if (!documentTableModel.getRow(modelRow).getType().equals(updatedDocuments.get(i).getType())) {
						documentTableModel.getRow(modelRow).setType(updatedDocuments.get(i).getType());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(9));
					}
					if (!documentTableModel.getRow(modelRow).getNotes().equals(updatedDocuments.get(i).getNotes())) {
						documentTableModel.getRow(modelRow).setNotes(updatedDocuments.get(i).getNotes());
						documentTableModel.fireTableCellUpdated(viewRow, t.convertColumnIndexToView(10));
					}
				}
				
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: edited meta-data for document(s).",
						"Edited the meta-data for one or more documents in the database and updated document view in GUI.");
				Dna.logger.log(l);
			}
		}
	}
	
	/**
	 * An action to start a dialog for batch-importing documents from a folder.
	 */
	class ActionBatchImportDocuments extends AbstractAction {
		private static final long serialVersionUID = -1460878736275897716L;
		
		public ActionBatchImportDocuments(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			new DocumentBatchImporter();
	    	refreshDocumentTable();
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: used document batch importer.",
					"Batch-imported documents to the database.");
			Dna.logger.log(l);
		}
	}

	/**
	 * An action to start a dialog for importing from another DNA database.
	 */
	class ActionImporter extends AbstractAction {
		private static final long serialVersionUID = 4613926523251135254L;

		public ActionImporter(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			new Importer(MainWindow.this);
	    	refreshDocumentTable();
			refreshStatementTable(new int[0]);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: used DNA database import dialog.",
					"Imported from another database into the current DNA database.");
			Dna.logger.log(l);
		}
	}

	/**
	 * An action to start a regex search dialog.
	 */
	class ActionSearchDialog extends AbstractAction {
		private static final long serialVersionUID = 3191345857213145306L;

		public ActionSearchDialog(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			// create search dialog
			SearchDialog sd = new SearchDialog(null, documentTablePanel.getSelectedDocumentIds()); // would normally be MainWindow.this, but then the main window can't be accessed while the dialog is open
			documentTablePanel.addListener(sd);

			// add selection listener to table to select documents and highlight text in the main window
			JTable searchTable = sd.getSearchTable();
			searchTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			    @Override
			    public void valueChanged(ListSelectionEvent e) {
			    	if (!e.getValueIsAdjusting()) {
						int viewRow = searchTable.getSelectedRow();
						if (viewRow > -1) {
							int documentId = (int) searchTable.getValueAt(viewRow, 0);
							int start = (int) searchTable.getValueAt(viewRow, 3);
							int stop = (int) searchTable.getValueAt(viewRow, 4);
							
							documentTablePanel.setSelectedDocumentId(documentId);
							textPanel.getTextWindow().grabFocus();
							textPanel.getTextWindow().select(start, stop);
						}
					}
			    }
			});
			sd.setVisible(true); // make visible here (i.e., only after adding selection listener to make it work)
		}
	}
	
	/**
	 * An action to reload the documents and statements from the database.
	 */
	class ActionRefresh extends AbstractAction {
		private static final long serialVersionUID = -5684628034931158710L;

		public ActionRefresh(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
	    	refreshDocumentTable();
			refreshStatementTable(new int[0]);
		}
	}

	/**
	 * An action to recode the selected statements.
	 */
	class ActionRecodeStatements extends AbstractAction {
		private static final long serialVersionUID = 3844766484800761404L;

		public ActionRecodeStatements(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			// gather data: row view indices, row model indices, statement IDs, statement type, coder IDs, and permission
			JTable statementTable = getStatementPanel().getStatementTable();
			int[] selectedRows = statementTable.getSelectedRows();
			int[] modelRows = new int[selectedRows.length];
			int[] statementIds = new int[selectedRows.length];
			int[] coderIds = new int[selectedRows.length];
			boolean permissions = Dna.sql.getActiveCoder().isPermissionEditStatements();
			int statementTypeId = statementTableModel.getRow(statementTable.convertRowIndexToModel(selectedRows[0])).getStatementTypeId();
			for (int i = 0; i < selectedRows.length; i++) {
				modelRows[i] = statementTable.convertRowIndexToModel(selectedRows[i]);
				statementIds[i] = statementTableModel.getRow(modelRows[i]).getId();
				coderIds[i] = statementTableModel.getRow(modelRows[i]).getCoderId();
				if (!Dna.sql.getActiveCoder().isPermissionEditOthersStatements(coderIds[i]) && Dna.sql.getActiveCoder().getId() != coderIds[i]) {
					permissions = false;
				}
				if (!Dna.sql.getActiveCoder().isPermissionEditOthersStatements() && Dna.sql.getActiveCoder().getId() != coderIds[i]) {
					permissions = false;
				}
				if (statementTypeId != statementTableModel.getRow(modelRows[i]).getStatementTypeId()) {
					permissions = false;
				}
			}
			
			// get statement type and initialize statement recoder dialog
			if (permissions) {
				StatementType statementType = Dna.sql.getStatementTypes()
						.stream()
						.filter(s -> s.getId() == statementTypeId)
						.findFirst()
						.orElse(null);
				StatementRecoder sr = new StatementRecoder(MainWindow.this, statementIds, statementType);
				if (sr.isChangesApplied()) {
					ArrayList<Statement> changedStatements = sr.getChangedStatements();
					int[] changedIds = changedStatements
							.stream()
							.map(s -> s.getId())
							.mapToInt(x -> x)
							.toArray();
					refreshStatementTable(changedIds);
				}
			}
		}
	}

	/**
	 * An action to remove the selected statements from the database.
	 */
	class ActionRemoveStatements extends AbstractAction {
		private static final long serialVersionUID = -4938838325040431112L;

		public ActionRemoveStatements(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			// gather data: row view indices, row model indices, and statement IDs
			JTable statementTable = getStatementPanel().getStatementTable();
			int[] selectedRows = statementTable.getSelectedRows();
			int[] modelRows = new int[selectedRows.length];
			int[] statementIds = new int[selectedRows.length];
			int[] documentIds = new int[selectedRows.length];
			for (int i = 0; i < selectedRows.length; i++) {
				modelRows[i] = statementTable.convertRowIndexToModel(selectedRows[i]);
				statementIds[i] = statementTableModel.getRow(modelRows[i]).getId();
				documentIds[i] = statementTableModel.getRow(modelRows[i]).getDocumentId();
			}
			
			// confirmation dialog, then delete statements from database and table
			String message = "Are you sure you want to delete " + selectedRows.length + " statements?";
			int dialog = JOptionPane.showConfirmDialog(MainWindow.this, message, "Confirmation required", JOptionPane.YES_NO_OPTION);
			if (dialog == 0) {
				statusBar.statementRefreshStart();
				boolean deleted = Dna.sql.deleteStatements(statementIds);
				if (deleted) {
					getTextPanel().paintStatements();
					statementTable.clearSelection();
					statementTableModel.removeStatements(modelRows);
					for (int i = 0; i < documentIds.length; i++) {
						documentTableModel.decreaseFrequency(documentIds[i]);
					}
					
					// log deleted statements
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI] Action executed: removed statement(s).",
							"Deleted statement(s) in the database and GUI.");
					Dna.logger.log(l);
				}
				statusBar.statementRefreshEnd();
			}
		}
	}

	/**
	 * An action to display a statement type editor dialog window.
	 */
	class ActionStatementTypeEditor extends AbstractAction {
		private static final long serialVersionUID = -9078666078201409563L;
		
		public ActionStatementTypeEditor(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			if (Dna.sql.getActiveCoder().isPermissionEditStatementTypes()) {
				new StatementTypeEditor(MainWindow.this);
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: opened statement type editor.",
						"Opened a statement type editor window from the GUI.");
				Dna.logger.log(l);
			} else {
				LogEvent l = new LogEvent(Logger.WARNING,
						"[GUI] Action could not be executed: insufficient permissions to open statement type editor.",
						"Attempted to open a statement type editor from the GUI, but the coder did not have sufficient permissions for editing statement types. This message should never appear because the menu item for opening a statement type editor dialog window should be grayed out when the active coder has insufficient permissions. Please report the full error log to the developers through the issue tracker on GitHub.");
				Dna.logger.log(l);
			}
		}
	}

	/**
	 * An action to display an attribute manager dialog window.
	 */
	class ActionAttributeManager extends AbstractAction {
		private static final long serialVersionUID = -9078666078201409563L;
		
		public ActionAttributeManager(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			if (Dna.sql.getActiveCoder().isPermissionEditAttributes() == true) {
				new AttributeManager(MainWindow.this);
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: opened attribute manager.",
						"Opened an attribute manager window from the GUI.");
				Dna.logger.log(l);
			} else {
				LogEvent l = new LogEvent(Logger.WARNING,
						"[GUI] Action could not be executed: insufficient permissions to open attribute manager.",
						"Attempted to open an attribute manager from the GUI, but the coder did not have sufficient permissions for editing attributes. This message should never appear because the menu item for opening an attribute manager should be grayed out when the active coder has insufficient permissions. Please report the full error log to the developers through the issue tracker on GitHub.");
				Dna.logger.log(l);
			}
		}
	}

	/**
	 * An action to display a network exporter dialog window.
	 */
	class ActionNetworkExporter extends AbstractAction {
		private static final long serialVersionUID = -19664358765966754L;

		public ActionNetworkExporter(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			new NetworkExporter(MainWindow.this);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: opened network export dialog window.",
					"Opened a network export dialog window from the GUI.");
			Dna.logger.log(l);
		}
	}

	/**
	 * An action to display a backbone exporter dialog window.
	 */
	class ActionBackboneExporter extends AbstractAction {
		private static final long serialVersionUID = -19624358765966754L;

		public ActionBackboneExporter(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}

		public void actionPerformed(ActionEvent e) {
			new BackboneExporter(MainWindow.this);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: opened backbone dialog window.",
					"Opened a backbone dialog window from the GUI.");
			Dna.logger.log(l);
		}
	}

	/**
	 * An action to display a coder manager dialog window.
	 */
	class ActionCoderRelationsEditor extends AbstractAction {
		private static final long serialVersionUID = -6341308994517637774L;

		public ActionCoderRelationsEditor(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			if (Dna.sql.getActiveCoder().isPermissionEditCoderRelations() && Dna.sql.getActiveCoder().getId() != 1) {
				CoderRelationsEditor cre = new CoderRelationsEditor();
				if (cre.isUpdateViewDocuments()) {
					documentTableModel.fireTableDataChanged();
				}
				if (cre.isUpdateViewStatements()) {
					statementTableModel.fireTableDataChanged();
				}
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"[GUI] Action executed: opened coder relations editor.",
						"Opened a coder relations editor window from the GUI.");
				Dna.logger.log(l);
			} else {
				LogEvent l = new LogEvent(Logger.WARNING,
						"[GUI] Action could not be executed: insufficient permissions to open coder relations editor.",
						"Attempted to open a coder relations editor from the GUI, but the coder did not have sufficient permissions for editing coder relations. This message should never appear because the menu item for opening a coder relations editor should be grayed out when the active coder has insufficient permissions. Please report the full error log to the developers through the issue tracker on GitHub.");
				Dna.logger.log(l);
			}
		}
	}
	
	/**
	 * An action to open a new logger dialog window.
	 */
	class ActionLoggerDialog extends AbstractAction {
		private static final long serialVersionUID = -629086240908166990L;

		public ActionLoggerDialog(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			new LoggerDialog();
		}
	}
	
	/**
	 * An action to display a new About DNA window.
	 */
	class ActionAboutWindow extends AbstractAction {
		private static final long serialVersionUID = -9078666078201409563L;
		
		public ActionAboutWindow(String text, ImageIcon icon, String desc, Integer mnemonic) {
			super(text, icon);
			putValue(SHORT_DESCRIPTION, desc);
			putValue(MNEMONIC_KEY, mnemonic);
		}
		
		public void actionPerformed(ActionEvent e) {
			new AboutWindow(Dna.version, Dna.date);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"[GUI] Action executed: opened About DNA window.",
					"Opened an About DNA window from the GUI.");
			Dna.logger.log(l);
		}
	}
}
