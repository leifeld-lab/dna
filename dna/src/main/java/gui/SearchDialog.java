package gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.swing.*;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableRowSorter;

import org.jdesktop.swingx.JXTextField;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;

/**
 * Search dialog for performing a regex search on all document texts.
 */
public class SearchDialog extends JDialog implements DocumentTablePanel.DocumentTableListener {
	private static final long serialVersionUID = -8328539384212006063L;
	private SearchSwingWorker searchSwingWorker;
	private JProgressBar progressBar;
	private JXTextField textField;
	private KeyAdapter enter;
	private JButton searchButton, revertButton, cancelButton;
	private JCheckBox documentCheckBox;
	private SearchTableModel searchTableModel;
	private JTable searchTable;
	private JScrollPane searchTableScrollPane;
	private JLabel statusLabel;
	private static final int H = 20;
	private int[] selectedDocumentIds;
	
	/**
	 * Constructor to create a new search dialog.
	 * 
	 * @param parent The parent frame.
	 */
	public SearchDialog(Frame parent, int[] selectedDocumentIds) {
		super(parent, "SearchDialog", true);
		this.selectedDocumentIds = selectedDocumentIds;
		this.setTitle("Regex search across document texts");
		ImageIcon statementIcon = new ImageIcon(getClass().getResource("/icons/tabler-icon-search.png"));
		this.setIconImage(statementIcon.getImage());
		this.setModal(false); // make sure the main window can be accessed
		this.setModalityType(ModalityType.DOCUMENT_MODAL); // make sure the main window can be accessed
		this.setAlwaysOnTop(true);
		
		// terminate search swing worker before disposing
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				if (searchSwingWorker != null) {
					searchSwingWorker.cancel(true);
				}
				dispose();
			}
		});
		
		// outer panel with border
		JPanel panel = new JPanel(new BorderLayout());
		CompoundBorder border = BorderFactory.createCompoundBorder(new EmptyBorder(10, 10, 10, 10), new TitledBorder("Regex search"));
		panel.setBorder(border);
		this.add(panel);
		
		// top right panel with cancel button and progress bar
		JPanel controlPanel = new JPanel(new BorderLayout());
		JPanel progressPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		
		ImageIcon cancelIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-x.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		cancelButton = new JButton("cancel", cancelIcon);
		cancelButton.setPreferredSize(new Dimension(cancelButton.getPreferredSize().width, H));
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (searchSwingWorker != null) {
					searchSwingWorker.cancel(true);
				}
			}
		});
		progressPanel.add(cancelButton);
		
		progressBar = new JProgressBar();
		progressBar.setIndeterminate(true);
		progressBar.setToolTipText("Searching...");
		progressBar.setString("Searching...");
		progressBar.setPreferredSize(new Dimension(progressBar.getPreferredSize().width, H));
		progressPanel.add(progressBar);
		controlPanel.add(progressPanel, BorderLayout.EAST);
		
		// top left panel with search field and buttons
		JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
		textField = new JXTextField("Regular expression search");
		textField.setColumns(30);
		textField.setPreferredSize(new Dimension(textField.getPreferredSize().width, H + 2));
		textField.setToolTipText("<html>More information: <a href=\"http://www.regular-expressions.info\">http://www.regular-expressions.info</a></html>");
		enter = new KeyAdapter() {
			public void keyPressed(KeyEvent e) {
				int key = e.getKeyCode();
				if (key == KeyEvent.VK_ENTER) {
					startSearchSwingWorker();
				}
			}
		};
		textField.addKeyListener(enter);
		textField.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void changedUpdate(DocumentEvent e) {
				toggleButtons();
			}

			@Override
			public void insertUpdate(DocumentEvent e) {
				toggleButtons();
			}

			@Override
			public void removeUpdate(DocumentEvent e) {
				toggleButtons();
			}
			
			private void toggleButtons() {
				if (textField.getText().equals("")) {
					searchButton.setEnabled(false);
				} else {
					searchButton.setEnabled(true);
				}
				if (textField.getText().equals("") && searchTable.getRowCount() == 0) {
					revertButton.setEnabled(false);
				} else {
					revertButton.setEnabled(true);
				}
			}
		});
		buttonPanel.add(textField);
		
		ImageIcon searchIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-search.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		searchButton = new JButton("Search", searchIcon);
		searchButton.setToolTipText("<html>Do a full-text search and find all occurrences of <br> the regular expression given in the text field.</html>");
		searchButton.setPreferredSize(new Dimension(searchButton.getPreferredSize().width, H));
		searchButton.setEnabled(false);
		searchButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				startSearchSwingWorker();
			}
		});
		buttonPanel.add(searchButton);
		
		ImageIcon revertIcon = new ImageIcon(new ImageIcon(getClass().getResource("/icons/tabler-icon-backspace.png")).getImage().getScaledInstance(18, 18, Image.SCALE_SMOOTH));
		revertButton = new JButton("Revert", revertIcon);
		revertButton.setToolTipText("Reset search results and text field.");
		revertButton.setPreferredSize(new Dimension(revertButton.getPreferredSize().width, H));
		revertButton.setEnabled(false);
		revertButton.addActionListener( new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (searchSwingWorker != null) {
					searchSwingWorker.cancel(true);
				}
				searchTableModel.clear();
				textField.setEnabled(true);
				textField.setText("");
				statusLabel.setText("");
				documentCheckBox.setSelected(false);
			}
		});
		buttonPanel.add(revertButton);

		documentCheckBox = new JCheckBox("Only in document(s) selected in main window");
		documentCheckBox.setToolTipText("Constrain the search to documents currently selected in the document table of the main window.");
		documentCheckBox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent actionEvent) {
				SearchDialog.this.searchTableModel.fireTableDataChanged();
				statusLabel.setText(searchTable.getRowCount() + " regex matches found.");
			}
		});
		buttonPanel.add(documentCheckBox);
		
		controlPanel.add(buttonPanel, BorderLayout.WEST);
		panel.add(controlPanel, BorderLayout.NORTH);

		// search result table
		searchTableModel = new SearchTableModel();
		searchTable = new JTable(searchTableModel);
		searchTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		searchTableScrollPane = new JScrollPane(searchTable);
		searchTableScrollPane.setPreferredSize(new Dimension(900, 300));
		searchTable.getColumnModel().getColumn(0).setPreferredWidth(40);
		searchTable.getColumnModel().getColumn(1).setPreferredWidth(130);
		searchTable.getColumnModel().getColumn(2).setPreferredWidth(250);
		searchTable.getColumnModel().getColumn(3).setPreferredWidth(40);
		searchTable.getColumnModel().getColumn(4).setPreferredWidth(40);
		searchTable.getColumnModel().getColumn(5).setPreferredWidth(400);
		SearchTableCellRenderer searchTableCellRenderer = new SearchTableCellRenderer();
		searchTable.setDefaultRenderer(String.class, searchTableCellRenderer);
		searchTable.setDefaultRenderer(int.class, searchTableCellRenderer);
		searchTable.setDefaultRenderer(LocalDateTime.class, searchTableCellRenderer);
		searchTable.getTableHeader().setReorderingAllowed(false);
		panel.add(searchTableScrollPane, BorderLayout.CENTER);

		// row filter
		TableRowSorter<SearchTableModel> sorter = new TableRowSorter<SearchTableModel>(searchTableModel);
		searchTable.setRowSorter(sorter);
		RowFilter<SearchTableModel, Integer> documentFilter = new RowFilter<SearchTableModel, Integer>() {
			public boolean include(Entry<? extends SearchTableModel, ? extends Integer> entry) {
				if (!SearchDialog.this.documentCheckBox.isSelected()) return true;
				if (SearchDialog.this.selectedDocumentIds.length == 0) return false;
				int documentId = (int) searchTableModel.getValueAt(entry.getIdentifier(), 0);
				if (IntStream.of(SearchDialog.this.selectedDocumentIds).anyMatch(i -> i == documentId)) {
					return true;
				} else {
					return false;
				}
			}
		};
		sorter.setRowFilter(documentFilter);

		// status message
		statusLabel = new JLabel("");
		panel.add(statusLabel, BorderLayout.SOUTH);
		
		this.pack();
		cancelButton.setVisible(false); // make button and progress bar invisible after packing layout to ensure right size
		progressBar.setVisible(false);
		this.setLocationRelativeTo(null);
		this.setVisible(false); // initially false because list selection listener first needs to be added in main window, then the dialog is set visible
		
		textField.grabFocus();
	}
	
	/**
	 * Return a reference to the search table, for example to add action
	 * listeners in the main window.
	 * 
	 * @return The search table.
	 */
	JTable getSearchTable() {
		return searchTable;
	}
	
	/**
	 * Cancel an old search swing worker if present and start a new worker.
	 */
	private void startSearchSwingWorker() {
		if (searchSwingWorker != null) {
			searchSwingWorker.cancel(true);
			searchSwingWorker.done();
		}
		searchSwingWorker = new SearchSwingWorker(LocalTime.now().toString(),
				textField.getText(),
				progressBar,
				cancelButton,
				searchButton,
				revertButton);
		searchSwingWorker.execute();
	}

	@Override
	public void documentSelected(int[] documentIds) {
		this.selectedDocumentIds = documentIds;
		this.searchTableModel.fireTableDataChanged();
		statusLabel.setText(searchTable.getRowCount() + " regex matches found.");
	}

	/**
	 * Swing worker class for loading documents containing search results (after
	 * pre-filtering) from the database and creating search results.
	 */
	private class SearchSwingWorker extends SwingWorker<List<SearchResult>, SearchResult> {
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
		 * The regex pattern for the search.
		 */
		private String regex;

		/**
		 * Create a new document table swing worker.
		 * 
		 * @param name  The name of the thread.
		 */
		private SearchSwingWorker(String name,
				String regex,
				JProgressBar progressBar,
				JButton cancelButton,
				JButton searchButton,
				JButton revertButton) {
			this.name = name;
			this.regex = regex;
			time = System.nanoTime(); // take the time to compute later how long the updating took
			progressBar.setVisible(true);
	    	cancelButton.setVisible(true);
	    	searchButton.setEnabled(false);
	    	revertButton.setEnabled(false);
	    	textField.setEnabled(false);
			searchTableModel.clear(); // remove all search results from the table model before re-populating the table
			LogEvent le = new LogEvent(Logger.MESSAGE,
					"[GUI] Initializing thread to generate search results: " + this.getName() + ".",
					"A new swing worker thread has been started to generate regex search results in the background: " + this.getName() + ".");
			Dna.logger.log(le);
		}
		
		@Override
		protected List<SearchResult> doInBackground() {
			// get comma-separate string of IDs of coders with the permission to view their documents
			String ids = Dna.sql.getCoders()
					.stream()
					.map(c -> c.getId())
					.mapToInt(c -> c)
					.filter(c -> c == Dna.sql.getActiveCoder().getId() || (Dna.sql.getActiveCoder().isPermissionViewOthersDocuments() && Dna.sql.getActiveCoder().isPermissionViewOthersDocuments(c)))
					.mapToObj(i -> ((Integer) i).toString()) // i is an int, not an Integer
					.collect(Collectors.joining(", "))
					.toString();
			
			try (Connection conn = Dna.sql.getDataSource().getConnection();
					PreparedStatement s = conn.prepareStatement("SELECT ID, Date, Title, Text FROM DOCUMENTS WHERE Text LIKE '%" + regex + "%' AND CODER IN (" + ids + ");");
					ResultSet rs = s.executeQuery();) {
				String text, title, match;
				int documentId, start, stop;
				LocalDateTime date;
				while (!isCancelled() && rs.next()) {
					if (isCancelled()) {
						return null;
					}
					title = rs.getString("Title");
					text = rs.getString("Text");
					documentId = rs.getInt("ID");
					date = LocalDateTime.ofEpochSecond(rs.getLong("Date"), 0, ZoneOffset.UTC);
					
			        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
			        Matcher matcher = pattern.matcher(text);
			        while (matcher.find()) {
			        	start = matcher.start();
			        	stop = matcher.end();
			        	int a = start - 30; // include 30 characters before match in table
			        	if (a < 0) {
			        		a = 0;
			        	}
			        	int b = stop + 30; // include 30 characters after match in table
			        	if (b > text.length() - 1) {
			        		b = text.length() - 1;
			        	}
			        	match = text.substring(a, b);
			        	publish(new SearchResult(documentId, title, date, start, stop, match));
			        }
				}
			} catch (SQLException e) {
				if (e.getMessage().matches(".*Interrupted during connection acquisition.*")) {
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"[GUI]  ├─ Retrieval of search results canceled in Thread " + this.getName() + ".",
							"Retrieving search results from the database was canceled, presumably because a new swing worker to retrieve new search results was initiated, which then superseded the existing thread.",
							e);
					Dna.logger.log(l);
				} else {
					LogEvent le = new LogEvent(Logger.WARNING,
							"[SQL]  ├─ Could not retrieve search results from database.",
							"The swing worker for generating search results in the background tried to retrieve all search results from the database to display them in the search result table, but some or all search results could not be retrieved because there was a problem while processing the result set. The search result table may be incomplete.",
							e);
					Dna.logger.log(le);
				}
			}
			return null;
		}
	    
	    @Override
	    protected void process(List<SearchResult> chunks) {
	    	searchTableModel.addRows(chunks); // transfer a batch of rows to the table model
	    }

	    @Override
	    protected void done() {
	    	progressBar.setVisible(false);
	    	cancelButton.setVisible(false);
	    	revertButton.setEnabled(true);
	    	searchButton.setEnabled(true);
	    	textField.setEnabled(true);
	    	statusLabel.setText(searchTable.getRowCount() + " regex matches found.");
			long elapsed = System.nanoTime(); // measure time again for calculating difference
			LogEvent le = new LogEvent(Logger.MESSAGE,
					"[GUI]  ├─ Retrieved all " + searchTableModel.getRowCount() + " search results in " + (elapsed - time) / 1000000 + " milliseconds.",
					"The swing worker retrieved the " + searchTableModel.getRowCount() + " search results from the DNA database in the "
					+ "background and stored them in the search result table. This took " + (elapsed - time) / 1000000 + " milliseconds.");
			Dna.logger.log(le);
			le = new LogEvent(Logger.MESSAGE,
					"[GUI]  └─ Closing thread to retrieve search results: " + this.getName() + ".",
					"The search result table has been populated with search results from the database. Closing thread: " + this.getName() + ".");
			Dna.logger.log(le);
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
	 * Table model for holding and managing search results.
	 */
	private class SearchTableModel extends AbstractTableModel {
		private static final long serialVersionUID = -5996730652945706619L;
		private ArrayList<SearchResult> rows = new ArrayList<SearchResult>();

		@Override
		public int getColumnCount() {
			return 6;
		}

		@Override
		public int getRowCount() {
			return rows.size();
		}

		@Override
		public Object getValueAt(int row, int column) {
			if (column == 0) {
				return rows.get(row).getDocumentId();
			} else if (column == 1) {
				return rows.get(row).getDateTime();
			} else if (column == 2) {
				return rows.get(row).getTitle();
			} else if (column == 3) {
				return rows.get(row).getStart();
			} else if (column == 4) {
				return rows.get(row).getStop();
			} else if (column == 5) {
				return rows.get(row).getDisplayText();
			}
			return null;
		}
		
		/**
		 * Get the name of a column
		 */
		public String getColumnName(int column) {
			switch (column) {
				case 0: return "ID";
				case 1: return "Date/time";
				case 2: return "Document title";
				case 3: return "Start";
				case 4: return "End";
				case 5: return "Match";
				default: return null;
			}
		}
		
		/**
		 * Get the class of a column.
		 */
		public Class<?> getColumnClass(int column) {
			switch (column) {
				case 0: return int.class;            // document ID
				case 1: return LocalDateTime.class;  // date/time
				case 2: return String.class;         // document title
				case 3: return int.class;           // start
				case 4: return int.class;           // stop
				case 5: return String.class;         // display text (match)
				default: return null;
			}
		}
		
		/**
		 * Is the cell editable?
		 */
		public boolean isCellEditable(int row, int column) {
			return false;
		}

		/**
		 * Add a list of {@link SearchResult} objects to the table model and
		 * notify the listeners.
		 * 
		 * @param chunks A list of {@link SearchResult} objects.
		 */
		void addRows(List<SearchResult> chunks) {
	    	int n = this.rows.size();
	        for (SearchResult row : chunks) {
	            rows.add(row);
	        }
	        fireTableRowsInserted(n, n + chunks.size() - 1); // subtract one because we don't need the cursor to be at the next position; it should refer to the last position
		}
		
		/**
		 * Clear the table and notify listeners.
		 */
		public void clear() {
			rows.clear();
			this.fireTableDataChanged();
		}
	}
	
	/**
	 * Search result class. Holds one row of the results table, including some
	 * document information, the start and stop coordinates of the match, and
	 * the text in the document matching the regex, plus a few words before and
	 * after the match.
	 */
	private class SearchResult implements Comparable<SearchResult> {
		private int documentId, start, stop;
		private String title, displayText;
		private LocalDateTime dateTime;

		/**
		 * Constructor to create a new search result.
		 * 
		 * @param documentId    The document ID.
		 * @param title         The document title.
		 * @param dateTime      The date and time of the document.
		 * @param start         The start coordinate of the match in the text.
		 * @param stop          The end coordinate of the match in the text.
		 * @param displayText   The text in the document matching the regex.
		 */
		public SearchResult(int documentId, String title, LocalDateTime dateTime, int start, int stop, String displayText) {
			this.documentId = documentId;
			this.title = title;
			this.dateTime = dateTime;
			this.start = start;
			this.stop = stop;
			this.displayText = displayText;
		}

		/**
		 * Get the document ID.
		 * 
		 * @return The document ID.
		 */
		int getDocumentId() {
			return documentId;
		}

		/**
		 * Get the start position in the document text.
		 * 
		 * @return The start position.
		 */
		int getStart() {
			return start;
		}

		/**
		 * Get the end position in the document text.
		 * 
		 * @return The end position.
		 */
		int getStop() {
			return stop;
		}

		/**
		 * Get the document title.
		 * 
		 * @return The title.
		 */
		String getTitle() {
			return title;
		}

		/**
		 * Get the display text, composed of the match +/- 30 characters.
		 * 
		 * @return The display text.
		 */
		String getDisplayText() {
			return displayText;
		}

		/**
		 * Get the date/time of the document.
		 * 
		 * @return The local date/time.
		 */
		LocalDateTime getDateTime() {
			return dateTime;
		}

		/**
		 * Implementation of the {@link java.lang.Comparable Comparable}
		 * interface to sort search results in the document table and possibly
		 * elsewhere.
		 */
		@Override
		public int compareTo(SearchResult s) {
			if (this.dateTime != null) {
				if (this.getDateTime().isBefore(s.getDateTime())) {
					return -1;
				} else if (this.getDateTime().isAfter(s.getDateTime())) {
					return 1;
				}
			}
			if (this.getDocumentId() < s.getDocumentId()) {
				return -1;
			} else {
				return 1;
			}
		}
	}

	/**
	 * Table cell renderer for the search table.
	 */
	private class SearchTableCellRenderer extends DefaultTableCellRenderer {
		private static final long serialVersionUID = -6413362632328288320L;

		@Override
		public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
			Class<?> colClass = searchTableModel.getColumnClass(column);
			DefaultTableCellRenderer renderer = (DefaultTableCellRenderer) super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (value == null) {
				return new JLabel("");
            } else if (colClass.equals(int.class)) {
            	renderer.setHorizontalAlignment(JLabel.RIGHT);
            	renderer.setText(String.valueOf((int) value));
            } else if (searchTableModel.getColumnName(column).equals("Match")) {
            	renderer.setHorizontalAlignment(JLabel.CENTER); // align matches at the center of the column
            	renderer.setText((String) value);
            } else if (colClass.equals(String.class)) {
            	renderer.setHorizontalAlignment(JLabel.LEFT);
            	renderer.setText((String) value);
            } else if (colClass.equals(LocalDateTime.class)) {
            	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MM yyyy HH:mm:ss");
				renderer.setText(((LocalDateTime) value).format(formatter));
            }
            return renderer;
		}
	}
}