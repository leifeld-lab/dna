package dna;

import java.io.File;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import me.tongfei.progressbar.ProgressBar;
import model.Value;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import org.rosuda.JRI.RConsoleOutputStream;
import org.rosuda.JRI.Rengine;

import dna.export.*;
import logger.LogEvent;
import logger.Logger;
import model.Coder;
import model.Statement;
import model.StatementType;
import sql.ConnectionProfile;
import sql.Sql;

/**
 * A class with public functions for accessing DNA without a GUI.
 */
public class HeadlessDna implements Logger.LogListener {
	private Exporter exporter;

	/**
	 * Constructor for creating an instance of the headless DNA class.
	 */
	public HeadlessDna() {
		// divert stdout to R console
		Rengine r = new Rengine();
		RConsoleOutputStream rsRegular = new RConsoleOutputStream(r, 0);
		System.setOut(new PrintStream(rsRegular));
		RConsoleOutputStream rsWarningError = new RConsoleOutputStream(r, 1);
		System.setErr(new PrintStream(rsWarningError));
	}

	/**
	 * A function for generating a brief report with details about the database.
	 */
	public void printDatabaseDetails() {
		String s = "";
		if (Dna.sql == null || Dna.sql.getConnectionProfile() == null || Dna.sql.getActiveCoder() == null) {
			s = "No active database.";
		} else {
			int documents = Dna.sql.countDocuments();
			int statements = Dna.sql.countStatements(-1);
			int[] coderItems = Dna.sql.countCoderItems(Dna.sql.getActiveCoder().getId());
			ArrayList<StatementType> statementTypes = Dna.sql.getStatementTypes();
			String statementString = " statements in ";
			if (statements == 1) {
				statementString = " statement in ";
			}
			String documentString = " documents.";
			if (documents == 1) {
				documentString = " document";
			}
			s = "DNA database: " + Dna.sql.getConnectionProfile().getUrl() + "\n" + statements + statementString + documents + documentString + "\nStatement types: ";
			for (int i = 0; i < statementTypes.size(); i++) {
				s = s + statementTypes.get(i).getLabel() + " (" + Dna.sql.countStatements(statementTypes.get(i).getId()) + ")";
				if (i < statementTypes.size() - 1) {
					s = s + ", ";
				}
			}
			s = s + ".\nActive coder '" + Dna.sql.getActiveCoder().getName() + "' (ID: " + Dna.sql.getActiveCoder().getId() + ") owns " + coderItems[0] + " documents and " + coderItems[1] + " statements.";
		}
		System.out.println(s);
		LogEvent l = new LogEvent(Logger.MESSAGE,
				"Database details were aggregated and printed.",
				"The active coder queried the database for the number of documents and statements and similar statistics and printed the output to the console.");
		Dna.logger.log(l);
	}
	
	/**
	 * Open a database connection and authenticate the coder.
	 * 
	 * @param coderId The coder ID. If smaller than {@code 1}, a coder password
	 *   check dialog is shown.
	 * @param coderPassword The coder password. If {@code null}, a coder
	 *   password check dialog is shown.
	 * @param type The database type. Can be {@code "sqlite"}, {@code "mysql"},
	 *   or {@code "postgresql"}.
	 * @param databaseUrl The database URL (for MySQL/MariaDB or PostgreSQL) or
	 *   file name with full path (for SQLite).
	 * @param databaseName The database name. Can be an empty string ({@code
	 *   ""}) for SQLite.
	 * @param databasePort The connection port for the database. Can be {@code
	 *   -1} for SQLite.
	 * @param databaseUser The user name to connect to the database. Can be an
	 *   empty string ({@code ""}) for SQLite.
	 * @param databasePassword The password to connect to the database. Can be
	 *   an empty string ({@code ""}) for SQLite.
	 * @return Indicator of successful coder authentication.
	 */
	public boolean openDatabase(int coderId, String coderPassword, String type, String databaseUrl, String databaseName, int databasePort, String databaseUser, String databasePassword) {
		if (coderId < 1) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Failed to authenticate coder " + coderId + ".",
					"Coder " + coderId + " could not be authenticated. Check the coder ID and password. You can query the available coders using the 'queryCoders' function.");
			Dna.logger.log(l);
			return false;
		}
		ConnectionProfile cp = new ConnectionProfile(type, databaseUrl, databaseName, databasePort, databaseUser, databasePassword);
		Sql testSql = new Sql(cp, true);
		boolean success = testSql.authenticate(coderId, coderPassword);
		String version = testSql.getVersion();
		if (success) {
			if (version.startsWith("3.0")) {
				Dna.sql.setConnectionProfile(cp, false);
				Dna.sql.selectCoder(coderId);
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"Coder " + Dna.sql.getActiveCoder().getId() + " (" + Dna.sql.getActiveCoder().getName() + ") successfully authenticated.",
						"Coder " + Dna.sql.getActiveCoder().getId() + " (" + Dna.sql.getActiveCoder().getName() + ") successfully authenticated. You can now use the functions available to this user.");
				Dna.logger.log(l);
			} else {
				LogEvent l = new LogEvent(Logger.ERROR,
						"Tried to open an incompatible database version.",
						"You tried to open a DNA database with version \" + version + \", but you can only open databases with version 3.0. Data from version 2 databases can also be imported into a new or existing DNA 3 database using the importer in the Documents menu.");
				Dna.logger.log(l);
			}

			printDatabaseDetails();
		} else {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Failed to authenticate coder " + coderId + ".",
					"Coder " + coderId + " could not be authenticated. Check the coder ID and password. You can query the available coders using the 'queryCoders' function.");
			Dna.logger.log(l);
		}
		return success;
	}
	
	/**
	 * Open a connection profile and authenticate its coder.
	 * 
	 * @param fileName The file name with full path to access the connection
	 *   profile. 
	 * @param clearCoderPassword The clear coder password.
	 * @return Indicator of whether the database was opened and the coder was
	 *   successfully authenticated.
	 */
	public boolean openConnectionProfile(String fileName, String clearCoderPassword) {
		ConnectionProfile cp = null;
		try {
			cp = new ConnectionProfile(fileName, clearCoderPassword);
		} catch (EncryptionOperationNotPossibleException e2) {
			cp = null;
			LogEvent l = new LogEvent(Logger.ERROR,
					"Connection profile could not be decrypted.",
					"Tried to decrypt a connection profile, but the decryption failed. Make sure the password corresponds to the coder who saved the connection profile.",
					e2);
			Dna.logger.log(l);
		}
		if (cp != null) {
			Sql sqlTemp = new Sql(cp, true); // just for authentication purposes, so a test
			if (sqlTemp.getDataSource() == null) {
				LogEvent l = new LogEvent(Logger.ERROR,
						"No data source available in the database connection.",
						"Tried to a connection profile, but the coder could not be authenticated because there is no data source available. This may be due to a failed connection to the database. Look out for other error messages.");
				Dna.logger.log(l);
			} else {
				boolean authenticated = sqlTemp.authenticate(-1, clearCoderPassword);
				if (authenticated) {
					Dna.sql.setConnectionProfile(sqlTemp.getConnectionProfile(), false);
					LogEvent l = new LogEvent(Logger.MESSAGE,
							"Connection profile opened and coder authenticated.",
							"A connection profile was opened, and coder " + Dna.sql.getActiveCoder().getId() + " (" + Dna.sql.getActiveCoder().getName() + ") was successfully authenticated. You can now use the functions available to this user.");
					Dna.logger.log(l);
					printDatabaseDetails();
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Save connection profile to a file.
	 * 
	 * @param fileName File name with absolute path.
	 * @param clearCoderPassword The clear coder password (for verification).
	 * @return Indicator of success.
	 */
	public boolean saveConnectionProfile(String fileName, String clearCoderPassword) {
		if (Dna.sql == null || Dna.sql.getConnectionProfile() == null || Dna.sql.getActiveCoder() == null) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"No database open. Could not save connection profile.",
					"Tried to save a connection profile, but no database connection was open or no coder was active.");
			Dna.logger.log(l);
			return false;
		}
		if (fileName == null || !fileName.toLowerCase().endsWith(".dnc")) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"File name must end with '.dnc'.",
					"Tried to save a connection profile with the following file name, but the file name does not end with '.dnc' as required: \"" + fileName + "\".");
			Dna.logger.log(l);
			return false;
		}
		File file = new File(fileName);
		if (file.exists()) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"File already exists and will not be overwritten.",
					"Tried to save a connection profile with the following file name, but the file already exists and will not be overwritten: \"" + fileName + "\".");
			Dna.logger.log(l);
			return false;
		}
		boolean authenticated = Dna.sql.authenticate(-1, clearCoderPassword);
		if (authenticated) {
			// write the connection profile to disk, with an encrypted version of the password
			ConnectionProfile.writeConnectionProfile(fileName, new ConnectionProfile(Dna.sql.getConnectionProfile()), clearCoderPassword);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Connection profile saved to file.",
					"A connection profile was successfully saved to the following file: \"" + fileName + "\".");
			Dna.logger.log(l);
			return true;
		} else {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Coder password could not be verified. Try again.",
					"Tried to save a connection profile, but the coder could not be authenticated. Please try again.");
			Dna.logger.log(l);
			return false;
		}
	}
	
	/**
	 * Close the current database.
	 */
	public void closeDatabase() {
		if (Dna.sql == null || Dna.sql.getConnectionProfile() == null || Dna.sql.getActiveCoder() == null) {
			Dna.sql.setConnectionProfile(null, false);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Tried to close database, but none was open.",
					"Tried to close database, but none was open. Set the current connection profile to null anyway.");
			Dna.logger.log(l);
			System.out.println("Tried to close database, but none was open.");
		} else {
			Dna.sql.setConnectionProfile(null, false);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Database was closed.",
					"Closed database connection.");
			Dna.logger.log(l);
			System.out.println("Database was closed.");
		}
		
	}
	
	/**
	 * Return IDs, names, and colors of all coders found in a database.
	 * 
	 * @param type The database type. Can be {@code "sqlite"}, {@code "mysql"},
	 *   or {@code "postgresql"}.
	 * @param databaseUrl The database URL (for MySQL/MariaDB or PostgreSQL) or
	 *   file name with full path (for SQLite).
	 * @param databaseName The database name. Can be an empty string ({@code
	 *   ""}) for SQLite.
	 * @param databasePort The connection port for the database. Can be {@code
	 *   -1} for SQLite.
	 * @param databaseUser The user name to connect to the database. Can be an
	 *   empty string ({@code ""}) for SQLite.
	 * @param databasePassword The password to connect to the database. Can be
	 *   an empty string ({@code ""}) for SQLite.
	 * @return An {@link Object} array, where the first element is an {@link
	 *   int} array of coder IDs, the second element is a {@link String} array
	 *   of coder names, and the third element is an array of coder colors.
	 */
	public Object[] queryCoders(String type, String databaseUrl, String databaseName, int databasePort, String databaseUser, String databasePassword) {
		ConnectionProfile testCp = new ConnectionProfile(type, databaseUrl, databaseName, databasePort, databaseUser, databasePassword);
		Sql testSql = new Sql(testCp, true);
		ArrayList<Coder> coders = testSql.queryCoders();
		Object[] objects = new Object[3];
		objects[0] = coders.stream().mapToInt(c -> c.getId()).toArray();
		objects[1] = coders.stream().map(c -> c.getName()).toArray(String[]::new);
		objects[2] = coders
				.stream()
				.map(c -> c.getColor())
				.map(col -> String.format("#%02X%02X%02X",
						col.getRed(),
						col.getGreen(),
						col.getBlue()))
				.toArray(String[]::new);
		LogEvent l = new LogEvent(Logger.MESSAGE,
				"Coders have been queried.",
				"The coders for the following database were queried successfully: " + databaseUrl);
		Dna.logger.log(l);
		return objects;
	}

	/**
	 * Convert start and stop date and time strings into {@link LocalDateTime} objects.
	 *
	 * @param startDate The start date. If {@code "dd.MM.yyyy"}, {@code ""}, or {@code null}, the earliest date in the
	 *                  database will be queried instead.
	 * @param startTime The start time. If {@code "00:00:00"}, {@code ""}, or {@code null}, {@code "00:00:00"} will be
	 *                  used.
	 * @param stopDate The end date. If {@code "dd.MM.yyyy"}, {@code ""}, or {@code null}, the last date in the
	 * 	               database will be queried instead.
	 * @param stopTime The end time. If {@code "00:00:00"}, {@code ""}, or {@code null}, {@code "00:00:00"} will be
	 * 	               used.
	 * @return A {@link LocalDateTime} array with two elements for the start and end date/time.
	 */
	private LocalDateTime[] formatDateTime(String startDate, String startTime, String stopDate, String stopTime) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = Dna.sql.getDateTimeRange();
		if (startTime == null || startTime.equals("")) {
			startTime = "00:00:00";
		}
		if (startDate == null || startDate.equals("") || startDate.equals("01.01.1900")) {
			ldtStart = dateRange[0];
		} else {
			String startString = startDate + " " + startTime;
			ldtStart = LocalDateTime.parse(startString, dtf);
			if (!startString.equals(dtf.format(ldtStart))) {
				ldtStart = dateRange[0];
				LogEvent le = new LogEvent(Logger.WARNING,
						"Start date or time is invalid.",
						"When initializing computations, the start date or time (" + startString + ") did not conform to the format dd.MM.yyyy HH:mm:ss and could not be interpreted. Assuming earliest date and time in the dataset: " + ldtStart.format(dtf) + ".");
				Dna.logger.log(le);
			}
		}
		if (stopTime == null || stopTime.equals("")) {
			stopTime = "23:59:59";
		}
		if (stopDate == null || stopDate.equals("") || stopDate.equals("31.12.2099")) {
			ldtStop = dateRange[1];
		} else {
			String stopString = stopDate + " " + stopTime;
			ldtStop = LocalDateTime.parse(stopString, dtf);
			if (!stopString.equals(dtf.format(ldtStop))) {
				ldtStop = dateRange[1];
				LogEvent le = new LogEvent(Logger.WARNING,
						"End date or time is invalid.",
						"When initializing computations, the end date or time (" + stopString + ") did not conform to the format dd.MM.yyyy HH:mm:ss and could not be interpreted. Assuming latest date and time in the dataset: " + ldtStop.format(dtf) + ".");
				Dna.logger.log(le);
			}
		}
		return new LocalDateTime[] { ldtStart, ldtStop };
	}

	/**
	 * Compute one-mode or two-mode network matrix based on R arguments.
	 *
	 * @param networkType            The network type as provided by rDNA (can be {@code "eventlist"}, {@code "twomode"}, or {@code "onemode"}).
	 * @param statementType          Statement type as a {@link String}.
	 * @param variable1              First variable for export, provided as a {@link String}.
	 * @param variable1Document      boolean indicating if the first variable is at the document level.
	 * @param variable2              Second variable for export, provided as a {@link String}.
	 * @param variable2Document      boolean indicating if the second variable is at the document level.
	 * @param qualifier              Qualifier variable as a {@link String}.
	 * @param qualifierDocument      boolean indicating if the qualifier variable is at the document level.
	 * @param qualifierAggregation   Aggregation rule for the qualifier variable (can be {@code "ignore"}, {@code "combine"}, {@code "subtract"}, {@code "congruence"}, or {@code "conflict"}).
	 * @param normalization          Normalization setting as a {@link String}, as provided by rDNA (can be {@code "no"}, {@code "activity"}, {@code "prominence"}, {@code "average"}, {@code "jaccard"}, or {@code "cosine"}).
	 * @param includeIsolates        boolean indicating whether nodes not currently present should still be inserted into the network matrix.
	 * @param duplicates             An input {@link String} from rDNA that can be {@code "include"}, {@code "document"}, {@code "week"}, {@code "month"}, {@code "year"}, or {@code "acrossrange"}.
	 * @param startDate              Start date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param stopDate               Stop date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param startTime              Start time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param stopTime               Stop time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param timeWindow             A {@link String} indicating the time window setting. Valid options are {@code "no"}, {@code "events"}, {@code "seconds"}, {@code "minutes"}, {@code "hours"}, {@code "days"}, {@code "weeks"}, {@code "months"}, and {@code "years"}.
	 * @param windowSize             Duration of the time window in the units specified in the {@code timeWindow} argument.
	 * @param kernel                 The kernel function for temporal smoothing: {@code "no"}, {@code "uniform"}, {@code "epanechnikov"}, {@code "triangular"}, or {@code "gaussian"}.
	 * @param excludeVariables       A {@link String} array with n elements, indicating the variable of the n'th value.
	 * @param excludeValues          A {@link String} array with n elements, indicating the value pertaining to the n'th variable {@link String}.
	 * @param excludeAuthors         A {@link String} array of values to exclude in the {@code author} variable at the document level.
	 * @param excludeSources         A {@link String} array of values to exclude in the {@code source} variable at the document level.
	 * @param excludeSections        A {@link String} array of values to exclude in the {@code section} variable at the document level.
	 * @param excludeTypes           A {@link String} array of values to exclude in the {@code "type"} variable at the document level.
	 * @param invertValues           boolean indicating whether the statement-level exclude values should be included (= {@code true}) rather than excluded.
	 * @param invertAuthors          boolean indicating whether the document-level author values should be included (= {@code true}) rather than excluded.
	 * @param invertSources          boolean indicating whether the document-level source values should be included (= {@code true}) rather than excluded.
	 * @param invertSections         boolean indicating whether the document-level section values should be included (= {@code true}) rather than excluded.
	 * @param invertTypes            boolean indicating whether the document-level type values should be included (= {@code true}) rather than excluded.
	 * @param outfile                {@link String} with a file name under which the resulting network should be saved.
	 * @param fileFormat             {@link String} with the file format. Valid values are {@code "csv"}, {@code "dl"}, {@code "graphml"}, and {@code null} (for no file export).
	 * @return                       A {@link Matrix} object containing the resulting one-mode or two-mode network.
	 */
	public void rNetwork(String networkType, String statementType, String variable1, boolean variable1Document, String variable2,
						 boolean variable2Document, String qualifier, boolean qualifierDocument, String qualifierAggregation, String normalization, boolean includeIsolates,
						 String duplicates, String startDate, String stopDate, String startTime, String stopTime, String timeWindow, int windowSize, String kernel,
						 String[] excludeVariables, String[] excludeValues, String[] excludeAuthors, String[] excludeSources, String[] excludeSections,
						 String[] excludeTypes, boolean invertValues, boolean invertAuthors, boolean invertSources, boolean invertSections,
						 boolean invertTypes, String outfile, String fileFormat) {

		// step 1: preprocess arguments
		StatementType st = Dna.sql.getStatementType(statementType); // format statement type

		// format dates and times with input formats "dd.MM.yyyy" and "HH:mm:ss"
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = formatDateTime(startDate, startTime, stopDate, stopTime);
		ldtStart = dateRange[0];
		ldtStop = dateRange[1];

		// process exclude variables: create HashMap with variable:value pairs
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		if (excludeVariables.length > 0) {
			for (int i = 0; i < excludeVariables.length; i++) {
				ArrayList<String> values = map.get(excludeVariables[i]);
				if (values == null) {
					values = new ArrayList<String>();
				}
				if (!values.contains(excludeValues[i])) {
					values.add(excludeValues[i]);
				}
				Collections.sort(values);
				map.put(excludeVariables[i], values);
			}
		}

		// initialize Exporter class
		this.exporter = new Exporter(
				networkType,
				st,
				variable1,
				variable1Document,
				variable2,
				variable2Document,
				qualifier,
				qualifierDocument,
				qualifierAggregation,
				normalization,
				includeIsolates,
				duplicates,
				ldtStart,
				ldtStop,
				timeWindow,
				windowSize,
				map,
				Stream.of(excludeAuthors).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSources).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSections).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeTypes).collect(Collectors.toCollection(ArrayList::new)),
				invertValues,
				invertAuthors,
				invertSources,
				invertSections,
				invertTypes,
				fileFormat,
				outfile);
		this.exporter.setKernelFunction(kernel);

		// step 2: filter
		this.exporter.loadData();
		this.exporter.filterStatements();

		// step 3: compute results
		if (networkType.equals("eventlist")) {
			// TODO
		} else {
			try {
				this.exporter.computeResults();
			} catch (Exception e) {
				LogEvent le = new LogEvent(Logger.ERROR,
						"Error while exporting network.",
						"An unexpected error occurred while exporting a network. See the stack trace for details. Consider reporting this error.",
						e);
				Dna.logger.log(le);
			}
		}

		// step 4: save to file
		if (fileFormat != null && !fileFormat.equals("") && outfile != null && !outfile.equals("")) {
			this.exporter.exportToFile();
		}
	}

	/**
	 * Compute a sequence of one-mode or two-mode network matrices using time windows based on R arguments.
	 *
	 * @param networkType            The network type as provided by rDNA (can be {@code "eventlist"}, {@code "twomode"}, or {@code "onemode"}).
	 * @param statementType          Statement type as a {@link String}.
	 * @param variable1              First variable for export, provided as a {@link String}.
	 * @param variable1Document      boolean indicating if the first variable is at the document level.
	 * @param variable2              Second variable for export, provided as a {@link String}.
	 * @param variable2Document      boolean indicating if the second variable is at the document level.
	 * @param qualifier              Qualifier variable as a {@link String}.
	 * @param qualifierDocument      boolean indicating if the qualifier variable is at the document level.
	 * @param qualifierAggregation   Aggregation rule for the qualifier variable (can be {@code "ignore"}, {@code "combine"}, {@code "subtract"}, {@code "congruence"}, or {@code "conflict"}).
	 * @param normalization          Normalization setting as a {@link String}, as provided by rDNA (can be {@code "no"}, {@code "activity"}, {@code "prominence"}, {@code "average"}, {@code "jaccard"}, or {@code "cosine"}).
	 * @param includeIsolates        boolean indicating whether nodes not currently present should still be inserted into the network matrix.
	 * @param duplicates             An input {@link String} from rDNA that can be {@code "include"}, {@code "document"}, {@code "week"}, {@code "month"}, {@code "year"}, or {@code "acrossrange"}.
	 * @param startDate              Start date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param stopDate               Stop date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param startTime              Start time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param stopTime               Stop time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param timeWindow             A {@link String} indicating the time window setting. Valid options are {@code "no"}, {@code "events"}, {@code "seconds"}, {@code "minutes"}, {@code "hours"}, {@code "days"}, {@code "weeks"}, {@code "months"}, and {@code "years"}.
	 * @param windowSize             Duration of the time window in the units specified in the {@code timeWindow} argument.
	 * @param kernel                 The kernel function for temporal smoothing: {@code "no"}, {@code "uniform"}, {@code "epanechnikov"}, {@code "triangular"}, or {@code "gaussian"}.
	 * @param normalizeToOne         boolean indicating if each cell in the network matrix should be divided by the sum of all cells of the same time step (i.e., normalized to one such that the sum of all cells is {@code 1.0}).
	 * @param indentTime             boolean indicating if the timeline should be indented at the beginning and end such that all networks in the sequence of time windows must fit entirely into the timeline. If false, the first mid-point starts at the beginning, and the last network ends at the stop date and time.
	 * @param excludeVariables       A {@link String} array with n elements, indicating the variable of the n'th value.
	 * @param excludeValues          A {@link String} array with n elements, indicating the value pertaining to the n'th variable {@link String}.
	 * @param excludeAuthors         A {@link String} array of values to exclude in the {@code author} variable at the document level.
	 * @param excludeSources         A {@link String} array of values to exclude in the {@code source} variable at the document level.
	 * @param excludeSections        A {@link String} array of values to exclude in the {@code section} variable at the document level.
	 * @param excludeTypes           A {@link String} array of values to exclude in the {@code "type"} variable at the document level.
	 * @param invertValues           boolean indicating whether the statement-level exclude values should be included (= {@code true}) rather than excluded.
	 * @param invertAuthors          boolean indicating whether the document-level author values should be included (= {@code true}) rather than excluded.
	 * @param invertSources          boolean indicating whether the document-level source values should be included (= {@code true}) rather than excluded.
	 * @param invertSections         boolean indicating whether the document-level section values should be included (= {@code true}) rather than excluded.
	 * @param invertTypes            boolean indicating whether the document-level type values should be included (= {@code true}) rather than excluded.
	 * @return                       A {@link Matrix} object containing the resulting one-mode or two-mode network.
	 */
	public void rTimeWindow(String networkType, String statementType, String variable1, boolean variable1Document, String variable2,
						 boolean variable2Document, String qualifier, boolean qualifierDocument, String qualifierAggregation, String normalization, boolean includeIsolates,
						 String duplicates, String startDate, String stopDate, String startTime, String stopTime, String timeWindow, int windowSize, String kernel,
						 boolean normalizeToOne, boolean indentTime, String[] excludeVariables, String[] excludeValues, String[] excludeAuthors, String[] excludeSources,
						 String[] excludeSections, String[] excludeTypes, boolean invertValues, boolean invertAuthors, boolean invertSources, boolean invertSections,
						 boolean invertTypes) {

		// step 1: preprocess arguments
		StatementType st = Dna.sql.getStatementType(statementType); // format statement type

		// format dates and times with input formats "dd.MM.yyyy" and "HH:mm:ss"
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = formatDateTime(startDate, startTime, stopDate, stopTime);
		ldtStart = dateRange[0];
		ldtStop = dateRange[1];

		// process exclude variables: create HashMap with variable:value pairs
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		if (excludeVariables.length > 0) {
			for (int i = 0; i < excludeVariables.length; i++) {
				ArrayList<String> values = map.get(excludeVariables[i]);
				if (values == null) {
					values = new ArrayList<String>();
				}
				if (!values.contains(excludeValues[i])) {
					values.add(excludeValues[i]);
				}
				Collections.sort(values);
				map.put(excludeVariables[i], values);
			}
		}

		// initialize Exporter class
		this.exporter = new Exporter(
				networkType,
				st,
				variable1,
				variable1Document,
				variable2,
				variable2Document,
				qualifier,
				qualifierDocument,
				qualifierAggregation,
				normalization,
				includeIsolates,
				duplicates,
				ldtStart,
				ldtStop,
				timeWindow,
				windowSize,
				map,
				Stream.of(excludeAuthors).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSources).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSections).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeTypes).collect(Collectors.toCollection(ArrayList::new)),
				invertValues,
				invertAuthors,
				invertSources,
				invertSections,
				invertTypes,
				null,
				null);
		this.exporter.setKernelFunction(kernel);
		this.exporter.setIndentTime(indentTime);

		// step 2: filter
		this.exporter.loadData();
		this.exporter.filterStatements();

		// step 3: compute results
		if (networkType.equals("eventlist")) {
			// TODO
		} else {
			try {
				this.exporter.computeResults();

				// step 4: normalize to one
				if (normalizeToOne) {
					this.exporter.normalizeMatrixResultsToOne();
				}
			} catch (Exception e) {
				LogEvent le = new LogEvent(Logger.ERROR,
						"Error while exporting network.",
						"An unexpected error occurred while exporting a network. See the stack trace for details. Consider reporting this error.",
						e);
				Dna.logger.log(le);
			}
		}
	}

	/**
	 * Compute a sequence of one-mode network matrices or a single one-mode network matrix, and compute their polarization score(s) and diagnostics.
	 *
	 * @param statementType      Statement type as a {@link String}.
	 * @param variable1          First variable for export, provided as a {@link String}.
	 * @param variable1Document  boolean indicating if the first variable is at the document level.
	 * @param variable2          Second variable for export, provided as a {@link String}.
	 * @param variable2Document  boolean indicating if the second variable is at the document level.
	 * @param qualifier          Qualifier variable as a {@link String}.
	 * @param duplicates         An input {@link String} from rDNA that can be {@code "include"}, {@code "document"}, {@code "week"}, {@code "month"}, {@code "year"}, or {@code "acrossrange"}.
	 * @param startDate          Start date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param stopDate           Stop date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param timeWindow         A {@link String} indicating the time window setting. Valid options are {@code "no"}, {@code "events"}, {@code "seconds"}, {@code "minutes"}, {@code "hours"}, {@code "days"}, {@code "weeks"}, {@code "months"}, and {@code "years"}.
	 * @param windowSize         Duration of the time window in the units specified in the {@code timeWindow} argument.
	 * @param kernel             The kernel function for temporal smoothing: {@code "no"}, {@code "uniform"}, {@code "epanechnikov"}, {@code "triangular"}, or {@code "gaussian"}.
	 * @param indentTime         boolean indicating if the timeline should be indented at the beginning and end such that all networks in the sequence of time windows must fit entirely into the timeline. If false, the first mid-point starts at the beginning, and the last network ends at the stop date and time.
	 * @param excludeVariables   A {@link String} array with n elements, indicating the variable of the n'th value.
	 * @param excludeValues      A {@link String} array with n elements, indicating the value pertaining to the n'th variable {@link String}.
	 * @param excludeAuthors     A {@link String} array of values to exclude in the {@code author} variable at the document level.
	 * @param excludeSources     A {@link String} array of values to exclude in the {@code source} variable at the document level.
	 * @param excludeSections    A {@link String} array of values to exclude in the {@code section} variable at the document level.
	 * @param excludeTypes       A {@link String} array of values to exclude in the {@code "type"} variable at the document level.
	 * @param invertValues       boolean indicating whether the statement-level exclude values should be included (= {@code true}) rather than excluded.
	 * @param invertAuthors      boolean indicating whether the document-level author values should be included (= {@code true}) rather than excluded.
	 * @param invertSources      boolean indicating whether the document-level source values should be included (= {@code true}) rather than excluded.
	 * @param invertSections     boolean indicating whether the document-level section values should be included (= {@code true}) rather than excluded.
	 * @param invertTypes        boolean indicating whether the document-level type values should be included (= {@code true}) rather than excluded.
	 * @param algorithm          The algorithm to maximise polarization at each time step. Can be "greedy" (for a greedy algorithm) or "genetic" (for a genetic algorithm).
	 * @param normaliseScores    boolean indicating whether the polarization scores should be normalised by dividing them by their theoretical maximum within a given network. This takes away the effect of more activity (possibly due to participation by more actors or more statements per actor) contributing to polarization scores and focuses solely on structure given the edge mass in the network. Without normalisation, time periods with more actors and activity will elevate the polarization of the network (at constant levels of being divided over concepts).
	 * @param numClusters		 The number of clusters or factions k, for example 2 for bi-polarization.
	 * @param numParents         Only for the genetic algorithm: The number of cluster solutions (i.e., parents) to generate in each iteration, for example 30 or 50.
	 * @param numterations       Only for the genetic algorithm: For how many generations should the genetic algorithm run at most? This is the maximal number of generations through which optimisation should be attempted. Will be lower if early convergence is detected. A suggested starting value is 1000.
	 * @param elitePercentage    Only for the genetic algorithm: The share of cluster solutions in each parent generation that is copied into the children generation without changes, between 0.0 and 1.0, usually around 0.1.
	 * @param mutationPercentage Only for the genetic algorithm: The probability with which each bit in any cluster solution is selected for mutation after the cross-over step. For example 0.1 to select 10% of the nodes to swap their memberships.
	 * @param randomSeed         Only for the genetic algorithm: The random seed to use for the random number generator. Pass 0 for random behaviour.
	 * @return                   A PolarizationResultTimeSeries object containing the results of the algorithm for each time step and iteration.
	 */
	public PolarizationResultTimeSeries rPolarization(String statementType, String variable1, boolean variable1Document,
			String variable2, boolean variable2Document, String qualifier, String duplicates, String startDate,
			String stopDate, String timeWindow, int windowSize, String kernel, boolean indentTime,
			String[] excludeVariables, String[] excludeValues, String[] excludeAuthors, String[] excludeSources,
			String[] excludeSections, String[] excludeTypes, boolean invertValues, boolean invertAuthors,
			boolean invertSources, boolean invertSections, boolean invertTypes, String algorithm, boolean normaliseScores,
			int numClusters, int numParents, int numIterations, double elitePercentage, double mutationPercentage,
			long randomSeed) {

		// step 1: preprocess arguments
		StatementType st = Dna.sql.getStatementType(statementType); // format statement type

		// format dates and times with input formats "dd.MM.yyyy" and "HH:mm:ss"
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = formatDateTime(startDate, "00:00:00", stopDate, "23:59:59");
		ldtStart = dateRange[0];
		ldtStop = dateRange[1];

		// process exclude variables: create HashMap with variable:value pairs
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		if (excludeVariables.length > 0) {
			for (int i = 0; i < excludeVariables.length; i++) {
				ArrayList<String> values = map.get(excludeVariables[i]);
				if (values == null) {
					values = new ArrayList<String>();
				}
				if (!values.contains(excludeValues[i])) {
					values.add(excludeValues[i]);
				}
				Collections.sort(values);
				map.put(excludeVariables[i], values);
			}
		}

		Polarization polarization = new Polarization(st, variable1, variable1Document, variable2, variable2Document,
				qualifier, false, duplicates, ldtStart, ldtStop, timeWindow, windowSize, kernel, indentTime,
				map, excludeAuthors, excludeSources,	excludeSections, excludeTypes, invertValues, invertAuthors,
				invertSources, invertSections, invertTypes, algorithm, normaliseScores, numClusters, numParents,
				numIterations, elitePercentage, mutationPercentage, randomSeed);

		return polarization.getResults();
	}

	/**
	 * Generate data to construct a barplot.
	 *
	 * @param statementType     Statement type as a {@link String}.
	 * @param variable          First variable for export, provided as a {@link String}.
	 * @param qualifier         Qualifier variable as a {@link String}.
	 * @param duplicates        An input {@link String} from rDNA that can be {@code "include"}, {@code "document"}, {@code "week"}, {@code "month"}, {@code "year"}, or {@code "acrossrange"}.
	 * @param startDate         Start date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param stopDate          Stop date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param startTime         Start time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param stopTime          Stop time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param excludeVariables  A {@link String} array with n elements, indicating the variable of the n'th value.
	 * @param excludeValues     A {@link String} array with n elements, indicating the value pertaining to the n'th variable {@link String}.
	 * @param excludeAuthors    A {@link String} array of values to exclude in the {@code author} variable at the document level.
	 * @param excludeSources    A {@link String} array of values to exclude in the {@code source} variable at the document level.
	 * @param excludeSections   A {@link String} array of values to exclude in the {@code section} variable at the document level.
	 * @param excludeTypes      A {@link String} array of values to exclude in the {@code "type"} variable at the document level.
	 * @param invertValues      boolean indicating whether the statement-level exclude values should be included (= {@code true}) rather than excluded.
	 * @param invertAuthors     boolean indicating whether the document-level author values should be included (= {@code true}) rather than excluded.
	 * @param invertSources     boolean indicating whether the document-level source values should be included (= {@code true}) rather than excluded.
	 * @param invertSections    boolean indicating whether the document-level section values should be included (= {@code true}) rather than excluded.
	 * @param invertTypes       boolean indicating whether the document-level type values should be included (= {@code true}) rather than excluded.
	 * @return                  A {@link BarplotResult} object.
	 */
	public BarplotResult rBarplotData(String statementType, String variable, String qualifier, String duplicates,
									  String startDate, String stopDate, String startTime, String stopTime,
									  String[] excludeVariables, String[] excludeValues, String[] excludeAuthors,
									  String[] excludeSources, String[] excludeSections, String[] excludeTypes,
									  boolean invertValues, boolean invertAuthors, boolean invertSources, boolean invertSections,
									  boolean invertTypes) {

		// step 1: preprocess arguments
		StatementType st = Dna.sql.getStatementType(statementType); // format statement type

		// format dates and times with input formats "dd.MM.yyyy" and "HH:mm:ss"
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = formatDateTime(startDate, startTime, stopDate, stopTime);
		ldtStart = dateRange[0];
		ldtStop = dateRange[1];

		// process exclude variables: create HashMap with variable:value pairs
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		if (excludeVariables.length > 0) {
			for (int i = 0; i < excludeVariables.length; i++) {
				ArrayList<String> values = map.get(excludeVariables[i]);
				if (values == null) {
					values = new ArrayList<String>();
				}
				if (!values.contains(excludeValues[i])) {
					values.add(excludeValues[i]);
				}
				Collections.sort(values);
				map.put(excludeVariables[i], values);
			}
		}

		// initialize Exporter class for barplot data
		this.exporter = new Exporter(
				st,
				variable,
				qualifier,
				duplicates,
				ldtStart,
				ldtStop,
				map,
				Stream.of(excludeAuthors).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSources).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSections).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeTypes).collect(Collectors.toCollection(ArrayList::new)),
				invertValues,
				invertAuthors,
				invertSources,
				invertSections,
				invertTypes);

		// step 2: filter
		this.exporter.loadData();
		this.exporter.filterStatements();

		// step 3: compute results
		BarplotResult barplotResult = this.exporter.generateBarplotData();
		return barplotResult;
	}

	/**
	 * Compute backbone and set of redundant entities on the second mode in a discourse network.
	 *
	 * @param method                 Backbone algorithm (can be {@code "nested"}, {@code "fixed"}, or {@code "penalty"}).
	 * @param backboneSize           The number of elements in the backbone set, as a fixed parameter. Only used when {@code method = "size"}.
	 * @param p                      Penalty parameter, for example {@code 7.5}. Only used when {@code method = "penalty"}.
	 * @param T                      Number of iterations, for example {@code 50000}. Only used when {@code method = "penalty"}.
	 * @param statementType          Statement type as a {@link String}.
	 * @param variable1              First variable for export, provided as a {@link String}.
	 * @param variable1Document      boolean indicating if the first variable is at the document level.
	 * @param variable2              Second variable for export, provided as a {@link String}.
	 * @param variable2Document      boolean indicating if the second variable is at the document level.
	 * @param qualifier              Qualifier variable as a {@link String}.
	 * @param qualifierDocument      boolean indicating if the qualifier variable is at the document level.
	 * @param qualifierAggregation   Aggregation rule for the qualifier variable (can be {@code "ignore"}, {@code "combine"}, {@code "subtract"}, {@code "congruence"}, or {@code "conflict"}). Note that negative values in the {@code "subtract"} case are replaced by {@code 0}.
	 * @param normalization          Normalization setting as a {@link String}, as provided by rDNA (can be {@code "no"}, {@code "activity"}, {@code "prominence"}, {@code "average"}, {@code "jaccard"}, or {@code "cosine"}).
	 * @param duplicates             An input {@link String} from rDNA that can be {@code "include"}, {@code "document"}, {@code "week"}, {@code "month"}, {@code "year"}, or {@code "acrossrange"}.
	 * @param startDate              Start date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param stopDate               Stop date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param startTime              Start time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param stopTime               Stop time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param excludeVariables       A {@link String} array with n elements, indicating the variable of the n'th value.
	 * @param excludeValues          A {@link String} array with n elements, indicating the value pertaining to the n'th variable {@link String}.
	 * @param excludeAuthors         A {@link String} array of values to exclude in the {@code author} variable at the document level.
	 * @param excludeSources         A {@link String} array of values to exclude in the {@code source} variable at the document level.
	 * @param excludeSections        A {@link String} array of values to exclude in the {@code section} variable at the document level.
	 * @param excludeTypes           A {@link String} array of values to exclude in the {@code "type"} variable at the document level.
	 * @param invertValues           boolean indicating whether the statement-level exclude values should be included (= {@code true}) rather than excluded.
	 * @param invertAuthors          boolean indicating whether the document-level author values should be included (= {@code true}) rather than excluded.
	 * @param invertSources          boolean indicating whether the document-level source values should be included (= {@code true}) rather than excluded.
	 * @param invertSections         boolean indicating whether the document-level section values should be included (= {@code true}) rather than excluded.
	 * @param invertTypes            boolean indicating whether the document-level type values should be included (= {@code true}) rather than excluded.
	 * @param outfile                {@link String} with a file name under which the resulting network should be saved.
	 * @param fileFormat             {@link String} with the file format. Valid values are {@code "xml"}, {@code "json"}, and {@code null} (for no file export).
	 * @return                       A {@link SimulatedAnnealingBackboneResult} object containing the results.
	 */
	public void rBackbone(String method, int backboneSize, double p, int T, String statementType, String variable1, boolean variable1Document, String variable2,
						  boolean variable2Document, String qualifier, boolean qualifierDocument, String qualifierAggregation, String normalization,
						  String duplicates, String startDate, String stopDate, String startTime, String stopTime,
						  String[] excludeVariables, String[] excludeValues, String[] excludeAuthors, String[] excludeSources, String[] excludeSections,
						  String[] excludeTypes, boolean invertValues, boolean invertAuthors, boolean invertSources, boolean invertSections,
						  boolean invertTypes, String outfile, String fileFormat) {

		// step 1: preprocess arguments
		StatementType st = Dna.sql.getStatementType(statementType); // format statement type

		// format dates and times with input formats "dd.MM.yyyy" and "HH:mm:ss"
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = Dna.sql.getDateTimeRange();
		if (startTime == null || startTime.equals("")) {
			startTime = "00:00:00";
		}
		if (startDate == null || startDate.equals("") || startDate.equals("01.01.1900")) {
			ldtStart = dateRange[0];
		} else {
			String startString = startDate + " " + startTime;
			ldtStart = LocalDateTime.parse(startString, dtf);
			if (!startString.equals(dtf.format(ldtStart))) {
				ldtStart = dateRange[0];
				LogEvent le = new LogEvent(Logger.WARNING,
						"Start date or time is invalid.",
						"When computing the backbone and redundant set of the network, the start date or time (" + startString + ") did not conform to the format dd.MM.yyyy HH:mm:ss and could not be interpreted. Assuming earliest date and time in the dataset: " + ldtStart.format(dtf) + ".");
				Dna.logger.log(le);
			}
		}
		if (stopTime == null || stopTime.equals("")) {
			stopTime = "23:59:59";
		}
		if (stopDate == null || stopDate.equals("") || stopDate.equals("31.12.2099")) {
			ldtStop = dateRange[1];
		} else {
			String stopString = stopDate + " " + stopTime;
			ldtStop = LocalDateTime.parse(stopString, dtf);
			if (!stopString.equals(dtf.format(ldtStop))) {
				ldtStop = dateRange[1];
				LogEvent le = new LogEvent(Logger.WARNING,
						"End date or time is invalid.",
						"When computing the backbone and redundant set of the network, the end date or time (" + stopString + ") did not conform to the format dd.MM.yyyy HH:mm:ss and could not be interpreted. Assuming latest date and time in the dataset: " + ldtStop.format(dtf) + ".");
				Dna.logger.log(le);
			}
		}

		// process exclude variables: create HashMap with variable:value pairs
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		if (excludeVariables.length > 0) {
			for (int i = 0; i < excludeVariables.length; i++) {
				ArrayList<String> values = map.get(excludeVariables[i]);
				if (values == null) {
					values = new ArrayList<String>();
				}
				if (!values.contains(excludeValues[i])) {
					values.add(excludeValues[i]);
				}
				Collections.sort(values);
				map.put(excludeVariables[i], values);
			}
		}

		// initialize Exporter class
		this.exporter = new Exporter(
				"onemode",
				st,
				variable1,
				variable1Document,
				variable2,
				variable2Document,
				qualifier,
				qualifierDocument,
				qualifierAggregation,
				normalization,
				true,
				duplicates,
				ldtStart,
				ldtStop,
				"no",
				1,
				map,
				Stream.of(excludeAuthors).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSources).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSections).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeTypes).collect(Collectors.toCollection(ArrayList::new)),
				invertValues,
				invertAuthors,
				invertSources,
				invertSections,
				invertTypes,
				null,
				null);

		// step 2: filter
		this.exporter.loadData();
		this.exporter.filterStatements();
		if (exporter.getFilteredStatements().size() == 0) {
			LogEvent le = new LogEvent(Logger.ERROR,
					"No statements left after filtering.",
					"Attempted to filter the statements by date and other criteria before finding backbone. But no statements were left after applying the filters. Perhaps the time period was mis-specified?");
			Dna.logger.log(le);
		}

		// step 3: compute results
		if (method.equals("nested")) {
			this.exporter.initializeNestedBackbone();
			int iterations = 0;
			for (int i = 0; i < exporter.getFullSize(); i++) {
				iterations = iterations + (exporter.getFullSize() - i);
			}
			try (ProgressBar pb = new ProgressBar("Nested backbone...", iterations)) {
				while (exporter.getBackboneSize() > 0) { // run up to the point where all concepts have been moved from the backbone set to the redundant set
					int it = 1;
					for (int i = 0; i < exporter.getFullSize() - exporter.getBackboneSize(); i++) {
						it = it + (exporter.getFullSize() - i);
					}
					pb.stepTo(it); // go from empty backbone set to full backbone set
					exporter.iterateNestedBackbone();
				}
				exporter.saveNestedBackboneResult();

				// step 4: save to file
				saveJsonXml(fileFormat, outfile);
				pb.stepTo(iterations);
			}
		} else if (method.equals("fixed") || method.equals("penalty")) {
			this.exporter.initializeSimulatedAnnealingBackbone(method.equals("penalty"), p, T, backboneSize); // initialize algorithm
			try (ProgressBar pb = new ProgressBar("Simulated annealing...", T)) {
				while (exporter.getCurrentT() <= T) { // run up to upper bound of iterations T, provided by the user
					pb.stepTo(exporter.getCurrentT());
					exporter.iterateSimulatedAnnealingBackbone(method.equals("penalty"));
				}
				exporter.saveSimulatedAnnealingBackboneResult(method.equals("penalty"));

				// step 4: save to file
				saveJsonXml(fileFormat, outfile);
				pb.stepTo(T);
			}
		}
	}

	/**
	 * Compute the spectral loss for a given backbone set relative to the full network.
	 *
	 * @param backboneEntities       An array of entities (e.g., concepts) for which the spectral loss should be computed relative to the full network.
	 * @param p                      The penalty parameter. Can be \code{0} to switch off the penalty.
	 * @param statementType          Statement type as a {@link String}.
	 * @param variable1              First variable for export, provided as a {@link String}.
	 * @param variable1Document      boolean indicating if the first variable is at the document level.
	 * @param variable2              Second variable for export, provided as a {@link String}.
	 * @param variable2Document      boolean indicating if the second variable is at the document level.
	 * @param qualifier              Qualifier variable as a {@link String}.
	 * @param qualifierDocument      boolean indicating if the qualifier variable is at the document level.
	 * @param qualifierAggregation   Aggregation rule for the qualifier variable (can be {@code "ignore"}, {@code "combine"}, {@code "subtract"}, {@code "congruence"}, or {@code "conflict"}). Note that negative values in the {@code "subtract"} case are replaced by {@code 0}.
	 * @param normalization          Normalization setting as a {@link String}, as provided by rDNA (can be {@code "no"}, {@code "activity"}, {@code "prominence"}, {@code "average"}, {@code "jaccard"}, or {@code "cosine"}).
	 * @param duplicates             An input {@link String} from rDNA that can be {@code "include"}, {@code "document"}, {@code "week"}, {@code "month"}, {@code "year"}, or {@code "acrossrange"}.
	 * @param startDate              Start date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param stopDate               Stop date for the export, provided as a {@link String} with format {@code "dd.MM.yyyy"}.
	 * @param startTime              Start time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param stopTime               Stop time for the export, provided as a {@link String} with format {@code "HH:mm:ss"}.
	 * @param excludeVariables       A {@link String} array with n elements, indicating the variable of the n'th value.
	 * @param excludeValues          A {@link String} array with n elements, indicating the value pertaining to the n'th variable {@link String}.
	 * @param excludeAuthors         A {@link String} array of values to exclude in the {@code author} variable at the document level.
	 * @param excludeSources         A {@link String} array of values to exclude in the {@code source} variable at the document level.
	 * @param excludeSections        A {@link String} array of values to exclude in the {@code section} variable at the document level.
	 * @param excludeTypes           A {@link String} array of values to exclude in the {@code "type"} variable at the document level.
	 * @param invertValues           boolean indicating whether the statement-level exclude values should be included (= {@code true}) rather than excluded.
	 * @param invertAuthors          boolean indicating whether the document-level author values should be included (= {@code true}) rather than excluded.
	 * @param invertSources          boolean indicating whether the document-level source values should be included (= {@code true}) rather than excluded.
	 * @param invertSections         boolean indicating whether the document-level section values should be included (= {@code true}) rather than excluded.
	 * @param invertTypes            boolean indicating whether the document-level type values should be included (= {@code true}) rather than excluded.
	 * @return                       A double array with the loss for the backbone and redundant set.
	 */
	public double[] rEvaluateBackboneSolution(String[] backboneEntities, int p, String statementType, String variable1, boolean variable1Document, String variable2,
						  boolean variable2Document, String qualifier, boolean qualifierDocument, String qualifierAggregation, String normalization,
						  String duplicates, String startDate, String stopDate, String startTime, String stopTime,
						  String[] excludeVariables, String[] excludeValues, String[] excludeAuthors, String[] excludeSources, String[] excludeSections,
						  String[] excludeTypes, boolean invertValues, boolean invertAuthors, boolean invertSources, boolean invertSections,
						  boolean invertTypes) {

		// step 1: preprocess arguments
		StatementType st = Dna.sql.getStatementType(statementType); // format statement type

		// format dates and times with input formats "dd.MM.yyyy" and "HH:mm:ss"
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
		LocalDateTime ldtStart, ldtStop;
		LocalDateTime[] dateRange = Dna.sql.getDateTimeRange();
		if (startTime == null || startTime.equals("")) {
			startTime = "00:00:00";
		}
		if (startDate == null || startDate.equals("") || startDate.equals("01.01.1900")) {
			ldtStart = dateRange[0];
		} else {
			String startString = startDate + " " + startTime;
			ldtStart = LocalDateTime.parse(startString, dtf);
			if (!startString.equals(dtf.format(ldtStart))) {
				ldtStart = dateRange[0];
				LogEvent le = new LogEvent(Logger.WARNING,
						"Start date or time is invalid.",
						"When computing the backbone and redundant set of the network, the start date or time (" + startString + ") did not conform to the format dd.MM.yyyy HH:mm:ss and could not be interpreted. Assuming earliest date and time in the dataset: " + ldtStart.format(dtf) + ".");
				Dna.logger.log(le);
			}
		}
		if (stopTime == null || stopTime.equals("")) {
			stopTime = "23:59:59";
		}
		if (stopDate == null || stopDate.equals("") || stopDate.equals("31.12.2099")) {
			ldtStop = dateRange[1];
		} else {
			String stopString = stopDate + " " + stopTime;
			ldtStop = LocalDateTime.parse(stopString, dtf);
			if (!stopString.equals(dtf.format(ldtStop))) {
				ldtStop = dateRange[1];
				LogEvent le = new LogEvent(Logger.WARNING,
						"End date or time is invalid.",
						"When computing the spectral loss of a backbone set, the end date or time (" + stopString + ") did not conform to the format dd.MM.yyyy HH:mm:ss and could not be interpreted. Assuming latest date and time in the dataset: " + ldtStop.format(dtf) + ".");
				Dna.logger.log(le);
			}
		}

		// process exclude variables: create HashMap with variable:value pairs
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		if (excludeVariables.length > 0) {
			for (int i = 0; i < excludeVariables.length; i++) {
				ArrayList<String> values = map.get(excludeVariables[i]);
				if (values == null) {
					values = new ArrayList<String>();
				}
				if (!values.contains(excludeValues[i])) {
					values.add(excludeValues[i]);
				}
				Collections.sort(values);
				map.put(excludeVariables[i], values);
			}
		}

		// initialize Exporter class
		this.exporter = new Exporter(
				"onemode",
				st,
				variable1,
				variable1Document,
				variable2,
				variable2Document,
				qualifier,
				qualifierDocument,
				qualifierAggregation,
				normalization,
				true,
				duplicates,
				ldtStart,
				ldtStop,
				"no",
				1,
				map,
				Stream.of(excludeAuthors).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSources).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeSections).collect(Collectors.toCollection(ArrayList::new)),
				Stream.of(excludeTypes).collect(Collectors.toCollection(ArrayList::new)),
				invertValues,
				invertAuthors,
				invertSources,
				invertSections,
				invertTypes,
				null,
				null);

		// step 2: filter
		this.exporter.loadData();
		this.exporter.filterStatements();
		if (exporter.getFilteredStatements().size() == 0) {
			LogEvent le = new LogEvent(Logger.ERROR,
					"No statements left after filtering.",
					"Attempted to filter the statements by date and other criteria before finding backbone. But no statements were left after applying the filters. Perhaps the time period was mis-specified?");
			Dna.logger.log(le);
		}

		// step 3: compute and return results
		return this.exporter.evaluateBackboneSolution(backboneEntities, p);
	}

	private void saveJsonXml(String fileFormat, String outfile) {
		if (fileFormat != null && outfile != null) {
			if (fileFormat.equals("json") && !outfile.toLowerCase().endsWith(".json")) {
				outfile = outfile + ".json";
				LogEvent le = new LogEvent(Logger.WARNING,
						"Appended \".json\" to file name.",
						"The outfile for the backbone export did not end with \".json\" although the \"json\" file format was chosen. Appending \".json\" to the file name.");
				Dna.logger.log(le);
			}
			if (fileFormat.equals("xml") && !outfile.toLowerCase().endsWith(".xml")) {
				outfile = outfile + ".xml";
				LogEvent le = new LogEvent(Logger.WARNING,
						"Appended \".xml\" to file name.",
						"The outfile for the backbone export did not end with \".xml\" although the \"xml\" file format was chosen. Appending \".xml\" to the file name.");
				Dna.logger.log(le);
			}
			if (!fileFormat.equals("xml") && !fileFormat.equals("json")) {
				fileFormat = null;
				LogEvent le = new LogEvent(Logger.WARNING,
						"File format for backbone export not recognized.",
						"The file format for saving a backbone and redundant set to disk was not recognized. Valid file formats are \"json\" and \"xml\". The file format you provided was \"" + fileFormat + "\". Not saving the file to disk because the file format is unknown.");
				Dna.logger.log(le);
			} else {
				this.exporter.writeBackboneToFile(outfile);
			}
		}
	}

	/* =================================================================================================================
	 * Functions for managing variables
	 * =================================================================================================================
	 */

	/**
	 * Retrieve variables and data type definitions for a given statement type (via label).
	 *
	 * @param statementTypeLabel  Label of the statement type for which variables should be retrieved.
	 * @return                    Array list of {@link Value} objects representing variables.
	 */
	public ArrayList<Value> getVariables(String statementTypeLabel) {
		return Dna.sql.getStatementType(statementTypeLabel).getVariables();
	}

	/**
	 * Retrieve variables and data type definitions for a given statement type (via ID).
	 *
	 * @param statementTypeId  ID of the statement type for which variables should be retrieved.
	 * @return                 Array list of {@link Value} objects representing variables.
	 */
	public ArrayList<Value> getVariables(int statementTypeId) {
		return Dna.sql.getStatementType(statementTypeId).getVariables();
	}

	/* =================================================================================================================
	 * Functions for managing attributes
	 * =================================================================================================================
	 */

	/**
	 * Retrieve entities with attributes for a specific variable with a given variable ID.
	 *
	 * @param variableId ID of the variable to query for entities and their attributes.
	 * @return Data frame with entities and attributes, as defined in {@link sql.DataExchange#getAttributes(int)}.
	 */
	public DataFrame getAttributes(int variableId) {
		DataFrame df = sql.DataExchange.getAttributes(variableId);

		LogEvent l = new LogEvent(Logger.MESSAGE,
				"Attributes have been queried.",
				"The attributes for Variable " + variableId + " have been successfully retrieved from the database.");
		Dna.logger.log(l);

		return df;
	}

	/**
	 * Retrieve entities with attributes for a specific variable with a given variable name and statement type ID.
	 *
	 * @param statementTypeId The statement type ID in which the variable is defined.
	 * @param variable The name of the variable.
	 * @return Data frame with entities and attributes, as defined in {@link sql.DataExchange#getAttributes(int)}.
	 */
	public DataFrame getAttributes(int statementTypeId, String variable) {
		DataFrame df = sql.DataExchange.getAttributes(statementTypeId, variable);

		LogEvent l = new LogEvent(Logger.MESSAGE,
				"Attributes have been queried.",
				"The attributes for Variable \"" + variable + "\" have been successfully retrieved from the database.");
		Dna.logger.log(l);

		return df;
	}

	/**
	 * Retrieve entities with attributes for a specific variable with a given variable name and statement type name.
	 *
	 * @param statementType The statement type in which the variable is defined.
	 * @param variable The name of the variable.
	 * @return Data frame with entities and attributes, as defined in {@link sql.DataExchange#getAttributes(int)}.
	 */
	public DataFrame getAttributes(String statementType, String variable) {
		DataFrame df = sql.DataExchange.getAttributes(statementType, variable);

		LogEvent l = new LogEvent(Logger.MESSAGE,
				"Attributes have been queried.",
				"The attributes for Variable \"" + variable + "\" have been successfully retrieved from the database.");
		Dna.logger.log(l);

		return df;
	}

	/**
 	 * Set entities and attributes for a variable identified by its variable ID.
 	 *
 	 * <p>The supplied {@link DataFrame} must contain {@code m + 3} columns:
 	 * <ul>
 	 *   <li>column 0: entity ID (int)</li>
 	 *   <li>column 1: entity value (String)</li>
 	 *   <li>column 2: entity color (hex RGB String, e.g. {@code "#AABBCC"})</li>
 	 *   <li>columns 3..m+2: attribute variable values (String)</li>
 	 * </ul>
 	 *
 	 * @param variableId The ID of the variable for which entities and attributes should be updated.
 	 * @param df A {@link DataFrame} containing entity IDs, values, colors, and attribute values.
 	 * @param simulate If {@code true}, all changes are rolled back after execution. If {@code false}, changes are committed to the database.
 	 */
	public void setAttributes(int variableId, DataFrame df, boolean simulate) {
		sql.DataExchange.setAttributes(variableId, df, simulate);

		LogEvent l = new LogEvent(
				Logger.MESSAGE,
				"Attributes updated.",
				simulate
						? "Attribute updates were simulated."
						: "Attribute updates were committed.");
		Dna.logger.log(l);
	}

	/**
	 * Set entities and attributes for a variable with statement type ID and variable name.
	 * 
	 * <p>The supplied {@link DataFrame} must contain {@code m + 3} columns:
 	 * <ul>
 	 *   <li>column 0: entity ID (int)</li>
 	 *   <li>column 1: entity value (String)</li>
 	 *   <li>column 2: entity color (hex RGB String, e.g. {@code "#AABBCC"})</li>
 	 *   <li>columns 3..m+2: attribute variable values (String)</li>
 	 * </ul>
 	 *
 	 * @param statementTypeId The ID of the statement type in which the variable is defined.
     * @param variable The name of the variable whose entities and attributes should be updated.
 	 * @param df A {@link DataFrame} containing entity IDs, values, colors, and attribute values.
 	 * @param simulate If {@code true}, all changes are rolled back after execution. If {@code false}, changes are committed to the database.
	 */
	public void setAttributes(int statementTypeId, String variable, DataFrame df, boolean simulate) {
		sql.DataExchange.setAttributes(statementTypeId, variable, df, simulate);

		LogEvent l = new LogEvent(
				Logger.MESSAGE,
				"Attributes updated.",
				simulate
						? "Attribute updates were simulated."
						: "Attribute updates were committed.");
		Dna.logger.log(l);
	}

	/**
	 * Set entities and attributes for a variable with statement type name and variable name.
	 * 
	 * <p>The supplied {@link DataFrame} must contain {@code m + 3} columns:
 	 * <ul>
 	 *   <li>column 0: entity ID (int)</li>
 	 *   <li>column 1: entity value (String)</li>
 	 *   <li>column 2: entity color (hex RGB String, e.g. {@code "#AABBCC"})</li>
 	 *   <li>columns 3..m+2: attribute variable values (String)</li>
 	 * </ul>
 	 *
 	 * @param statementType The name of the statement type in which the variable is defined.
     * @param variable The name of the variable whose entities and attributes should be updated.
 	 * @param df A {@link DataFrame} containing entity IDs, values, colors, and attribute values.
 	 * @param simulate If {@code true}, all changes are rolled back after execution. If {@code false}, changes are committed to the database.
	 */
	public void setAttributes(String statementType, String variable, DataFrame df, boolean simulate) {
		sql.DataExchange.setAttributes(statementType, variable, df, simulate);

		LogEvent l = new LogEvent(
				Logger.MESSAGE,
				"Attributes updated.",
				simulate
						? "Attribute updates were simulated."
						: "Attribute updates were committed.");
		Dna.logger.log(l);
	}


	/* =================================================================================================================
	 * Functions for managing statements
	 * =================================================================================================================
	 */

	/**
	 * Retrieve statements for a given statement type ID, possibly filtered by statement IDs.
	 *
	 * @param statementTypeId Statement type ID.
	 * @param statementIds    Array of statement IDs to filter by. If empty, all statements of the type are returned.
	 * @return Data frame with entities and attributes, as defined in {@link sql.DataExchange#getStatements(int, int[])}.
	 */
	public DataFrame getStatements(int statementTypeId, int[] statementIds) {
		DataFrame df = sql.DataExchange.getStatements(statementTypeId, statementIds);
		if (df == null) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Statements could not be retrieved.",
					"The statements for Statement Type ID " + statementTypeId + " could not be retrieved from the database. Perhaps the ID is invalid or the database connection failed.");
			Dna.logger.log(l);
			return null;
		}
		if (df.nrow() == 0) {
			LogEvent l = new LogEvent(Logger.WARNING,
					"No statements found.",
					"The statements for Statement Type ID " + statementTypeId + " were retrieved from the database, but no statements were found. Perhaps the ID is invalid or the database is empty.");
			Dna.logger.log(l);
		} else {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Statements have been queried.",
					"The statements for Statement Type ID " + statementTypeId + " have been successfully retrieved from the database.");
			Dna.logger.log(l);
		}
		return df;
	}

	/**
	 * Retrieve statements for a given statement type label, possibly filtered by statement IDs.
	 *
	 * @param statementType	Statement type label.
	 * @param statementIds  Array of statement IDs to filter by. If empty, all statements of the type are returned.
	 * @return Data frame with entities and attributes, as defined in {@link sql.DataExchange#getStatements(String, int[])}.
	 */
	public DataFrame getStatements(String statementType, int[] statementIds) {
		DataFrame df = sql.DataExchange.getStatements(statementType, statementIds);
		if (df == null) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Statements could not be retrieved.",
					"The statements for Statement Type " + statementType + " could not be retrieved from the database. Perhaps the statement type label is invalid or the database connection failed.");
			Dna.logger.log(l);
			return null;
		}
		if (df.nrow() == 0) {
			LogEvent l = new LogEvent(Logger.WARNING,
					"No statements found.",
					"The statements for Statement Type " + statementType + " were retrieved from the database, but no statements were found. Perhaps the statement type label is invalid or the database is empty.");
			Dna.logger.log(l);
		} else {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Statements have been queried.",
					"The statements for Statement Type " + statementType + " have been successfully retrieved from the database.");
			Dna.logger.log(l);
		}
		return df;
	}

	/**
	 * Add a new statement with custom contents to the database. Entities can be null. Entity IDs will be disregarded; entities are matched with the database using their values.
	 * 
	 * @param documentId       The document ID of the document to which the statement should be added.
	 * @param startCaret       The start position of the statement in the document.
	 * @param endCaret         The stop position of the statement in the document.
	 * @param statementTypeId  The ID of the statement type of which the statement to be created is an instance.
	 * @param coderId          The ID of the coder that adds the current statement.
	 * @param varNames         The variable names to which the values should be added, in the same order as the values.
	 * @param values           The values to be added to the statement variables as an {@link Object} array.
	 * @return                 A new ID of the statement that was added.
	 */
	public int addStatement(int documentId, int startCaret, int endCaret, int statementTypeId, int coderId, String[] varNames, Object[] values) {
		int statementId = sql.DataExchange.addStatement(documentId, startCaret, endCaret, statementTypeId, coderId, varNames, values);
		if (statementId == -1) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Statement could not be added.",
					"The new statement could not be added to the database. Please check the provided parameters and the database connection.");
			Dna.logger.log(l);
		} else {
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Statement has been added.",
					"The new statement with ID " + statementId + " has been successfully added to the database.");
			Dna.logger.log(l);
		}
		return statementId;
	}
	
	/**
	 * Get the {@link Exporter} object that contains the results.
	 *
	 * @return {@link Exporter} object with results.
	 */
	public Exporter getExporter() {
		return this.exporter;
	}

	@Override
	public void processLogEvents() {
		LogEvent l = Dna.logger.getRow(Dna.logger.getRowCount() - 1);
		if (l.getPriority() == 2 || l.getPriority() == 3) {
			l.print();
		}
	}
}