package dna.export;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import dna.Dna;
import logger.LogEvent;
import logger.Logger;
import me.tongfei.progressbar.ProgressBar;
import model.*;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.jdom.Attribute;
import org.jdom.Comment;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.ojalgo.array.DenseArray;
import org.ojalgo.array.Primitive64Array;
import org.ojalgo.function.aggregator.Aggregator;
import org.ojalgo.matrix.Primitive64Matrix;
import org.ojalgo.matrix.decomposition.Eigenvalue;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Exporter class. This class contains functions for filtering statement array
 * lists, creating network matrices, and writing networks to files.
 */
public class Exporter {
	private StatementType statementType;
	private String networkType, variable1, variable2, qualifier, qualifierAggregation;
	private String normalization, duplicates, timeWindow;
	private boolean variable1Document, variable2Document, qualifierDocument, isolates;
	private LocalDateTime startDateTime, stopDateTime;
	private int windowSize;
	private HashMap<String, ArrayList<String>> excludeValues;
	private ArrayList<String> excludeAuthors, excludeSources, excludeSections, excludeTypes;
	private boolean invertValues, invertAuthors, invertSources, invertSections, invertTypes;
	private String fileFormat, outfile;

	/**
	 * Holds all documents.
	 */
	private ArrayList<TableDocument> documents;
	/**
	 * Holds a mapping of document IDs to indices in the {@link #documents} array list.
	 */
	private HashMap<Integer, Integer> docMap;
	/**
	 * Holds a mapping of statement type variable names to data types for quick lookup.
	 */
	private HashMap<String, String> dataTypes;
	/**
	 * Holds all statements.
	 */
	private ArrayList<ExportStatement> originalStatements;
	/**
	 * Holds the statements that remain after filtering by date, exclude filter, duplicates etc.
	 */
	private ArrayList<ExportStatement> filteredStatements;
	/**
	 * Holds the resulting matrices. Can have size 1.
	 */
	private ArrayList<Matrix> matrixResults;

	// common backbone algorithm objects
	private String[] fullConcepts;
	private Matrix fullMatrix;
	private ArrayList<String> currentBackboneList, currentRedundantList;
	private double[] eigenvaluesFull;

	// objects for nested backbone algorithm
	private int counter;
	private int[] iteration, numStatements;
	private String[] entity;
	private double[] backboneLoss, redundantLoss;
	ArrayList<Matrix> backboneMatrices = new ArrayList<>();
	ArrayList<Matrix> redundantMatrices = new ArrayList<>();
	private NestedBackboneResult nestedBackboneResult = null;

	// objects for simulated annealing backbone algorithm
	private ArrayList<Double> temperatureLog, acceptanceProbabilityLog, penalizedBackboneLossLog, acceptanceRatioLastHundredIterationsLog;
	private ArrayList<Integer> acceptedLog, proposedBackboneSizeLog, acceptedBackboneSizeLog, finalBackboneSizeLog;
	private String selectedAction;
	private ArrayList<String> actionList, candidateBackboneList, candidateRedundantList, finalBackboneList, finalRedundantList;
	private ArrayList<ExportStatement> currentStatementList, candidateStatementList, finalStatementList; // declare candidate statement list at t
	private Matrix currentMatrix, candidateMatrix, finalMatrix; // candidate matrix at the respective t, Y^{B^*_t}
	private boolean accept;
	private double p, temperature, acceptance, r, oldLoss, newLoss, finalLoss, log;
	private double[] eigenvaluesCurrent, eigenvaluesCandidate, eigenvaluesFinal;
	private int T, t, backboneSize;
	private SimulatedAnnealingBackboneResult simulatedAnnealingBackboneResult = null;

	// time smoothing
	/**
	 * Kernel function used for time slice network smoothing. Can be {@code "no"} (for no kernel function; uses legacy
	 * code instead); {@code "uniform"} (for uniform kernel, which is similar to "no"); {@code "epanechnikov"} (for the
	 * Epanechnikov kernel function; @code "triangular"} (for the triangular kernel function); or {@code "gaussian"}
	 * (for the Gaussian, or standard normal, kernel function).
	 */
	private String kernel = "no";
	/**
	 * For kernel-smoothed time slices, should the first mid-point be half a bandwidth or time window duration after the
	 * start date and the last mid-point be half a bandwidth or duration before the last date to allow sufficient data
	 * around the end points of the timeline?
	 */
	private boolean indentTime = true;

	public void setKernelFunction(String kernel) {
		this.kernel = kernel;
	}


	/**
	 * Set the value of the indentBandwidth field in this class. It indicates if the start and end date of the time
	 * window algorithm should be adjusted to include only networks that entirely fit into the set timeline.
	 *
	 * @param indentTime Parameter setting. Should the start and stop date and time be indented?
	 */
	public void setIndentTime(boolean indentTime) {
		this.indentTime = indentTime;
	}

	/**
	 * <p>Create a new Exporter class instance, holding an array list of export
	 * statements (i.e., statements with added document information and a hash
	 * map for easier access to variable values.
	 * 
	 * @param networkType The type of network to be exported. Valid values are:
	 *   <ul>
	 *     <li>{@code "twomode"} (to create a two-mode network)</li>
	 *     <li>{@code "onemode"} (to create a one-mode network)</li>
	 *     <li>{@code "eventlist"} (to create an event list)</li>
	 *   </ul>
	 * @param statementType The statement type.
	 * @param variable1 The name of the first variable, for example {@code
	 *   "organization"}. In addition to the variables defined in the statement
	 *   type, the document variables {@code author}, {@code source}, {@code
	 *   section}, {@code type}, {@code id}, and {@code title} are valid. If
	 *   document-level variables are used, this must be declared using the
	 *   {@code variable1Document} argument.
	 * @param variable1Document Is the first variable defined at the document
	 *   level, for instance the author or document ID?
	 * @param variable2 The name of the second variable, for example {@code
	 *   "concept"}. In addition to the variables defined in the statement type,
	 *   the document variables {@code author}, {@code source}, {@code section},
	 *   {@code type}, {@code id}, and {@code title} are valid. If
	 *   document-level variables are used, this must be declared using the
	 *   {@code variable2Document} argument.
	 * @param variable2Document Is the second variable defined at the document
	 *   level, for instance the author or document ID?
	 * @param qualifier The qualifier variable, for example {@code
	 *  "agreement"}.
	 * @param qualifierDocument Is the qualifier variable defined at the
	 *   document level, for instance the author or document ID?
	 * @param qualifierAggregation The way in which the qualifier variable is
	 *   used to aggregate ties in the network.<br/>
	 *   Valid values if the {@code networkType} argument equals {@code
	 *   "onemode"} are:
	 *   <ul>
	 *     <li>{@code "ignore"} (for ignoring the qualifier variable)</li>
	 *     <li>{@code "congruence"} (for recording a network tie only if both
	 *       nodes have the same qualifier value in the binary case or for
	 *       recording the similarity between the two nodes on the qualifier
	 *       variable in the integer case)</li>
	 *     <li>{@code "conflict"} (for recording a network tie only if both
	 *       nodes have a different qualifier value in the binary case or for
	 *       recording the distance between the two nodes on the qualifier
	 *       variable in the integer case)</li>
	 *     <li>{@code "subtract"} (for subtracting the conflict tie value from
	 *       the congruence tie value in each dyad)</li>
	 *     <li>{@code "congruence & conflict"} (only applicable to time window
	 *       networks: add both a congruence and a conflict network to the time
	 *       window list of networks at each time step)</li>
	 *   </ul>
	 *   Valid values if the {@code networkType} argument equals {@code
	 *   "twomode"} are:
	 *   <ul>
	 *     <li>{@code "ignore"} (for ignoring the qualifier variable)</li>
	 *     <li>{@code "combine"} (for creating multiplex combinations, e.g.,
	 *       {@code 1} for positive, {@code 2} for negative, and {@code 3} for
	 *       mixed)</li>
	 *     <li>{@code "subtract"} (for subtracting negative from positive
	 *       ties)</li>
	 *   </ul>
	 *   The argument is ignored if {@code networkType} equals {@code
	 *   "eventlist"}.
	 * @param normalization Normalization of edge weights. Valid settings for
	 *   <em>one-mode</em> networks are:
     *   <ul>
	 *     <li>{@code "no"} (for switching off normalization)</li>
	 *     <li>{@code "average"} (for average activity normalization)</li>
	 *     <li>{@code "jaccard"} (for Jaccard coefficient normalization)</li>
	 *     <li>{@code "cosine"} (for cosine similarity normalization)</li>
	 *   </ul>
	 *   Valid settings for <em>two-mode</em> networks are:
	 *   <ul>
	 *     <li>{@code "no"} (for switching off normalization)</li>
	 *     <li>{@code "activity"} (for activity normalization)</li>
	 *     <li>{@code "prominence"} (for prominence normalization)</li>
	 *   </ul>
	 * @param isolates Should all nodes of the respective variable be included
	 *    in the network matrix ({@code true}), or should only those nodes be
	 *    included that are active in the current time period and are not
	 *    excluded ({@code false})?
	 * @param duplicates Setting for excluding duplicate statements before
	 *   network construction. Valid values are:
	 *   <ul>
	 *     <li>{@code "include"} (for including all statements in network
	 *       construction)</li>
	 *     <li>{@code "document"} (for counting only one identical statement per
	 *       document)</li>
	 *     <li>{@code "week"} (for counting only one identical statement per
	 *       calendar week as defined in the UK locale, i.e., Monday to Sunday)
	 *       </li>
	 *     <li>{@code "month"} (for counting only one identical statement per
	 *       calendar month)</li>
	 *     <li>{@code "year"} (for counting only one identical statement per
	 *       calendar year)</li>
	 *     <li>{@code "acrossrange"} (for counting only one identical statement
	 *       across the whole time range)</li>
	 *   </ul>
	 * @param startDateTime The start date and time for network construction.
	 *   All statements before this specified date/time will be excluded.
	 * @param stopDateTime The stop date and time for network construction.
	 *   All statements after this specified date/time will be excluded.
	 * @param timeWindow If any of the time units is selected, a moving time
	 *    window will be imposed, and only the statements falling within the
	 *    time period defined by the window will be used to create the network.
	 *    The time window will then be moved forward by one time unit at a time,
	 *    and a new network with the new time boundaries will be created. This
	 *    is repeated until the end of the overall time span is reached. All
	 *    time windows will be saved as separate network matrices in a list. The
	 *    duration of each time window is defined by the {@code windowsize}
	 *    argument. For example, this could be used to create a time window of
	 *    six months that moves forward by one month each time, thus creating
	 *    time windows that overlap by five months. If {@code "events"} is used
	 *    instead of a natural time unit, the time window will comprise exactly
	 *    as many statements as defined in the {@code windowsize} argument.
	 *    However, if the start or end statement falls on a date and time where
	 *    multiple events happen, those additional events that occur
	 *    simultaneously are included because there is no other way to decide
	 *    which of the statements should be selected. Therefore the window size
	 *    is sometimes extended when the start or end point of a time window is
	 *    ambiguous in event time. Valid argument values are:
	 *   <ul>
	 *     <li>{@code "no"} (no time window will be used)</li>
	 *     <li>{@code "events"} (time window length = number of statements)</li>
	 *     <li>{@code "seconds"} (number of seconds)</li>
	 *     <li>{@code "minutes"} (number of minutes)</li>
	 *     <li>{@code "hours"} (number of hours)</li>
	 *     <li>{@code "days"} (number of days)</li>
	 *     <li>{@code "weeks"} (number of calendar weeks)</li>
	 *     <li>{@code "months"} (number of calendar months)</li>
	 *     <li>{@code "years"} (number of calendar years)</li>
	 *   </ul>
	 *   @param windowSize The number of time units of which a moving time
	 *     window is comprised. This can be the number of statement events, the
	 *     number of days etc., as defined in the {@code timeWindow} argument.
	 *   @param excludeValues A hash map that contains values which should be
	 *     excluded during network construction. The hash map is indexed by
	 *     variable name (for example, {@code "organization"} as the key, and
	 *     the corresponding value is an array list of values to exclude, for
	 *     example {@code "org A"} or {@code "org B"}. This is irrespective of
	 *     whether these values appear in {@code variable1}, {@code variable2},
	 *     or the {@code qualifier} variable. Note that only variables at the
	 *     statement level can be used here. There are separate arguments for
	 *     excluding statements nested in documents with certain meta-data.
	 *   @param excludeAuthors An array of authors. If a statement is nested in
	 *     a document where one of these authors is set in the {@code author}
	 *     meta-data field, the statement is excluded from network construction.
	 *   @param excludeSources An array of sources. If a statement is nested in
	 *     a document where one of these sources is set in the {@code source}
	 *     meta-data field, the statement is excluded from network construction.
	 *   @param excludeSections An array of sections. If a statement is nested
	 *     in a document where one of these sections is set in the {@code
	 *     section} meta-data field, the statement is excluded from network
	 *     construction.
	 *   @param excludeTypes An array of types. If a statement is nested in a
	 *     document where one of these types is set in the {@code type}
	 *     meta-data field, the statement is excluded from network construction.
	 *   @param invertValues Indicates whether the entries provided by the
	 *     {@code excludeValues} argument should be excluded from network
	 *     construction ({@code false}) or if they should be the only values
	 *     that should be included during network construction ({@code true}).
	 *   @param invertAuthors Indicates whether the values provided by the
	 *     {@code excludeAuthors} argument should be excluded from network
	 *     construction ({@code false}) or if they should be the only values
	 *     that should be included during network construction ({@code true}).
	 *   @param invertSources Indicates whether the values provided by the
	 *     {@code excludeSources} argument should be excluded from network
	 *     construction ({@code false}) or if they should be the only values
	 *     that should be included during network construction ({@code true}).
	 *   @param invertSections Indicates whether the values provided by the
	 *     {@code excludeSections} argument should be excluded from network
	 *     construction ({@code false}) or if they should be the only values
	 *     that should be included during network construction ({@code true}).
	 *   @param invertTypes Indicates whether the values provided by the
	 *     {@code excludeTypes} argument should be excluded from network
	 *     construction ({@code false}) or if they should be the only values
	 *     that should be included during network construction ({@code true}).
	 *   @param fileFormat The file format specification for saving the
	 *     resulting network(s) to a file instead of returning an object. Valid
	 *     values are:
	 *     <ul>
	 *       <li>{@code "csv"} (for network matrices or event lists)</li>
	 *       <li>{@code "dl"} (for UCINET DL full-matrix files)</li>
	 *       <li>{@code "graphml"} (for visone {@code .graphml} files; this
	 *         specification is also compatible with time windows)</li>
	 *     </ul>
	 *   @param outfile The file name for saving the network.
	 */
	public Exporter(
			String networkType,
			StatementType statementType,
			String variable1,
			boolean variable1Document,
			String variable2,
			boolean variable2Document,
			String qualifier,
			boolean qualifierDocument,
			String qualifierAggregation,
			String normalization,
			boolean isolates,
			String duplicates,
			LocalDateTime startDateTime,
			LocalDateTime stopDateTime,
			String timeWindow,
			int windowSize,
			HashMap<String, ArrayList<String>> excludeValues,
			ArrayList<String> excludeAuthors,
			ArrayList<String> excludeSources,
			ArrayList<String> excludeSections,
			ArrayList<String> excludeTypes,
			boolean invertValues,
			boolean invertAuthors,
			boolean invertSources,
			boolean invertSections,
			boolean invertTypes,
			String fileFormat,
			String outfile) {

		// create a list of document variables for easier if-condition checking below
		ArrayList<String> documentVariables = new ArrayList<String>();
		documentVariables.add("author");
		documentVariables.add("source");
		documentVariables.add("section");
		documentVariables.add("type");
		documentVariables.add("id");
		documentVariables.add("title");

		// check network type
		// valid input: 'eventlist', 'twomode', or 'onemode'
		this.networkType = networkType;
		this.networkType = this.networkType.toLowerCase();
		if (!this.networkType.equals("eventlist") && !this.networkType.equals("twomode") && !this.networkType.equals("onemode")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Network type setting invalid.",
					"When exporting a network, the network type was set to be \"" + networkType + "\", but the only valid options are \"onemode\", \"twomode\", and \"eventlist\". Using the default value \"twomode\" in this case.");
			Dna.logger.log(le);
			this.networkType = "twomode";
		}

		// check statement type
		this.statementType = statementType;
		ArrayList<String> shortTextVariables = Stream.of(this.statementType.getVariablesList(false, true, false, false)).collect(Collectors.toCollection(ArrayList::new));
		if (shortTextVariables.size() < 2) {
			LogEvent le = new LogEvent(Logger.ERROR,
					"Exporter: Statement type contains fewer than two short text variables.",
					"When exporting a network, the statement type \"" + this.statementType.getLabel() + "\" (ID: " + this.statementType.getId() + ") was selected, but this statement type contains fewer than two short text variables. At least two short text variables are required for network construction.");
			Dna.logger.log(le);
		}

		// check variable1, variable1Document, variable2, and variable2Document
		this.variable1Document = variable1Document;
		if (this.variable1Document && !documentVariables.contains(variable1)) {
			this.variable1Document = false;
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Variable 1 is not a document-level variable.",
					"When exporting a network, Variable 1 was set to be a document-level variable, but \"" + variable1 + "\" does not exist as a document-level variable. Trying to interpret it as a statement-level variable instead.");
			Dna.logger.log(le);
		}

		this.variable2Document = variable2Document;
		if (this.variable2Document && !documentVariables.contains(variable2)) {
			this.variable2Document = false;
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Variable 2 is not a document-level variable.",
					"When exporting a network, Variable 2 was set to be a document-level variable, but \"" + variable2 + "\" does not exist as a document-level variable. Trying to interpret it as a statement-level variable instead.");
			Dna.logger.log(le);
		}

		this.variable1 = variable1;
		this.variable2 = variable2;
		if (!variable1Document && !shortTextVariables.contains(this.variable1)) {
			String var1 = this.variable1;
			int counter = 0;
			while (var1.equals(this.variable1) || var1.equals(this.variable2)) {
				var1 = shortTextVariables.get(counter);
				counter++;
			}
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Variable 1 does not exist in statement type.",
					"When exporting a network, Variable 1 was set to be \"" + this.variable1 + "\", but this variable is undefined in the statement type \"" + this.statementType + "\" or is not a short text variable. Using variable \"" + var1 + "\" instead.");
			Dna.logger.log(le);
			this.variable1 = var1;
		}
		if (!variable2Document && !shortTextVariables.contains(this.variable2)) {
			String var2 = this.variable2;
			int counter = 0;
			while (var2.equals(this.variable1) || var2.equals(this.variable2)) {
				var2 = shortTextVariables.get(counter);
				counter++;
			}
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Variable 2 does not exist in statement type.",
					"When exporting a network, Variable 2 was set to be \"" + this.variable2 + "\", but this variable is undefined in the statement type \"" + this.statementType + "\" or is not a short text variable. Using variable \"" + var2 + "\" instead.");
			Dna.logger.log(le);
			this.variable2 = var2;
		}
		if (this.variable1.equals(this.variable2)) {
			String var2 = this.variable2;
			int counter = 0;
			while (var2.equals(this.variable1)) {
				var2 = shortTextVariables.get(counter);
				counter++;
			}
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Variables 1 and 2 are identical.",
					"When exporting a network, Variable 1 and Variable 2 were identical (\"" + this.variable1 + "\"). Changing Variable 2 to \"" + var2 + "\" instead.");
			Dna.logger.log(le);
			this.variable2 = var2;
		}

		// check qualifier, qualifierDocument, and qualifierAggregation
		this.qualifierDocument = qualifierDocument;
		if (qualifier == null && this.qualifierDocument) {
			this.qualifierDocument = false;
		} else if (qualifier != null && this.qualifierDocument && !documentVariables.contains(qualifier)) {
			this.qualifierDocument = false;
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Qualifier variable is not a document-level variable.",
					"When exporting a network, the qualifier variable was set to be a document-level variable, but \"" + qualifier + "\" does not exist as a document-level variable. Trying to interpret it as a statement-level variable instead.");
			Dna.logger.log(le);
		}

		this.qualifierAggregation = qualifierAggregation.toLowerCase();
		this.qualifier = qualifier;
		ArrayList<String> variables = Stream.of(this.statementType.getVariablesList(false, true, true, true)).collect(Collectors.toCollection(ArrayList::new));
		if (this.qualifier != null && !this.qualifierDocument && (!variables.contains(this.qualifier) || this.qualifier.equals(this.variable1) || this.qualifier.equals(this.variable2))) {
			this.qualifier = null;
			if (!this.qualifierAggregation.equals("ignore")) {
				this.qualifierAggregation = "ignore";
				LogEvent le = new LogEvent(Logger.WARNING,
						"Exporter: Qualifier variable undefined or invalid.",
						"When exporting a network, the qualifier variable was either not defined as a variable in the statement type \"" + this.statementType.getLabel() + "\" or was set to be identical to Variable 1 or Variable 2. Hence, no qualifier is used.");
				Dna.logger.log(le);
			}
		}

		if (!this.qualifierAggregation.equals("ignore") &&
				!this.qualifierAggregation.equals("subtract") &&
				!this.qualifierAggregation.equals("combine") &&
				!this.qualifierAggregation.equals("congruence") &&
				!this.qualifierAggregation.equals("conflict") &&
				!(this.qualifierAggregation.equals("congruence & conflict") && timeWindow.equals("events"))) {
			this.qualifierAggregation = "ignore";
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Qualifier aggregation setting invalid.",
					"When exporting a network, the qualifier aggregation setting was \"" + qualifierAggregation + "\". The only valid settings are \"ignore\", \"combine\", \"congruence\", and \"conflict\", depending on other settings. Using \"ignore\" now.");
			Dna.logger.log(le);
		}
		if (this.qualifierAggregation.equals("combine") && !this.networkType.equals("twomode")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Qualifier aggregation incompatible with network type.",
					"When exporting a network, the qualifier aggregation setting was \"combine\", but this setting is only compatible with two-mode networks. Using \"ignore\" now.");
			Dna.logger.log(le);
			this.qualifierAggregation = "ignore";
		}
		if ((this.qualifierAggregation.equals("congruence") || this.qualifierAggregation.equals("conflict")) && !this.networkType.equals("onemode")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Qualifier aggregation incompatible with network type.",
					"When exporting a network, the qualifier aggregation setting was \"" + this.qualifierAggregation + "\", but this setting is only compatible with one-mode networks. Using \"ignore\" now.");
			Dna.logger.log(le);
			this.qualifierAggregation = "ignore";
		}
		if (this.qualifier == null && !this.qualifierAggregation.equals("ignore")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Qualifier aggregation incompatible with qualifier variable.",
					"When exporting a network, the qualifier aggregation setting was \"" + this.qualifierAggregation + "\", but no qualifier variable was selected. Using \"ignore\" now.");
			Dna.logger.log(le);
			this.qualifierAggregation = "ignore";
		}

		// check normalization (valid values: 'no', 'activity', 'prominence', 'average', 'jaccard', or 'cosine')
		this.normalization = normalization.toLowerCase();
		if (!this.normalization.equals("no") &&
				!this.normalization.equals("activity") &&
				!this.normalization.equals("prominence") &&
				!this.normalization.equals("average") &&
				!this.normalization.equals("jaccard") &&
				!this.normalization.equals("cosine")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Normalization setting invalid.",
					"When exporting a network, normalization was set to \"" + normalization + "\", which is invalid. The only valid values are \"no\", \"activity\", \"prominence\", \"average\", \"jaccard\", and \"cosine\". Using the default value \"no\" in this case.");
			Dna.logger.log(le);
			this.normalization = "no";
		}
		if ((this.normalization.equals("activity") || this.normalization.equals("prominence")) && !this.networkType.equals("twomode")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Normalization setting invalid.",
					"When exporting a network, normalization was set to \"" + normalization + "\", which is only possible with two-mode networks. Using the default value \"no\" in this case.");
			Dna.logger.log(le);
			this.normalization = "no";
		}
		if ((this.normalization.equals("average") || this.normalization.equals("jaccard") || this.normalization.equals("cosine")) && !this.networkType.equals("onemode")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Normalization setting invalid.",
					"When exporting a network, normalization was set to \"" + normalization + "\", which is only possible with one-mode networks. Using the default value \"no\" in this case.");
			Dna.logger.log(le);
			this.normalization = "no";
		}

		// isolates setting
		this.isolates = isolates;

		// check duplicates setting (valid settings: 'include', 'document', 'week', 'month', 'year', or 'acrossrange')
		this.duplicates = duplicates.toLowerCase();
		if (!this.duplicates.equals("include") &&
				!this.duplicates.equals("document") &&
				!this.duplicates.equals("week") &&
				!this.duplicates.equals("month") &&
				!this.duplicates.equals("year") &&
				!this.duplicates.equals("acrossrange")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Duplicates setting invalid.",
					"When exporting a network, the duplicates setting was \"" + duplicates + "\", which is invalid. The only valid values are \"include\", \"document\", \"week\", \"month\", \"year\", and \"acrossrange\". Using the default value \"include\" in this case.");
			Dna.logger.log(le);
			this.duplicates = "include";
		}

		// check time window arguments
		this.timeWindow = timeWindow;
		if (this.timeWindow == null) {
			this.timeWindow = "no";
		} else if (!this.timeWindow.equals("no") &&
				!this.timeWindow.equals("seconds") &&
				!this.timeWindow.equals("minutes") &&
				!this.timeWindow.equals("hours") &&
				!this.timeWindow.equals("days") &&
				!this.timeWindow.equals("weeks") &&
				!this.timeWindow.equals("months") &&
				!this.timeWindow.equals("years") &&
				!this.timeWindow.equals("events")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Time window setting invalid.",
					"When exporting a network, the time window setting was \"" + this.timeWindow + "\", which is invalid. The only valid values are \"no\", \"seconds\", \"minutes\", \"hours\", \"days\", \"weeks\", \"months\", \"years\", and \"events\". Using the default value \"no\" in this case.");
			Dna.logger.log(le);
			this.timeWindow = "no";
		}
		this.windowSize = windowSize;
		if (this.windowSize < 1 && !this.timeWindow.equals("no")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Time window size invalid.",
					"When exporting a network, the time window size was " + this.windowSize + ", which is invalid in combination with a time window setting other than \"no\". Using the minimum value of 1 in this case.");
			Dna.logger.log(le);
			this.windowSize = 1;
		}

		// check file export format and file name arguments
		if (fileFormat != null) {
			this.fileFormat = fileFormat.toLowerCase();
			if (!this.fileFormat.equals("csv") && !this.fileFormat.equals("dl") && !this.fileFormat.equals("graphml")) {
				LogEvent le = new LogEvent(Logger.WARNING,
						"Exporter: File format invalid.",
						"When exporting a network, the file format setting was " + this.fileFormat + ", but \"csv\", \"dl\", and \"graphml\" are the only valid settings. Using \"graphml\" in this case.");
				Dna.logger.log(le);
				this.fileFormat = "graphml";
			}
		} else {
			this.fileFormat = null;
		}
		this.outfile = outfile;
		if (this.outfile != null) {
			if (this.fileFormat.equals("graphml") && !this.outfile.toLowerCase().endsWith(".graphml")) {
				this.outfile = this.outfile + ".graphml";
			} else if (this.fileFormat.equals("csv") && !this.outfile.toLowerCase().endsWith(".csv")) {
				this.outfile = this.outfile + ".csv";
			} else if (this.fileFormat.equals("dl") && !this.outfile.toLowerCase().endsWith(".dl")) {
				this.outfile = this.outfile + ".dl";
			}
		}

		// remaining arguments
		this.startDateTime = startDateTime;
		this.stopDateTime = stopDateTime;
		this.excludeValues = excludeValues;
		this.invertValues = invertValues;
		this.excludeAuthors = excludeAuthors;
		this.invertAuthors = invertAuthors;
		this.excludeSources = excludeSources;
		this.invertSources = invertSources;
		this.excludeSections = excludeSections;
		this.invertSections = invertSections;
		this.excludeTypes = excludeTypes;
		this.invertTypes = invertTypes;
	}

	/**
	 * Constructor with reduced information for generating barplot data. A variable is specified for which frequency
	 * counts by qualifier level are computed.
	 *
	 * @param statementType The statement type.
	 * @param variable1 The name of the first variable, for example {@code
	 *   "organization"}. In addition to the variables defined in the statement
	 *   type, the document variables {@code author}, {@code source}, {@code
	 *   section}, {@code type}, {@code id}, and {@code title} are valid. If
	 *   document-level variables are used, this must be declared using the
	 *   {@code variable1Document} argument.
	 * @param qualifier The qualifier variable, for example {@code
	 *  "agreement"}.
	 * @param duplicates Setting for excluding duplicate statements before
	 *   network construction. Valid values are:
	 *   <ul>
	 *     <li>{@code "include"} (for including all statements in network
	 *       construction)</li>
	 *     <li>{@code "document"} (for counting only one identical statement per
	 *       document)</li>
	 *     <li>{@code "week"} (for counting only one identical statement per
	 *       calendar week as defined in the UK locale, i.e., Monday to Sunday)
	 *       </li>
	 *     <li>{@code "month"} (for counting only one identical statement per
	 *       calendar month)</li>
	 *     <li>{@code "year"} (for counting only one identical statement per
	 *       calendar year)</li>
	 *     <li>{@code "acrossrange"} (for counting only one identical statement
	 *       across the whole time range)</li>
	 *   </ul>
	 * @param startDateTime The start date and time for network construction.
	 *   All statements before this specified date/time will be excluded.
	 * @param stopDateTime The stop date and time for network construction.
	 *   All statements after this specified date/time will be excluded.
	 * @param excludeValues A hash map that contains values which should be
	 *   excluded during network construction. The hash map is indexed by
	 *   variable name (for example, {@code "organization"} as the key, and
	 *   the corresponding value is an array list of values to exclude, for
	 *   example {@code "org A"} or {@code "org B"}. This is irrespective of
	 *   whether these values appear in {@code variable1}, {@code variable2},
	 *   or the {@code qualifier} variable. Note that only variables at the
	 *   statement level can be used here. There are separate arguments for
	 *   excluding statements nested in documents with certain meta-data.
	 * @param excludeAuthors An array of authors. If a statement is nested in
	 *   a document where one of these authors is set in the {@code author}
	 *   meta-data field, the statement is excluded from network construction.
	 * @param excludeSources An array of sources. If a statement is nested in
	 *   a document where one of these sources is set in the {@code source}
	 *   meta-data field, the statement is excluded from network construction.
	 * @param excludeSections An array of sections. If a statement is nested
	 *   in a document where one of these sections is set in the {@code
	 *   section} meta-data field, the statement is excluded from network
	 *   construction.
	 * @param excludeTypes An array of types. If a statement is nested in a
	 *   document where one of these types is set in the {@code type}
	 *   meta-data field, the statement is excluded from network construction.
	 * @param invertValues Indicates whether the entries provided by the
	 *   {@code excludeValues} argument should be excluded from network
	 *   construction ({@code false}) or if they should be the only values
	 *   that should be included during network construction ({@code true}).
	 * @param invertAuthors Indicates whether the values provided by the
	 *   {@code excludeAuthors} argument should be excluded from network
	 *   construction ({@code false}) or if they should be the only values
	 *   that should be included during network construction ({@code true}).
	 * @param invertSources Indicates whether the values provided by the
	 *   {@code excludeSources} argument should be excluded from network
	 *   construction ({@code false}) or if they should be the only values
	 *   that should be included during network construction ({@code true}).
	 * @param invertSections Indicates whether the values provided by the
	 *   {@code excludeSections} argument should be excluded from network
	 *   construction ({@code false}) or if they should be the only values
	 *   that should be included during network construction ({@code true}).
	 * @param invertTypes Indicates whether the values provided by the
	 *   {@code excludeTypes} argument should be excluded from network
	 *   construction ({@code false}) or if they should be the only values
	 *   that should be included during network construction ({@code true}).
	 */
	public Exporter(
			StatementType statementType,
			String variable1,
			String qualifier,
			String duplicates,
			LocalDateTime startDateTime,
			LocalDateTime stopDateTime,
			HashMap<String, ArrayList<String>> excludeValues,
			ArrayList<String> excludeAuthors,
			ArrayList<String> excludeSources,
			ArrayList<String> excludeSections,
			ArrayList<String> excludeTypes,
			boolean invertValues,
			boolean invertAuthors,
			boolean invertSources,
			boolean invertSections,
			boolean invertTypes) {

		this.statementType = statementType;
		ArrayList<String> shortTextVariables = Stream.of(this.statementType.getVariablesList(false, true, false, false)).collect(Collectors.toCollection(ArrayList::new));

		// check variable1, variable1Document, variable2, and variable2Document
		this.variable1 = variable1;
		this.variable1Document = false;
		if (!shortTextVariables.contains(this.variable1)) {
			String var1 = this.variable1;
			int counter = 0;
			while (var1.equals(this.variable1)) {
				var1 = shortTextVariables.get(counter);
				counter++;
			}
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Variable does not exist in statement type.",
					"When generating barplot data, the variable was set to be \"" + this.variable1 + "\", but this variable is undefined in the statement type \"" + this.statementType + "\" or is not a short text variable. Using variable \"" + var1 + "\" instead.");
			Dna.logger.log(le);
			this.variable1 = var1;
		}

		// check qualifier, qualifierDocument, and qualifierAggregation
		this.qualifierDocument = false;
		this.qualifierAggregation = "ignore";
		if (qualifier != null) {
			this.qualifier = qualifier;
			this.qualifierAggregation = "combine";
		}
		ArrayList<String> variables = Stream.of(this.statementType.getVariablesList(false, true, true, true)).collect(Collectors.toCollection(ArrayList::new));
		if (this.qualifier != null && (!variables.contains(this.qualifier) || this.qualifier.equals(this.variable1))) {
			this.qualifier = null;
			this.qualifierAggregation = "ignore";
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Qualifier variable undefined or invalid.",
					"When generating barplot data, the qualifier variable was either not defined as a variable in the statement type \"" + this.statementType.getLabel() + "\" or was set to be identical to the barplot variable. Hence, no qualifier is used.");
			Dna.logger.log(le);
		}

		// check duplicates setting (valid settings: 'include', 'document', 'week', 'month', 'year', or 'acrossrange')
		this.duplicates = duplicates.toLowerCase();
		if (!this.duplicates.equals("include") &&
				!this.duplicates.equals("document") &&
				!this.duplicates.equals("week") &&
				!this.duplicates.equals("month") &&
				!this.duplicates.equals("year") &&
				!this.duplicates.equals("acrossrange")) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Exporter: Duplicates setting invalid.",
					"When generating barplot data, the duplicates setting was \"" + duplicates + "\", which is invalid. The only valid values are \"include\", \"document\", \"week\", \"month\", \"year\", and \"acrossrange\". Using the default value \"include\" in this case.");
			Dna.logger.log(le);
			this.duplicates = "include";
		}

		// remaining arguments
		this.startDateTime = startDateTime;
		this.stopDateTime = stopDateTime;
		this.excludeValues = excludeValues;
		this.invertValues = invertValues;
		this.excludeAuthors = excludeAuthors;
		this.invertAuthors = invertAuthors;
		this.excludeSources = excludeSources;
		this.invertSources = invertSources;
		this.excludeSections = excludeSections;
		this.invertSections = invertSections;
		this.excludeTypes = excludeTypes;
		this.invertTypes = invertTypes;
	}

	/**
	 * Load statements and documents from the database and pre-process them.
	 */
	public void loadData() {
		// put variable data types into a map for quick lookup
		this.dataTypes = new HashMap<String, String>();
		for (int i = 0; i < this.statementType.getVariables().size(); i++) {
			this.dataTypes.put(this.statementType.getVariables().get(i).getKey(), this.statementType.getVariables().get(i).getDataType());
		}

		// get documents and create document hash map for quick lookup
		this.documents = Dna.sql.getTableDocuments(new int[0]);
		Collections.sort(documents);
		this.docMap = new HashMap<Integer, Integer>();
		for (int i = 0; i < documents.size(); i++) {
			docMap.put(documents.get(i).getId(), i);
		}

		// get statements and convert to {@link ExportStatement} objects with additional information
		this.originalStatements = Dna.sql.getStatements(new int[0],
				this.statementType.getId(),
				this.startDateTime,
				this.stopDateTime,
				this.excludeAuthors,
				this.invertAuthors,
				this.excludeSources,
				this.invertSources,
				this.excludeSections,
				this.invertSections,
				this.excludeTypes,
				this.invertTypes)
		.stream()
		.map(s -> {
			int docIndex = docMap.get(s.getDocumentId());
			return new ExportStatement(s,
					documents.get(docIndex).getTitle(),
					documents.get(docIndex).getAuthor(),
					documents.get(docIndex).getSource(),
					documents.get(docIndex).getSection(),
					documents.get(docIndex).getType());
		})
		.collect(Collectors.toCollection(ArrayList::new));
		if (this.originalStatements.size() == 0) {
			Dna.logger.log(
					new LogEvent(Logger.WARNING,
							"No statements found.",
							"When processing data for export, no statements were found in the database in the time period under scrutiny and given any document-level exclusion filters.")
			);
		}
	}
	
	/**
	 * Extract the labels for all nodes for a variable from the statements,
	 * conditional on isolates settings.
	 * 
	 * @param processedStatements These are usually filtered statements, but
	 *   could be more processed than just filtered, for example for
	 *   constructing time window sequences of network matrices.
	 * @param variable String indicating the variable for which labels should be
	 *   extracted, for example {@code "organization"}.
	 * @param variableDocument Is the variable a document-level variable?
	 * @return String array containing all sorted node names.
	 */
	String[] extractLabels(
			ArrayList<ExportStatement> processedStatements,
			String variable,
			boolean variableDocument) {
		
		// decide whether to use the original statements or the filtered statements
		ArrayList<ExportStatement> finalStatements;
		if (this.isolates) {
			finalStatements = originalStatements;
		} else {
			finalStatements = processedStatements;
		}
		
		// go through statements and extract names
		ArrayList<String> names = new ArrayList<String>();
		String n = null;
		ExportStatement es;
		for (int i = 0; i < finalStatements.size(); i++) {
			es = finalStatements.get(i);
			if (variableDocument) {
				if (variable.equals("author")) {
					n = es.getAuthor();
				} else if (variable.equals("source")) {
					n = es.getSource();
				} else if (variable.equals("section")) {
					n = es.getSection();
				} else if (variable.equals("type")) {
					n = es.getType();
				} else if (variable.equals("id")) {
					n = es.getDocumentIdAsString();
				} else if (variable.equals("title")) {
					n = es.getTitle();
				}
			} else {
				n = (String) es.get(variable).toString();
			}
			if (!names.contains(n)) {
				names.add(n);
			}
		}
		
		// sort and convert to array, then return
		Collections.sort(names);
		if (names.size() > 0 && names.get(0).equals("")) { // remove empty field
			names.remove(0);
		}
		String[] nameArray = new String[names.size()];
		if (names.size() > 0) {
			for (int i = 0; i < names.size(); i++) {
				nameArray[i] = names.get(i);
			}
		}
		return nameArray;
	}

	/**
	 * Filter the statements based on the {@link #originalStatements} slot of
	 * the class and create a filtered statement list, which is saved in the
	 * {@link #filteredStatements} slot of the class. 
	 */
	public void filterStatements() {
		try (ProgressBar pb = new ProgressBar("Filtering statements", this.originalStatements.size())) {
			pb.stepTo(0);

			// create a deep copy of the original statements
			this.filteredStatements = new ArrayList<ExportStatement>();
			for (int i = 0; i < this.originalStatements.size(); i++) {
				this.filteredStatements.add(new ExportStatement(this.originalStatements.get(i)));
			}

			// sort statements by date and time
			Collections.sort(this.filteredStatements);

			// Create arrays with variable values
			String[] values1 = retrieveValues(this.filteredStatements, this.variable1, this.variable1Document);
			String[] values2 = new String[0];
			if (this.variable2 != null) {
				values2 = retrieveValues(this.filteredStatements, this.variable2, this.variable2Document);
			}
			String[] qualifierValues = new String[0];
			if (this.qualifierDocument || (!this.qualifierAggregation.equals("ignore") && dataTypes.get(this.qualifier).equals("short text"))) {
				qualifierValues = retrieveValues(this.filteredStatements, this.qualifier, this.qualifierDocument);
			}

			// process and exclude statements
			ExportStatement s;
			ArrayList<ExportStatement> al = new ArrayList<ExportStatement>();
			String previousVar1 = null;
			String previousVar2 = null;
			String previousQualifier = null;
			LocalDateTime cal, calPrevious;
			int year, month, week, yearPrevious, monthPrevious, weekPrevious;
			for (int i = 0; i < this.filteredStatements.size(); i++) {
				boolean select = true;
				s = this.filteredStatements.get(i);

				// check against excluded values
				Iterator<String> keyIterator = this.excludeValues.keySet().iterator();
				while (keyIterator.hasNext()) {
					String key = keyIterator.next();
					String string = "";
					if (dataTypes.get(key) == null) {
						throw new NullPointerException("'" + key + "' is not a statement-level variable and cannot be excluded.");
					} else if (dataTypes.get(key).equals("boolean") || dataTypes.get(key).equals("integer")) {
						string = String.valueOf(s.get(key));
					} else if (dataTypes.get(key).equals("short text")) {
						string = ((Entity) s.get(key)).getValue();
					} else if (dataTypes.get(key).equals("long text")) {
						string = (String) s.get(key);
					}
					if ((this.excludeValues.get(key).contains(string) && !this.invertValues) ||
							(!this.excludeValues.get(key).contains(string) && this.invertValues)) {
						select = false;
					}
				}

				// check against empty fields
				if (select &&
						this.networkType != null &&
						!this.networkType.equals("eventlist") &&
						(values1[i].equals("") || values2[i].equals("") || (!this.qualifierAggregation.equals("ignore") && (qualifierDocument || dataTypes.get(qualifier).equals("short text")) && qualifierValues[i].equals("")))) {
					select = false;
				} else if (select && this.networkType == null && values1[i].equals("")) { // barplot data because no network type defined
					select = false;
				}

				// check for duplicates
				cal = s.getDateTime();
				year = cal.getYear();
				month = cal.getMonthValue();
				@SuppressWarnings("static-access")
				WeekFields weekFields = WeekFields.of(Locale.UK.getDefault()); // use UK definition of calendar weeks
				week = cal.get(weekFields.weekOfWeekBasedYear());
				if (!this.duplicates.equals("include")) {
					for (int j = al.size() - 1; j >= 0; j--) {
						if (!this.variable1Document) {
							previousVar1 = ((Entity) al.get(j).get(this.variable1)).getValue();
						} else if (this.variable1.equals("author")) {
							previousVar1 = al.get(j).getAuthor();
						} else if (this.variable1.equals("source")) {
							previousVar1 = al.get(j).getSource();
						} else if (this.variable1.equals("section")) {
							previousVar1 = al.get(j).getSection();
						} else if (this.variable1.equals("type")) {
							previousVar1 = al.get(j).getType();
						} else if (this.variable1.equals("id")) {
							previousVar1 = al.get(j).getDocumentIdAsString();
						} else if (this.variable1.equals("title")) {
							previousVar1 = al.get(j).getTitle();
						}
						if (this.variable2 != null) {
							if (!this.variable2Document) {
								previousVar2 = ((Entity) al.get(j).get(this.variable2)).getValue();
							} else if (this.variable2.equals("author")) {
								previousVar2 = al.get(j).getAuthor();
							} else if (this.variable2.equals("source")) {
								previousVar2 = al.get(j).getSource();
							} else if (this.variable2.equals("section")) {
								previousVar2 = al.get(j).getSection();
							} else if (this.variable2.equals("type")) {
								previousVar2 = al.get(j).getType();
							} else if (this.variable2.equals("id")) {
								previousVar2 = al.get(j).getDocumentIdAsString();
							} else if (this.variable2.equals("title")) {
								previousVar2 = al.get(j).getTitle();
							}
						}
						if (!this.qualifierAggregation.equals("ignore") && (qualifierDocument || dataTypes.get(this.qualifier).equals("short text"))) {
							if (!this.qualifierDocument) {
								previousQualifier = ((Entity) al.get(j).get(this.qualifier)).getValue();
							} else if (this.qualifier.equals("author")) {
								previousQualifier = al.get(j).getAuthor();
							} else if (this.qualifier.equals("source")) {
								previousQualifier = al.get(j).getSource();
							} else if (this.qualifier.equals("section")) {
								previousQualifier = al.get(j).getSection();
							} else if (this.qualifier.equals("type")) {
								previousQualifier = al.get(j).getType();
							} else if (this.qualifier.equals("id")) {
								previousQualifier = al.get(j).getDocumentIdAsString();
							} else if (this.qualifier.equals("title")) {
								previousQualifier = al.get(j).getTitle();
							}
						}
						calPrevious = al.get(j).getDateTime();
						yearPrevious = calPrevious.getYear();
						monthPrevious = calPrevious.getMonthValue();
						@SuppressWarnings("static-access")
						WeekFields weekFieldsPrevious = WeekFields.of(Locale.UK.getDefault()); // use UK definition of calendar weeks
						weekPrevious = calPrevious.get(weekFieldsPrevious.weekOfWeekBasedYear());
						if (s.getStatementTypeId() == al.get(j).getStatementTypeId()
								&& ( (al.get(j).getDocumentId() == s.getDocumentId() && duplicates.equals("document"))
								|| duplicates.equals("acrossrange")
								|| (duplicates.equals("year") && year == yearPrevious)
								|| (duplicates.equals("month") && month == monthPrevious)
								|| (duplicates.equals("week") && week == weekPrevious) )
								&& values1[i].equals(previousVar1)
								&& (values2.length == 0 /* for barplot data */ || values2[i].equals(previousVar2))
								&& (this.qualifierAggregation.equals("ignore")
									|| (dataTypes.get(this.qualifier).equals("short text") && qualifierValues[i].equals(previousQualifier))
									|| (!dataTypes.get(this.qualifier).equals("short text") && (Integer) s.get(this.qualifier) == (Integer) al.get(j).get(this.qualifier)))) {
							select = false;
							break;
						}
					}
				}

				// add only if the statement passed all checks
				if (select) {
					al.add(s);
				}

				pb.stepTo(i + 1);
			}
			this.filteredStatements = al;
			pb.stepTo(this.originalStatements.size());
		}
	}

	/**
	 * Retrieve the values across statements/documents given the name of the
	 * variable. E.g., provide a variable name and information on whether the
	 * variable is defined at the document level (e.g., author or section) or at
	 * the statement level (e.g., organization), and return a one-dimensional
	 * array of values (e.g., the organization names or authors for all
	 * statements provided.
	 * 
	 * @param statements Original or filtered array list of statements.
	 * @param variable String denoting the first variable (containing the row
	 *   values).
	 * @param documentLevel Indicates if the first variable is at the document
	 *   level.
	 * @return String array of values.
	 */
	private String[] retrieveValues(ArrayList<ExportStatement> statements, String variable, boolean documentLevel) {
		ExportStatement s;
		String[] values = new String[statements.size()];
		for (int i = 0; i < statements.size(); i++) {
			s = statements.get(i);
			if (documentLevel) {
				if (variable.equals("author")) {
					values[i] = s.getAuthor();
				} else if (variable.equals("source")) {
					values[i] = s.getSource();
				} else if (variable.equals("section")) {
					values[i] = s.getSection();
				} else if (variable.equals("type")) {
					values[i] = s.getType();
				} else if (variable.equals("id")) {
					values[i] = s.getDocumentIdAsString();
				} else if (variable.equals("title")) {
					values[i] = s.getTitle();
				}
			} else {
				values[i] = ((Entity) s.get(variable)).getValue();
			}
		}
		return values;
	}

	/**
	 * Count how often a value is used across the range of filtered statements.
	 * 
	 * @param variableValues String array of all values of a certain variable in
	 *   a set of statements.
	 * @param uniqueNames String array of unique values of the same variable
	 *   across all statements.
	 * @return {@link int} array of value frequencies for each unique value in
	 *   same order as {@code uniqueNames}.
	 */
	private int[] countFrequencies(String[] variableValues, String[] uniqueNames) {
		int[] frequencies = new int[uniqueNames.length];
		for (int i = 0; i < uniqueNames.length; i++) {
			for (int j = 0; j < variableValues.length; j++) {
				if (uniqueNames[i].equals(variableValues[j])) {
					frequencies[i] = frequencies[i] + 1;
				}
			}
		}
		return frequencies;
	}

	/**
	 * Lexical ranking of a binary vector.
	 *
	 * Examples:
	 *
	 * [0, 0] -> 0
	 * [0, 1] -> 1
	 * [1, 0] -> 2
	 * [1, 1] -> 3
	 * [0, 0, 1, 0, 1, 0] -> 10
	 *
	 * This bijection is used to map combinations of qualifier values into edge
	 * weights in the resulting network matrix.
	 *
	 * Source: <a href="https://cw.fel.cvut.cz/wiki/_media/courses/b4m33pal/pal06.pdf">https://cw.fel.cvut.cz/wiki/_media/courses/b4m33pal/pal06.pdf</a>.
	 *
	 * @param binaryVector A binary int array of arbitrary length, indicating
	 *   which qualifier values are used in the dataset
	 * @return An integer
	 */
	private int lexRank(int[] binaryVector) {
		int n = binaryVector.length;
		int r = 0;
		for (int i = 0; i < n; i++) {
			if (binaryVector[i] > 0) {
				r = r + (int) Math.pow(2, n - i - 1);
			}
		}
		return r;
	}
	
	/**
	 * Create a three-dimensional array (variable 1 x variable 2 x qualifier).
	 * 
	 * @param names1 {@link String} array containing the row labels.
	 * @param names2 {@link String} array containing the column labels.
	 * @return 3D double array.
	 */
	private double[][][] createArray(ArrayList<ExportStatement> processedStatements, String[] names1, String[] names2) {

		// unique qualifier values (i.e., all of them found at least once in the dataset)
		String[] qualifierString = null;
		int[] qualifierInteger = new int[] { 0 };
		int qualifierLength = 1;
		if (qualifier == null) {
			// do nothing; go with qualifierLength = 1
		} else if (!this.qualifierDocument && dataTypes.get(qualifier).equals("boolean")) {
			qualifierInteger = new int[] {0, 1};
			qualifierLength = 2;
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("integer")) {
			qualifierInteger = this.originalStatements
					.stream()
					.mapToInt(s -> (int) s.get(qualifier))
					.distinct()
					.sorted()
					.toArray();
			qualifierLength = qualifierInteger.length;
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("short text")) {
			qualifierString = this.originalStatements
					.stream()
					.map(s -> (String) ((Entity) s.get(qualifier)).getValue())
					.distinct()
					.sorted()
					.toArray(String[]::new);
			qualifierLength = qualifierString.length;
		} else if (qualifierDocument) {
			qualifierString = Arrays.stream(retrieveValues(processedStatements, this.qualifier, this.qualifierDocument))
					.distinct()
					.sorted()
					.toArray(String[]::new);
			qualifierLength = qualifierString.length;
		}

		// Create arrays with variable values
		String[] values1 = retrieveValues(processedStatements, variable1, variable1Document);
		String[] values2 = retrieveValues(processedStatements, variable2, variable2Document);
		
		// create and populate array
		double[][][] array = new double[names1.length][names2.length][qualifierLength]; // 3D array: rows x cols x qualifier value
		for (int i = 0; i < processedStatements.size(); i++) {
			String n1 = values1[i]; // retrieve first value from statement
			String n2 = values2[i]; // retrieve second value from statement
			String qString = null;
			int qInteger = -1;
			if (qualifier != null) {
				if (this.qualifierDocument) {
					TableDocument d = documents.get(docMap.get(processedStatements.get(i).getDocumentId()));
					if (qualifier.equals("id")) {
						qString = String.valueOf(d.getId());
					} else if (qualifier.equals("title")) {
						qString = d.getTitle();
					} else if (qualifier.equals("author")) {
						qString = d.getAuthor();
					} else if (qualifier.equals("source")) {
						qString = d.getSource();
					} else if (qualifier.equals("section")) {
						qString = d.getSection();
					} else if (qualifier.equals("type")) {
						qString = d.getType();
					}
					qInteger = -1;
				} else if (dataTypes.get(qualifier).equals("short text")) {
					qString = ((Entity) processedStatements.get(i).get(qualifier)).getValue(); // retrieve short text qualifier value from statement (via Entity)
					qInteger = -1;
				} else {
					qInteger = (int) processedStatements.get(i).get(qualifier); // retrieve integer or boolean qualifier value from statement
					qString = null;
				}
			}
			
			// find out which matrix row corresponds to the first value
			int row = -1;
			for (int j = 0; j < names1.length; j++) {
				if (names1[j].equals(n1)) {
					row = j;
					break;
				}
			}
			
			// find out which matrix column corresponds to the second value
			int col = -1;
			for (int j = 0; j < names2.length; j++) {
				if (names2[j].equals(n2)) {
					col = j;
					break;
				}
			}
			
			// find out which qualifier level corresponds to the qualifier value
			int qual = 0;
			if (qualifierLength > 1) {
				for (int j = 0; j < qualifierLength; j++) {
					if (qualifierDocument && qualifierString[j].equals(qString) ||
							(dataTypes.containsKey(qualifier) && dataTypes.get(qualifier).equals("short text") && qualifierString[j].equals(qString)) ||
							(!qualifierDocument && !dataTypes.get(qualifier).equals("short text") && qualifierInteger[j] == qInteger)) {
						qual = j;
						break;
					}
				}
			}

			// add match to matrix (note that duplicates were dealt with at the statement filter stage)
			array[row][col][qual] = array[row][col][qual] + 1.0;
		}
		
		return array;
	}
	
	/**
	 * Compute the results. Choose the right method based on the settings.
	 */
	public void computeResults() {
		if (networkType.equals("onemode") && timeWindow.equals("no")) {
			computeOneModeMatrix();
		} else if (networkType.equals("twomode") && timeWindow.equals("no")) {
			computeTwoModeMatrix();
		} else if (!networkType.equals("eventlist") && !timeWindow.equals("no")) {
			if (this.kernel.equals("no")) {
				computeTimeWindowMatrices();
			} else {
				computeKernelSmoothedTimeSlices();
			}
		}
	}
	
	/**
	 * Wrapper method to compute one-mode network matrix with class settings and save within class.
	 */
	private void computeOneModeMatrix() {
		ArrayList<Matrix> matrices = new ArrayList<Matrix>(); 
		matrices.add(this.computeOneModeMatrix(this.filteredStatements,
				this.qualifierAggregation, this.startDateTime, this.stopDateTime));
		this.matrixResults = matrices;
	}

	/**
	 * Create a one-mode network {@link Matrix}.
	 *
	 * @param processedStatements Usually the filtered list of export
	 *   statements, but it can be a more processed list of export statements,
	 *   for example for use in constructing a time window sequence of networks.
	 * @param aggregation Qualifier aggregation; usually the qualifier
	 *   aggregation in the constructor/class field ({@link
	 *   #qualifierAggregation}), but it can deviate from it, for example in the
	 *   time window functionality, where multiple versions need to be created.
	 * @param start Start date/time.
	 * @param stop End date/time.
	 * @return {@link Matrix Matrix} object containing a one-mode network
	 *   matrix.
	 */
	Matrix computeOneModeMatrix(ArrayList<ExportStatement> processedStatements, String aggregation, LocalDateTime start, LocalDateTime stop) {
		String[] names1 = this.extractLabels(processedStatements, this.variable1, this.variable1Document);
		String[] names2 = this.extractLabels(processedStatements, this.variable2, this.variable2Document);

		if (processedStatements.size() == 0) {
			double[][] m = new double[names1.length][names1.length];
			Matrix mt = new Matrix(m, names1, names1, true, start, stop);
			return mt;
		}

		String[] qualifierString;
		int[] qualifierInteger = new int[] { 0 };
		int qualifierLength = 1;
		if (qualifier == null) {
			// do nothing, go with qualifierLength = 1
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("boolean")) {
			qualifierInteger = new int[] {0, 1};
			qualifierLength = 2;
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("integer")) {
			qualifierInteger = this.originalStatements
					.stream()
					.mapToInt(s -> (int) s.get(qualifier))
					.distinct()
					.sorted()
					.toArray();
			qualifierLength = qualifierInteger.length;
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("short text")) {
			qualifierString = this.originalStatements
					.stream()
					.map(s -> (String) ((Entity) s.get(qualifier)).getValue())
					.distinct()
					.sorted()
					.toArray(String[]::new);
			qualifierLength = qualifierString.length;
		} else if (qualifierDocument) {
			qualifierString = Arrays.stream(retrieveValues(processedStatements, this.qualifier, this.qualifierDocument))
					.distinct()
					.sorted()
					.toArray(String[]::new);
			qualifierLength = qualifierString.length;
		}

		double[][][] array = createArray(processedStatements, names1, names2);
		double[][] mat1 = new double[names1.length][names1.length];  // square matrix for results
		double range = Math.abs(qualifierInteger[qualifierInteger.length - 1] - qualifierInteger[0]);

		double i1count = 0.0;
		double i2count = 0.0;
		for (int i1 = 0; i1 < names1.length; i1++) {
			for (int i2 = 0; i2 < names1.length; i2++) {
				if (i1 != i2) {
					for (int j = 0; j < names2.length; j++) {
						// "ignore": sum up i1 and i2 independently over levels of k, then multiply.
						// In the binary case, this amounts to counting how often each concept is used and then multiplying frequencies.
						if (aggregation.equals("ignore")) {
							i1count = 0.0;
							i2count = 0.0;
							for (int k = 0; k < qualifierLength; k++) {
								i1count = i1count + array[i1][j][k];
								i2count = i2count + array[i2][j][k];
							}
							mat1[i1][i2] = mat1[i1][i2] + i1count * i2count;
						} else {
							if (qualifierDocument || dataTypes.get(qualifier).equals("short text")) {
								for (int k = 0; k < qualifierLength; k++) {
									if (aggregation.equals("congruence")) {
										mat1[i1][i2] = mat1[i1][i2] + (array[i1][j][k] * array[i2][j][k]); // multiply matches on the qualifier and add
									} else if (aggregation.equals("conflict")) {
										if (array[i1][j][k] > 0 && array[i2][j][k] == 0) {
											mat1[i1][i2] = mat1[i1][i2] + array[i1][j][k]; // add counts where only i1 is active
										} else if (array[i1][j][k] == 0 && array[i2][j][k] > 0) {
											mat1[i1][i2] = mat1[i1][i2] + array[i2][j][k]; // add counts where only i2 is active
										}
									} else if (aggregation.equals("subtract")) {
										mat1[i1][i2] = mat1[i1][i2] + (array[i1][j][k] * array[i2][j][k]); // multiply matches on the qualifier and add
										if (array[i1][j][k] > 0 && array[i2][j][k] == 0) {
											mat1[i1][i2] = mat1[i1][i2] - array[i1][j][k]; // subtract counts where only i1 is active
										} else if (array[i1][j][k] == 0 && array[i2][j][k] > 0) {
											mat1[i1][i2] = mat1[i1][i2] - array[i2][j][k]; // subtract counts where only i2 is active
										}
									}
								}
							} else if (dataTypes.get(qualifier).equals("boolean") || dataTypes.get(qualifier).equals("integer")) {
								for (int k1 = 0; k1 < qualifierLength; k1++) {
									for (int k2 = 0; k2 < qualifierLength; k2++) {
										if (aggregation.equals("congruence")) {
											// "congruence": sum up proximity of i1 and i2 per level of k, weighted by joint usage.
											// In the binary case, this reduces to the sum of weighted matches per level of k
											mat1[i1][i2] = mat1[i1][i2] + (array[i1][j][k1] * array[i2][j][k2] * (1.0 - ((Math.abs(qualifierInteger[k1] - qualifierInteger[k2]) / range))));
										} else if (aggregation.equals("conflict")) {
											// "conflict": same as congruence, but distance instead of proximity
											mat1[i1][i2] = mat1[i1][i2] + (array[i1][j][k1] * array[i2][j][k2] * ((Math.abs(qualifierInteger[k1] - qualifierInteger[k2]) / range)));
										} else if (aggregation.equals("subtract")) {
											// "subtract": congruence - conflict
											mat1[i1][i2] = mat1[i1][i2] + (array[i1][j][k1] * array[i2][j][k2] * (1.0 - ((Math.abs(qualifierInteger[k1] - qualifierInteger[k2]) / range))));
											mat1[i1][i2] = mat1[i1][i2] - (array[i1][j][k1] * array[i2][j][k2] * ((Math.abs(qualifierInteger[k1] - qualifierInteger[k2]) / range)));
										}
									}
								}
							}
						}
					}

					// normalization
					double norm = 1.0;
					if (this.normalization.equals("no")) {
						norm = 1.0;
					} else if (this.normalization.equals("average")) {
						i1count = 0.0;
						i2count = 0.0;
						for (int j = 0; j < names2.length; j++) {
							for (int k = 0; k < qualifierLength; k++) {
								i1count = i1count + array[i1][j][k];
								i2count = i2count + array[i2][j][k];
							}
						}
						norm = (i1count + i2count) / 2;
					} else if (this.normalization.equals("jaccard")) {
						double m10 = 0.0;
						double m01 = 0.0;
						double m11 = 0.0;
						for (int j = 0; j < names2.length; j++) {
							for (int k = 0; k < qualifierLength; k++) {
								if (array[i2][j][k] == 0) {
									m10 = m10 + array[i1][j][k];
								}
								if (array[i1][j][k] == 0) {
									m01 = m01 + array[i2][j][k];
								}
								if (array[i1][j][k] > 0 && array[i2][j][k] > 0) {
									m11 = m11 + (array[i1][j][k] * array[i2][j][k]);
								}
							}
						}
						norm = m01 + m10 + m11;
					} else if (this.normalization.equals("cosine")) {
						i1count = 0.0;
						i2count = 0.0;
						for (int j = 0; j < names2.length; j++) {
							for (int k = 0; k < qualifierLength; k++) {
								i1count = i1count + array[i1][j][k];
								i2count = i2count + array[i2][j][k];
							}
						}
						norm = Math.sqrt(i1count * i1count) * Math.sqrt(i2count * i2count);
					}
					if (norm == 0) {
						mat1[i1][i2] = 0;
					} else {
						mat1[i1][i2] = mat1[i1][i2] / norm;
					}
				}
			}
		}

		// does the matrix contain only integer values? (i.e., no normalization and boolean or short text qualifier)
		boolean integerBoolean;
		if (this.normalization.equals("no") && (aggregation.equals("ignore") || qualifierDocument || dataTypes.get(qualifier).equals("boolean") || dataTypes.get(qualifier).equals("short text"))) {
			integerBoolean = true;
		} else {
			integerBoolean = false;
		}

		Matrix matrix = new Matrix(mat1, names1, names1, integerBoolean, start, stop);
		matrix.setNumStatements(this.filteredStatements.size());
		return matrix;
	}

	/**
	 * Wrapper method to compute two-mode network matrix with class settings.
	 */
	public void computeTwoModeMatrix() {
		ArrayList<Matrix> matrices = new ArrayList<Matrix>(); 
		matrices.add(this.computeTwoModeMatrix(this.filteredStatements,	this.startDateTime, this.stopDateTime));
		this.matrixResults = matrices;
	}

	/**
	 * Create a two-mode network {@link Matrix}.
	 *
	 * @param processedStatements Usually the filtered list of export
	 *   statements, but it can be a more processed list of export statements,
	 *   for example for use in constructing a time window sequence of networks.
	 * @param start Start date/time.
	 * @param stop End date/time.
	 * @return {@link Matrix Matrix} object containing a two-mode network matrix.
	 */
	private Matrix computeTwoModeMatrix(ArrayList<ExportStatement> processedStatements, LocalDateTime start, LocalDateTime stop) {
		String[] names1 = this.extractLabels(processedStatements, this.variable1, this.variable1Document);
		String[] names2 = this.extractLabels(processedStatements, this.variable2, this.variable2Document);

		if (processedStatements.size() == 0) {
			double[][] m = new double[names1.length][names2.length];
			Matrix mt = new Matrix(m, names1, names2, true, start, stop);
			return mt;
		}

		String[] qualifierString = null;
		int[] qualifierInteger = new int[] { 0 };
		int qualifierLength = 1;
		if (qualifier == null) {
			// do nothing, go with qualifierLength = 1
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("boolean")) {
			qualifierInteger = new int[] {0, 1};
			qualifierLength = 2;
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("integer")) {
			qualifierInteger = this.originalStatements
					.stream()
					.mapToInt(s -> (int) s.get(qualifier))
					.distinct()
					.sorted()
					.toArray();
			qualifierLength = qualifierInteger.length;
		} else if (!qualifierDocument && dataTypes.get(qualifier).equals("short text")) {
			qualifierString = this.originalStatements
					.stream()
					.map(s -> (String) ((Entity) s.get(qualifier)).getValue())
					.distinct()
					.sorted()
					.toArray(String[]::new);
			qualifierLength = qualifierString.length;
		} else if (qualifierDocument) {
			qualifierString = Arrays.stream(retrieveValues(processedStatements, this.qualifier, this.qualifierDocument))
					.distinct()
					.sorted()
					.toArray(String[]::new);
			qualifierLength = qualifierString.length;
		}

		double[][][] array = createArray(processedStatements, names1, names2);

		// combine levels of the qualifier variable conditional on qualifier aggregation option
		double[][] mat = new double[names1.length][names2.length];  // initialized with zeros
		HashMap<Integer, ArrayList> combinations = new HashMap<Integer, ArrayList>();
		for (int i = 0; i < names1.length; i++) {
			for (int j = 0; j < names2.length; j++) {
				if (this.qualifierAggregation.equals("combine")) { // combine
					double[] vec = array[i][j]; // may be weighted, so create a second, binary vector vec2
					int[] vec2 = new int[vec.length];
					ArrayList qualVal;
					if (qualifierDocument || dataTypes.get(qualifier).equals("short text")) {
						qualVal = new ArrayList<String>();
						for (int k = 0; k < vec.length; k++) {
							if (vec[k] > 0) {
								vec2[k] = 1;
							}
							assert qualifierString != null;
							qualVal.add(qualifierString[k]);
						}
					} else {
						qualVal = new ArrayList<Integer>();
						for (int k = 0; k < vec.length; k++) {
							if (vec[k] > 0) {
								vec2[k] = 1;
							}
							qualVal.add(qualifierInteger[k]);
						}
					}
					int lr = lexRank(vec2);
					mat[i][j] = lr; // compute lexical rank, i.e., map the combination of values to a single integer
					combinations.put(lr, qualVal); // the bijection needs to be stored for later reporting
				} else {
					for (int k = 0; k < qualifierLength; k++) {
						if (this.qualifierAggregation.equals("ignore")) { // ignore
							mat[i][j] = mat[i][j] + array[i][j][k]; // duplicates were already filtered out in the statement filter, so just add
						} else if (this.qualifierAggregation.equals("subtract")) { // subtract
							if (!qualifierDocument && dataTypes.get(qualifier).equals("integer")) {
								if (qualifierInteger[k] < 0) { // subtract weighted absolute value
									mat[i][j] = mat[i][j] - (Math.abs(qualifierInteger[k]) * array[i][j][k]);
								} else if (qualifierInteger[k] >= 0) { // add weighted absolute value
									mat[i][j] = mat[i][j] + (Math.abs(qualifierInteger[k]) * array[i][j][k]);
								}
							} else if (!qualifierDocument && dataTypes.get(qualifier).equals("boolean")) {
								if (qualifierInteger[k] == 0) { // zero category: subtract number of times this happens from edge weight
									mat[i][j] = mat[i][j] - array[i][j][k];
								} else if (qualifierInteger[k] > 0) { // one category: add number of times this happens to edge weight
									mat[i][j] = mat[i][j] + array[i][j][k];
								}
							} else if (qualifierDocument || dataTypes.get(qualifier).equals("short text")) {
								mat[i][j] = mat[i][j] + array[i][j][k]; // nothing to subtract because there is no negative mention with short text variables
							}
						}
					}
				}
			}
		}

		// report combinations if necessary
		if (combinations.size() > 0) {
			String s = "";
			Iterator<Integer> keyIterator = combinations.keySet().iterator();
			while (keyIterator.hasNext()){
				Integer key = (Integer) keyIterator.next();
				ArrayList<Integer> values = combinations.get(key);
				s = "An edge weight of " + key + " maps onto combination: ";
				for (int i = 0; i < values.size(); i++) {
					s = s + values.get(i) + " ";
				}
				s = s + "\n";
			}
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Qualifier \"combine\" option combinatorial mapping established.",
					s);
			Dna.logger.log(l);
		}

		// normalization
		boolean integerBoolean = false;
		if (this.normalization.equals("no")) {
			integerBoolean = true;
		} else if (this.normalization.equals("activity")) {
			integerBoolean = false;
			double currentDenominator;
			for (int i = 0; i < names1.length; i++) {
				currentDenominator = 0.0;
				if (qualifierAggregation.equals("ignore")) { // iterate through columns of matrix and sum weighted values
					for (int j = 0; j < names2.length; j++) {
						currentDenominator = currentDenominator + mat[i][j];
					}
				} else if (qualifierAggregation.equals("combine")) { // iterate through columns of matrix and count how many are larger than one
					LogEvent l = new LogEvent(Logger.WARNING,
							"Normalization and \"combine\" yield uninterpretable results.",
							"When exporting a network, the use of normalization and the qualifier setting \"combine\" were used together. These settings together yield results that cannot be interpreted in a meaningful way.");
					Dna.logger.log(l);
					for (int j = 0; j < names2.length; j++) {
						if (mat[i][j] > 0.0) {
							currentDenominator = currentDenominator + 1.0;
						}
					}
				} else if (qualifierAggregation.equals("subtract")) { // iterate through array and sum for different levels
					for (int j = 0; j < names2.length; j++) {
						for (int k = 0; k < qualifierLength; k++) {
							currentDenominator = currentDenominator + array[i][j][k];
						}
					}
				}
				for (int j = 0; j < names2.length; j++) { // divide all values by current denominator
					if (currentDenominator == 0) {
						mat[i][j] = 0;
					} else {
						mat[i][j] = mat[i][j] / currentDenominator;
					}
				}
			}
		} else if (this.normalization.equals("prominence")) {
			integerBoolean = false;
			double currentDenominator;
			for (int i = 0; i < names2.length; i++) {
				currentDenominator = 0.0;
				if (this.qualifierAggregation.equals("ignore")) { // iterate through rows of matrix and sum weighted values
					for (int j = 0; j < names1.length; j++) {
						currentDenominator = currentDenominator + mat[j][i];
					}
				} else if (this.qualifierAggregation.equals("combine")) { // iterate through rows of matrix and count how many are larger than one
					LogEvent l = new LogEvent(Logger.WARNING,
							"Normalization and \"combine\" yield uninterpretable results.",
							"When exporting a network, the use of normalization and the qualifier setting \"combine\" were used together. These settings together yield results that cannot be interpreted in a meaningful way.");
					Dna.logger.log(l);
					for (int j = 0; j < names1.length; j++) {
						if (mat[i][j] > 0.0) {
							currentDenominator = currentDenominator + 1.0;
						}
					}
				} else if (this.qualifierAggregation.equals("subtract")) { // iterate through array and sum for different levels
					for (int j = 0; j < names1.length; j++) {
						for (int k = 0; k < qualifierLength; k++) {
							currentDenominator = currentDenominator + array[j][i][k];
						}
					}
				}
				for (int j = 0; j < names1.length; j++) { // divide all values by current denominator
					if (currentDenominator == 0) {
						mat[j][i] = 0;
					} else {
						mat[j][i] = mat[j][i] / currentDenominator;
					}
				}
			}
		}

		// create Matrix object and return
		Matrix matrix = new Matrix(mat, names1, names2, integerBoolean, start, stop); // assemble the Matrix object with labels
		matrix.setNumStatements(this.filteredStatements.size());
		return matrix;
	}

    /**
     * Compute a series of network matrices using kernel smoothing.
     * This function creates a series of network matrices (one-mode or two-mode) similar to the time window approach,
     * but using kernel smoothing around a forward-moving mid-point on the time axis (gamma). The networks are defined
     * by the mid-point {@code gamma}, the window size {@code w}, and the kernel function. These parameters are saved in
     * the fields of the class. All networks have the same dimensions, i.e., isolates are included at any time point, to
     * make the networks comparable and amenable to distance functions and other pair-wise computations.
     */
	public void computeKernelSmoothedTimeSlices() {
		// check and fix normalization setting for unimplemented normalization settings
		if (Exporter.this.normalization.equals("jaccard") || Exporter.this.normalization.equals("cosine") || Exporter.this.normalization.equals("activity") || Exporter.this.normalization.equals("prominence")) {
			LogEvent l = new LogEvent(Logger.WARNING,
					Exporter.this.normalization + " normalization not implemented.",
					Exporter.this.normalization + " normalization has not been implemented (yet?) for kernel-smoothed networks. Using \"average\" normalization instead.");
			Exporter.this.normalization = "average";
			Dna.logger.log(l);
		}

		// check and fix two-mode qualifier aggregation for unimplemented settings
		if (Exporter.this.qualifierAggregation.equals("combine")) {
			LogEvent l = new LogEvent(Logger.WARNING,
					Exporter.this.qualifierAggregation + " qualifier aggregation not implemented.",
					Exporter.this.qualifierAggregation + " qualifier aggregation has not been implemented for kernel-smoothed networks. Using \"subtract\" qualifier aggregation instead.");
			Exporter.this.qualifierAggregation = "subtract";
			Dna.logger.log(l);
		}

		// initialise variables and constants
		Collections.sort(this.filteredStatements); // probably not necessary, but can't hurt to have it
		if (this.windowSize % 2 != 0) { // windowSize is the w constant in the paper; only even numbers are acceptable because adding or subtracting w / 2 to or from gamma would not yield integers
			this.windowSize = this.windowSize + 1;
		}
		LocalDateTime firstDate = Exporter.this.filteredStatements.get(0).getDateTime();
		LocalDateTime lastDate = Exporter.this.filteredStatements.get(Exporter.this.filteredStatements.size() - 1).getDateTime();
		final int W_HALF = windowSize / 2;
		LocalDateTime b = this.startDateTime.isBefore(firstDate) ? firstDate : this.startDateTime;  // start of statement list
		LocalDateTime e = this.stopDateTime.isAfter(lastDate) ? lastDate : this.stopDateTime;  // end of statement list
		LocalDateTime gamma = b; // current time while progressing through list of statements
		LocalDateTime e2 = e; // indented end point (e minus half w)
		if (Exporter.this.indentTime) {
			if (timeWindow.equals("minutes")) {
				gamma = gamma.plusMinutes(W_HALF);
				e2 = e.minusMinutes(W_HALF);
			} else if (timeWindow.equals("hours")) {
				gamma = gamma.plusHours(W_HALF);
				e2 = e.minusHours(W_HALF);
			} else if (timeWindow.equals("days")) {
				gamma = gamma.plusDays(W_HALF);
				e2 = e.minusDays(W_HALF);
			} else if (timeWindow.equals("weeks")) {
				gamma = gamma.plusWeeks(W_HALF);
				e2 = e.minusWeeks(W_HALF);
			} else if (timeWindow.equals("months")) {
				gamma = gamma.plusMonths(W_HALF);
				e2 = e.minusMonths(W_HALF);
			} else if (timeWindow.equals("years")) {
				gamma = gamma.plusYears(W_HALF);
				e2 = e.minusYears(W_HALF);
			}
		}

		// save the labels of the variables and qualifier and put indices in hash maps for fast retrieval
		String[] var1Values = extractLabels(Exporter.this.filteredStatements, Exporter.this.variable1, Exporter.this.variable1Document);
		String[] var2Values = extractLabels(Exporter.this.filteredStatements, Exporter.this.variable2, Exporter.this.variable2Document);
		String[] qualValues = new String[] { "" };
		if (Exporter.this.qualifier != null) {
			 qualValues = extractLabels(Exporter.this.filteredStatements, Exporter.this.qualifier, Exporter.this.qualifierDocument);
		}
		if (Exporter.this.qualifier != null && dataTypes.get(Exporter.this.qualifier).equals("integer")) {
			int[] qual = Exporter.this.originalStatements.stream().mapToInt(s -> (int) s.get(Exporter.this.qualifier)).distinct().sorted().toArray();
			if (qual.length < qualValues.length) {
				qualValues = IntStream.rangeClosed(qual[0], qual[qual.length - 1])
						.mapToObj(String::valueOf)
						.toArray(String[]::new);
			}
		}
		HashMap<String, Integer> var1Map = new HashMap<>();
		for (int i = 0; i < var1Values.length; i++) {
			var1Map.put(var1Values[i], i);
		}
		HashMap<String, Integer> var2Map = new HashMap<>();
		for (int i = 0; i < var2Values.length; i++) {
			var2Map.put(var2Values[i], i);
		}
		HashMap<String, Integer> qualMap = new HashMap<>();
		if (Exporter.this.qualifier != null) {
			for (int i = 0; i < qualValues.length; i++) {
				qualMap.put(qualValues[i], i);
			}
		}

		// create an array list of empty Matrix results, store all date-time stamps in them, and save indices in a hash map
		Exporter.this.matrixResults = new ArrayList<>();
		if (Exporter.this.kernel.equals("gaussian")) { // for each mid-point gamma, create an empty Matrix and save the start, mid, and end time points in it as defined by the start and end of the whole time range; the actual matrix is injected later
			if (timeWindow.equals("minutes")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, b, gamma, e));
					gamma = gamma.plusMinutes(1);
				}
			} else if (timeWindow.equals("hours")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, b, gamma, e));
					gamma = gamma.plusHours(1);
				}
			} else if (timeWindow.equals("days")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, b, gamma, e));
					gamma = gamma.plusDays(1);
				}
			} else if (timeWindow.equals("weeks")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, b, gamma, e));
					gamma = gamma.plusWeeks(1);
				}
			} else if (timeWindow.equals("months")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, b, gamma, e));
					gamma = gamma.plusMonths(1);
				}
			} else if (timeWindow.equals("years")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, b, gamma, e));
					gamma = gamma.plusYears(1);
				}
			}
		} else { // for each mid-point gamma, create an empty Matrix and save the start, mid, and end time points in it as defined by width w; the actual matrix is injected later
			if (timeWindow.equals("minutes")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, gamma.minusMinutes(W_HALF).isBefore(b) ? b : gamma.minusMinutes(W_HALF), gamma, gamma.plusMinutes(W_HALF).isAfter(e) ? e : gamma.plusMinutes(W_HALF)));
					gamma = gamma.plusMinutes(1);
				}
			} else if (timeWindow.equals("hours")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, gamma.minusHours(W_HALF).isBefore(b) ? b : gamma.minusHours(W_HALF), gamma, gamma.plusHours(W_HALF).isAfter(e) ? e : gamma.plusHours(W_HALF)));
					gamma = gamma.plusHours(1);
				}
			} else if (timeWindow.equals("days")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, gamma.minusDays(W_HALF).isBefore(b) ? b : gamma.minusDays(W_HALF), gamma, gamma.plusDays(W_HALF).isAfter(e) ? e : gamma.plusDays(W_HALF)));
					gamma = gamma.plusDays(1);
				}
			} else if (timeWindow.equals("weeks")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, gamma.minusWeeks(W_HALF).isBefore(b) ? b : gamma.minusWeeks(W_HALF), gamma, gamma.plusWeeks(W_HALF).isAfter(e) ? e : gamma.plusWeeks(W_HALF)));
					gamma = gamma.plusWeeks(1);
				}
			} else if (timeWindow.equals("months")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, gamma.minusMonths(W_HALF).isBefore(b) ? b : gamma.minusMonths(W_HALF), gamma, gamma.plusMonths(W_HALF).isAfter(e) ? e : gamma.plusMonths(W_HALF)));
					gamma = gamma.plusMonths(1);
				}
			} else if (timeWindow.equals("years")) {
				while (!gamma.isAfter(e2)) {
					Exporter.this.matrixResults.add(new Matrix(var1Values, Exporter.this.networkType.equals("onemode") ? var1Values : var2Values, false, gamma.minusYears(W_HALF).isBefore(b) ? b : gamma.minusYears(W_HALF), gamma, gamma.plusYears(W_HALF).isAfter(e) ? e : gamma.plusYears(W_HALF)));
					gamma = gamma.plusYears(1);
				}
			}
		}

		// create a 3D array, go through the statements, and populate the array
		@SuppressWarnings("unchecked")
		ArrayList<ExportStatement>[][][] X = (ArrayList<ExportStatement>[][][]) new ArrayList<?>[var1Values.length][var2Values.length][qualValues.length];
		for (int i = 0; i < var1Values.length; i++) {
			for (int j = 0; j < var2Values.length; j++) {
				if (Exporter.this.qualifier == null) {
					X[i][j][0] = new ArrayList<ExportStatement>();
				} else {
					for (int k = 0; k < qualValues.length; k++) {
						X[i][j][k] = new ArrayList<ExportStatement>();
					}
				}
			}
		}

		Exporter.this.filteredStatements.stream().forEach(s -> {
			int var1Index = -1;
			if (Exporter.this.variable1Document) {
				if (Exporter.this.variable1.equals("author")) {
					var1Index = var1Map.get(s.getAuthor());
				} else if (Exporter.this.variable1.equals("source")) {
					var1Index = var1Map.get(s.getSource());
				} else if (Exporter.this.variable1.equals("section")) {
					var1Index = var1Map.get(s.getSection());
				} else if (Exporter.this.variable1.equals("type")) {
					var1Index = var1Map.get(s.getType());
				} else if (Exporter.this.variable1.equals("id")) {
					var1Index = var1Map.get(s.getDocumentIdAsString());
				} else if (Exporter.this.variable1.equals("title")) {
					var1Index = var1Map.get(s.getTitle());
				}
			} else {
				var1Index = var1Map.get(((Entity) s.get(Exporter.this.variable1)).getValue());
			}
			int var2Index = -1;
			if (Exporter.this.variable2Document) {
				if (Exporter.this.variable2.equals("author")) {
					var2Index = var2Map.get(s.getAuthor());
				} else if (Exporter.this.variable2.equals("source")) {
					var2Index = var2Map.get(s.getSource());
				} else if (Exporter.this.variable2.equals("section")) {
					var2Index = var2Map.get(s.getSection());
				} else if (Exporter.this.variable2.equals("type")) {
					var2Index = var2Map.get(s.getType());
				} else if (Exporter.this.variable2.equals("id")) {
					var2Index = var2Map.get(s.getDocumentIdAsString());
				} else if (Exporter.this.variable2.equals("title")) {
					var2Index = var2Map.get(s.getTitle());
				}
			} else {
				var2Index = var2Map.get(((Entity) s.get(Exporter.this.variable2)).getValue());
			}
			int qualIndex = -1;
			if (Exporter.this.qualifierDocument && Exporter.this.qualifier != null) {
				if (Exporter.this.qualifier.equals("author")) {
					qualIndex = qualMap.get(s.getAuthor());
				} else if (Exporter.this.qualifier.equals("source")) {
					qualIndex = qualMap.get(s.getSource());
				} else if (Exporter.this.qualifier.equals("section")) {
					qualIndex = qualMap.get(s.getSection());
				} else if (Exporter.this.qualifier.equals("type")) {
					qualIndex = qualMap.get(s.getType());
				} else if (Exporter.this.qualifier.equals("id")) {
					qualIndex = qualMap.get(s.getDocumentIdAsString());
				} else if (Exporter.this.qualifier.equals("title")) {
					qualIndex = qualMap.get(s.getTitle());
				}
			} else {
				if (Exporter.this.qualifier == null) {
					qualIndex = 0;
				} else if (dataTypes.get(Exporter.this.qualifier).equals("integer") || dataTypes.get(Exporter.this.qualifier).equals("boolean")) {
					qualIndex = qualMap.get(String.valueOf((int) s.get(Exporter.this.qualifier)));
				} else {
					qualIndex = qualMap.get(((Entity) s.get(Exporter.this.qualifier)).getValue());
				}
			}
			X[var1Index][var2Index][qualIndex].add(s);
		});

		// process each matrix result in a parallel stream instead of for-loop and add calculation results
		ArrayList<Matrix> processedResults = ProgressBar.wrap(Exporter.this.matrixResults.parallelStream(), "Kernel smoothing")
				.map(matrixResult -> processTimeSlice(matrixResult, X))
				.collect(Collectors.toCollection(ArrayList::new));
		Exporter.this.matrixResults = processedResults;
	}

	/**
	 * Compute a one-mode or two-mode network matrix with kernel-weighting and inject it into a {@link Matrix} object.
	 * To compute the kernel-weighted network projection, the 3D array X with statement array lists corresponding to
	 * each i-j-k combination is needed because it stores the statement data including their date-time stamp, and
	 * the current matrix result is needed because it stores the mid-point gamma. The kernel-weighted temporal distance
	 * between the statement time and gamma is computed and used in creating the network.
	 *
	 * @param matrixResult The matrix result into which the network matrix will be inserted.
	 * @param X A 3D array containing the data.
	 * @return The matrix result after inserting the network matrix.
	 */
	public Matrix processTimeSlice(Matrix matrixResult, ArrayList<ExportStatement>[][][] X) {
		if (this.networkType.equals("twomode")) {
			double[][] m = new double[X.length][X[0].length];
			for (int i = 0; i < X.length; i++) {
				for (int j = 0; j < X[0].length; j++) {
					for (int k = 0; k < X[0][0].length; k++) {
						for (int t = 0; t < X[i][j][k].size(); t++) {
							if (Exporter.this.kernel.equals("gaussian") || (!X[i][j][k].get(t).getDateTime().isBefore(matrixResult.getStart()) && !X[i][j][k].get(t).getDateTime().isAfter(matrixResult.getStop()))) { // for computational efficiency, don't include statements outside of temporal bandwidth in computations if not necessary
								if (Exporter.this.qualifierAggregation.equals("ignore")) {
									m[i][j] = m[i][j] + zeta(X[i][j][k].get(t).getDateTime(), matrixResult.getDateTime(), Exporter.this.windowSize, Exporter.this.timeWindow, Exporter.this.kernel);
								} else if (Exporter.this.qualifierAggregation.equals("subtract")) {
									if (Exporter.this.dataTypes.get(Exporter.this.qualifier).equals("boolean")) {
										m[i][j] = m[i][j] + (((double) k) - 0.5) * 2 * zeta(X[i][j][k].get(t).getDateTime(), matrixResult.getDateTime(), Exporter.this.windowSize, Exporter.this.timeWindow, Exporter.this.kernel);
									} else if (Exporter.this.dataTypes.get(Exporter.this.qualifier).equals("integer")) {
										m[i][j] = m[i][j] + k * zeta(X[i][j][k].get(t).getDateTime(), matrixResult.getDateTime(), Exporter.this.windowSize, Exporter.this.timeWindow, Exporter.this.kernel);
									} else if (Exporter.this.dataTypes.get(Exporter.this.qualifier).equals("short text")) {
										m[i][j] = m[i][j] + zeta(X[i][j][k].get(t).getDateTime(), matrixResult.getDateTime(), Exporter.this.windowSize, Exporter.this.timeWindow, Exporter.this.kernel);
									}
								}
							}
						}
					}
				}
			}
			matrixResult.setMatrix(m);
		} else if (this.networkType.equals("onemode")) {
			double[][] m = new double[X.length][X.length];
			double[][] norm = new double[X.length][X.length];
			for (int i = 0; i < X.length; i++) {
				for (int i2 = 0; i2 < X.length; i2++) {
					for (int j = 0; j < X[0].length; j++) {
						for (int k = 0; k < X[0][0].length; k++) {
							if (Exporter.this.normalization.equals("average") && X[i][j][k].size() + X[i2][j][k].size() != 0.0) {
								norm[i][i2] = norm[i][i2] + 2.0 / (X[i][j][k].size() + X[i2][j][k].size());
							}
							for (int k2 = 0; k2 < X[0][0].length; k2++) {
								double qsim = 1.0;
								if (Exporter.this.qualifier != null && !dataTypes.get(Exporter.this.qualifier).equals("short text") && !Exporter.this.qualifierDocument) {
									qsim = Math.abs(1.0 - ((double) Math.abs(k - k2) / (double) Math.abs(X[0][0].length - 1)));
								}
								double qdiff = 1.0 - qsim;
								for (int t = 0; t < X[i][j][k].size(); t++) {
									if (Exporter.this.kernel.equals("gaussian") || (!X[i][j][k].get(t).getDateTime().isBefore(matrixResult.getStart()) && !X[i][j][k].get(t).getDateTime().isAfter(matrixResult.getStop()))) { // for computational efficiency, don't include statements outside of temporal bandwidth in computations if not necessary
										double z1 = zeta(X[i][j][k].get(t).getDateTime(), matrixResult.getDateTime(), Exporter.this.windowSize, Exporter.this.timeWindow, Exporter.this.kernel);
										for (int t2 = 0; t2 < X[i2][j][k2].size(); t2++) {
											if (Exporter.this.kernel.equals("gaussian") || (!X[i2][j][k2].get(t2).getDateTime().isBefore(matrixResult.getStart()) && !X[i2][j][k2].get(t2).getDateTime().isAfter(matrixResult.getStop()))) { // for computational efficiency, don't include statements outside of temporal bandwidth in computations if not necessary
												double z2 = zeta(X[i2][j][k2].get(t2).getDateTime(), matrixResult.getDateTime(), Exporter.this.windowSize, Exporter.this.timeWindow, Exporter.this.kernel);
												double z = Math.sqrt(z1 * z2);
												if (Exporter.this.qualifierAggregation.equals("congruence")) {
													m[i][i2] = m[i][i2] + qsim * z;
												} else if (Exporter.this.qualifierAggregation.equals("conflict")) {
													m[i][i2] = m[i][i2] + qdiff * z;
												} else if (Exporter.this.qualifierAggregation.equals("subtract")) {
													m[i][i2] = m[i][i2] + qsim * z - qdiff * z;
												} else if (Exporter.this.qualifierAggregation.equals("ignore")) {
													m[i][i2] = m[i][i2] + z;
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (Exporter.this.normalization.equals("average")) {
				for (int i = 0; i < X.length; i++) {
					for (int i2 = 0; i2 < X.length; i2++) {
						if (m[i][i2] != 0.0 && norm[i][i2] != 0.0) {
							m[i][i2] = m[i][i2] * norm[i][i2];
						}
					}
				}
			}
			matrixResult.setMatrix(m);
		}
		return matrixResult;
	}

    /**
     * Return a standardized time weight after applying a kernel function to a time difference.
     *
     * @param t The current time in the time window.
     * @param gamma The mid-point of the time window.
     * @param w The width of the time window, which defines the beginning and end of the time window.
     * @param timeWindow The time unit. Valid values are {@code "seconds"}, {@code "minutes"}, {@code "hours"}, {@code "days"}, {@code "weeks"}, {@code "months"}, and {@code "years"}.
     * @param kernel The kernel function ({@code "uniform"}, {@code "epanechnikov"}, {@code "triangular"}, or {@code "gaussian"}).
     * @return Kernel-weighted time difference between time points t and gamma.
     */
	private double zeta(LocalDateTime t, LocalDateTime gamma, int w, String timeWindow, String kernel) {
		Duration duration = Duration.between(t, gamma);
		Period period;
		long diff = 0;
        switch (timeWindow) {
            case "seconds":
                diff = duration.toSeconds();
                break;
            case "minutes":
                diff = duration.toMinutes();
                break;
            case "hours":
                diff = duration.toHours();
                break;
			case "days":
				diff = duration.toDays();
				break;
			case "weeks":
				diff = duration.toDays() / 7;
				break;
			case "months":
				period = Period.between(t.toLocalDate(), gamma.toLocalDate());
				diff = period.getMonths() + (long) period.getYears() * (long) 12;
				break;
			case "years":
				period = Period.between(t.toLocalDate(), gamma.toLocalDate());
				diff = period.getYears();
				break;
        }

		double diff_std = 2 * (double) diff / (double) w; // standardised time difference between -1 and 1

		if (kernel.equals("uniform")) {
			if (diff_std >= -1 && diff_std <= 1) {
				return 0.5;
			} else {
				return 0.0;
			}
		} else if (kernel.equals("epanechnikov")) {
			if (diff_std >= -1 && diff_std <= 1) {
				return 0.75 * (1.0 - (diff_std * diff_std));
			} else {
				return 0.0;
			}
		} else if (kernel.equals("triangular")) {
			if (diff_std >= -1 && diff_std <= 1) {
				return Math.abs(1.0 - diff_std);
			} else {
				return 0.0;
			}
		} else if (kernel.equals("gaussian")) {
			return (1.0 / Math.sqrt(2.0 * Math.PI)) * Math.exp(-0.5 * diff_std * diff_std);
		}
		return 0.0;
	}

	/**
	 * Normalize all values in each results matrix to make them sum to 1.0. Useful for phase transition methods.
	 */
	public void normalizeMatrixResultsToOne() {
		try (ProgressBar pb = new ProgressBar("Matrix normalization", Exporter.this.matrixResults.size())) {
			for (Matrix matrixResult : Exporter.this.matrixResults) {
				double[][] matrix = matrixResult.getMatrix();
				double sum = 0.0;
				for (double[] rows : matrix) {
					for (int j = 0; j < matrix[0].length; j++) {
						sum += rows[j];
					}
				}
				if (sum != 0.0) {
					for (int i = 0; i < matrix.length; i++) {
						for (int j = 0; j < matrix[0].length; j++) {
							matrix[i][j] = matrix[i][j] / sum;
						}
					}
				}
				pb.step();
			}
		}
	}

	/**
	 * Compute a distance matrix for the elements of the matrix results stored in the Exporter class.
	 *
	 * @param distanceMethod The distance method: {@code "absdiff"} for the sum of element-wise absolute differences or {@code "spectral"} for normalized Laplacian distances
	 * @return The distance matrix as a 2D array.
	 */
	public double[][] computeDistanceMatrix(String distanceMethod) {
		int t = Exporter.this.matrixResults.size();
		double[][] distanceMatrix = new double[t][t];
		int dim = Exporter.this.matrixResults.get(0).getMatrix().length;
		double[][] eigenvalues = new double[t][dim];

		// precompute eigenvalues to avoid race conditions
		if (distanceMethod.equals("spectral")) {
			ProgressBar.wrap(IntStream.range(0, Exporter.this.matrixResults.size()).parallel(), "Normalized eigenvalues").forEach(i -> {
				eigenvalues[i] = computeNormalizedEigenvalues(Exporter.this.matrixResults.get(i).getMatrix(), "ojalgo"); // TODO: try out "apache", debug, and compare speed
			});
		}

		ProgressBar.wrap(IntStream.range(0, Exporter.this.matrixResults.size()).parallel(), "Distance matrix").forEach(i -> {
			IntStream.range(i + 1, Exporter.this.matrixResults.size()).forEach(j -> { // start from i + 1 to ensure symmetry and avoid redundant computation (= upper triangle)
				double distance = 0.0;
				if (distanceMethod.equals("spectral")) {
					distance = spectralLoss(eigenvalues[i], eigenvalues[j]);
				} else if (distanceMethod.equals("absdiff")) { // sum of element-wise absolute differences
					for (int a = 0; a < Exporter.this.matrixResults.get(i).getMatrix().length; a++) {
						for (int b = 0; b < Exporter.this.matrixResults.get(j).getMatrix()[0].length; b++) {
							distance += Math.abs(Exporter.this.matrixResults.get(i).getMatrix()[a][b] - Exporter.this.matrixResults.get(j).getMatrix()[a][b]);
						}
					}
				}
				distanceMatrix[i][j] = distance;
				distanceMatrix[j][i] = distance; // since the distance matrix is symmetric, set both [i][j] and [j][i]
			});
		});
		return distanceMatrix;
	}

	/**
	 * Create a series of one-mode or two-mode networks using a moving time window.
	 */
	public void computeTimeWindowMatrices() {
		ArrayList<Matrix> timeWindowMatrices = new ArrayList<Matrix>();
		Collections.sort(this.filteredStatements); // probably not necessary, but can't hurt to have it
		ArrayList<ExportStatement> currentWindowStatements = new ArrayList<ExportStatement>(); // holds all statements in the current time window
		ArrayList<ExportStatement> startStatements = new ArrayList<ExportStatement>(); // holds all statements corresponding to the time stamp of the first statement in the window
		ArrayList<ExportStatement> stopStatements = new ArrayList<ExportStatement>(); // holds all statements corresponding to the time stamp of the last statement in the window
		ArrayList<ExportStatement> beforeStatements = new ArrayList<ExportStatement>(); // holds all statements between (and excluding) the time stamp of the first statement in the window and the focal statement
		ArrayList<ExportStatement> afterStatements = new ArrayList<ExportStatement>(); // holds all statements between (and excluding) the focal statement and the time stamp of the last statement in the window
		if (this.timeWindow.equals("events")) {
			try (ProgressBar pb = new ProgressBar("Time window matrices", this.filteredStatements.size())) {
				pb.stepTo(0);
				if (this.windowSize < 2) {
					LogEvent l = new LogEvent(Logger.WARNING,
							"Time window size < 2 was chosen.",
							"When exporting a network, the time window size must be at least two events. With one statement event, there can be no ties in the network.");
					Dna.logger.log(l);
				}
				int iteratorStart, iteratorStop, i, j;
				int samples;
				for (int t = 0; t < this.filteredStatements.size(); t++) {
					int halfDuration = (int) Math.floor(this.windowSize / 2);
					iteratorStart = t - halfDuration;
					iteratorStop = t + halfDuration;

					startStatements.clear();
					stopStatements.clear();
					beforeStatements.clear();
					afterStatements.clear();
					if (iteratorStart >= 0 && iteratorStop < this.filteredStatements.size()) {
						for (i = 0; i < this.filteredStatements.size(); i++) {
							if (this.filteredStatements.get(i).getDateTime().equals(this.filteredStatements.get(iteratorStart).getDateTime())) {
								startStatements.add(this.filteredStatements.get(i));
							}
							if (this.filteredStatements.get(i).getDateTime().equals(this.filteredStatements.get(iteratorStop).getDateTime())) {
								stopStatements.add(this.filteredStatements.get(i));
							}
							if (this.filteredStatements.get(i).getDateTime().isAfter(this.filteredStatements.get(iteratorStart).getDateTime()) && i < t) {
								beforeStatements.add(this.filteredStatements.get(i));
							}
							if (this.filteredStatements.get(i).getDateTime().isBefore(this.filteredStatements.get(iteratorStop).getDateTime()) && i > t) {
								afterStatements.add(this.filteredStatements.get(i));
							}
						}
						if (startStatements.size() + beforeStatements.size() > halfDuration || stopStatements.size() + afterStatements.size() > halfDuration) {
							samples = 1; // this number should be larger than the one below, for example 10 (for 10 random combinations of start and stop statements)
						} else {
							samples = 1;
						}

						for (j = 0; j < samples; j++) {
							// add statements from start, before, after, and stop set to current window
							currentWindowStatements.clear();
							Collections.shuffle(startStatements);
							for (i = 0; i < halfDuration - beforeStatements.size(); i++) {
								currentWindowStatements.add(startStatements.get(i));
							}
							currentWindowStatements.addAll(beforeStatements);
							currentWindowStatements.add(this.filteredStatements.get(t));
							currentWindowStatements.addAll(afterStatements);
							Collections.shuffle(stopStatements);
							for (i = 0; i < halfDuration - afterStatements.size(); i++) {
								currentWindowStatements.add(stopStatements.get(i));
							}

							// convert time window to network and add to list
							if (currentWindowStatements.size() > 0) {
								int firstDocId = currentWindowStatements.get(0).getDocumentId();
								LocalDateTime first = null;
								for (i = 0; i < this.documents.size(); i++) {
									if (firstDocId == this.documents.get(i).getId()) {
										first = documents.get(i).getDateTime();
										break;
									}
								}
								int lastDocId = currentWindowStatements.get(currentWindowStatements.size() - 1).getDocumentId();
								LocalDateTime last = null;
								for (i = this.documents.size() - 1; i > -1; i--) {
									if (lastDocId == this.documents.get(i).getId()) {
										last = this.documents.get(i).getDateTime();
										break;
									}
								}
								Matrix m;
								if (this.networkType.equals("twomode")) {
									m = computeTwoModeMatrix(currentWindowStatements, first, last);
									m.setDateTime(this.filteredStatements.get(t).getDateTime());
									m.setNumStatements(currentWindowStatements.size());
									timeWindowMatrices.add(m);
								} else {
									if (qualifierAggregation.equals("congruence & conflict")) { // note: the networks are saved in alternating order and need to be disentangled
										m = computeOneModeMatrix(currentWindowStatements, "congruence", first, last);
										m.setDateTime(this.filteredStatements.get(t).getDateTime());
										m.setNumStatements(currentWindowStatements.size());
										timeWindowMatrices.add(m);
										m = computeOneModeMatrix(currentWindowStatements, "conflict", first, last);
										m.setDateTime(this.filteredStatements.get(t).getDateTime());
										m.setNumStatements(currentWindowStatements.size());
										timeWindowMatrices.add(m);
									} else {
										m = computeOneModeMatrix(currentWindowStatements, this.qualifierAggregation, first, last);
										m.setDateTime(this.filteredStatements.get(t).getDateTime());
										m.setNumStatements(currentWindowStatements.size());
										timeWindowMatrices.add(m);
									}

								}
							}
						}
					}
					pb.stepTo(t + 1);
				}
			}
		} else {
			try (ProgressBar pb = new ProgressBar("Time window matrices", 100)) {
				long percent = 0;
				pb.stepTo(percent);
				LocalDateTime startCalendar = this.startDateTime; // start of statement list
				LocalDateTime stopCalendar = this.stopDateTime; // end of statement list
				LocalDateTime currentTime = this.startDateTime; // current time while progressing through list of statements
				LocalDateTime windowStart; // start of the time window
				LocalDateTime windowStop; // end of the time window
				LocalDateTime iTime; // time of the statement to be potentially added to the time slice
				int addition = 0;
				while (!currentTime.isAfter(stopCalendar)) {
					LocalDateTime matrixTime = currentTime;
					windowStart = matrixTime;
					windowStop = matrixTime;
					currentWindowStatements.clear();
					addition = (int) Math.round(((double) windowSize - 1) / 2);
					if (timeWindow.equals("seconds")) {
						windowStart = windowStart.minusSeconds(addition);
						windowStop = windowStop.plusSeconds(addition);
						currentTime = currentTime.plusSeconds(1);
					} else if (timeWindow.equals("minutes")) {
						windowStart = windowStart.minusMinutes(addition);
						windowStop = windowStop.plusMinutes(addition);
						currentTime = currentTime.plusMinutes(1);
					} else if (timeWindow.equals("hours")) {
						windowStart = windowStart.minusHours(addition);
						windowStop = windowStop.plusHours(addition);
						currentTime = currentTime.plusHours(1);
					} else if (timeWindow.equals("days")) {
						windowStart = windowStart.minusDays(addition);
						windowStop = windowStop.plusDays(addition);
						currentTime = currentTime.plusDays(1);
					} else if (timeWindow.equals("weeks")) {
						windowStart = windowStart.minusWeeks(addition);
						windowStop = windowStop.plusWeeks(addition);
						currentTime = currentTime.plusWeeks(1);
					} else if (timeWindow.equals("months")) {
						windowStart = windowStart.minusMonths(addition);
						windowStop = windowStop.plusMonths(addition);
						currentTime = currentTime.plusMonths(1);
					} else if (timeWindow.equals("years")) {
						windowStart = windowStart.minusYears(addition);
						windowStop = windowStop.plusYears(addition);
						currentTime = currentTime.plusYears(1);
					}
					if (!windowStart.isBefore(startCalendar) && !windowStop.isAfter(stopCalendar)) {
						for (int i = 0; i < this.filteredStatements.size(); i++) {
							iTime = this.filteredStatements.get(i).getDateTime();
							if (!iTime.isBefore(windowStart) && !iTime.isAfter(windowStop)) {
								currentWindowStatements.add(this.filteredStatements.get(i));
							}
						}
						// if (currentWindowStatements.size() > 0) {
							Matrix m;
							if (this.networkType.equals("twomode")) {
								m = computeTwoModeMatrix(currentWindowStatements, windowStart, windowStop);
							} else {
								m = computeOneModeMatrix(currentWindowStatements, this.qualifierAggregation, windowStart, windowStop);
							}
							m.setDateTime(matrixTime);
							m.setNumStatements(currentWindowStatements.size());
							timeWindowMatrices.add(m);
						// }
					}
					percent = 100 * (currentTime.toEpochSecond(ZoneOffset.UTC) - startCalendar.toEpochSecond(ZoneOffset.UTC)) / (stopCalendar.toEpochSecond(ZoneOffset.UTC) - startCalendar.toEpochSecond(ZoneOffset.UTC));
					pb.stepTo(percent);
				}
			}
		}
		this.matrixResults = timeWindowMatrices;
	}
	
	/**
	 * Sets the qualifier aggregation value.
	 *
	 * @param qualifierAggregation the new value for qualifier aggregation
	 */
	public void setQualifierAggregation(String qualifierAggregation) {
		this.qualifierAggregation = qualifierAggregation;
	}

	/**
	 * Retrieves the data type associated with the specified variable.
	 *
	 * @param key the variable name whose associated data type is to be returned
	 * @return the data type associated with the specified variable, or null if the key does not exist
	 */
	public String getDataType(String key) {
		return this.dataTypes.get(key);
	}

	/**
	 * Get the computed network matrix results as an array list.
	 * 
	 * @return An array list of {@link Matrix Matrix} objects. If time window functionality was used, there are
	 * multiple matrices in the list, otherwise just one.
	 */
	public ArrayList<Matrix> getMatrixResults() {
		if (this.matrixResults == null) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Results have not been computed and could not be returned.",
					"The network matrix results were not computed and cannot be returned. A null object will be returned instead.");
			Dna.logger.log(l);
		}
		return this.matrixResults;
	}

	/**
	 * Get the computed network matrix results as an array.
	 *
	 * @return An array of {@link Matrix Matrix} objects. If time window functionality was used, there are
	 * multiple matrices in the list, otherwise just one.
	 */
	public Matrix[] getMatrixResultsArray() {
		if (this.matrixResults == null) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Results have not been computed and could not be returned.",
					"The network matrix results were not computed and cannot be returned. A null object will be returned instead.");
			Dna.logger.log(l);
		}
		return this.matrixResults.stream().toArray(Matrix[]::new);
	}

	/**
	 * Write results to file.
	 */
	public void exportToFile() {
		if (networkType.equals("eventlist") && fileFormat.equals("csv")) {
			eventCSV();
		} else if (fileFormat.equals("csv")) {
			exportCSV();
		} else if (fileFormat.equals("dl")) {
			exportDL();
		} else if (fileFormat.equals("graphml")) {
			exportGraphml();
		}
	}

	/**
	 * Write an event list to a CSV file. The event list contains all filtered
	 * statements including their IDs, date/time stamps, variable values, text,
	 * and document meta-data. There is one statement per row. The event list
	 * can be used for estimating relational event models.
	 */
	private void eventCSV() {
		try (ProgressBar pb = new ProgressBar("Exporting events", this.filteredStatements.size())) {
			pb.stepTo(0);
			String key;
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			try {
				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.outfile), "UTF-8"));
				out.write("\"statement_id\";\"time\"");
				for (int i = 0; i < statementType.getVariables().size(); i++) {
					out.write(";\"" + statementType.getVariables().get(i).getKey() + "\"");
				}
				out.write(";\"start_position\";\"stop_position\";\"text\";\"coder\";\"document_id\";\"document_title\";\"document_author\";\"document_source\";\"document_section\";\"document_type\"");
				ExportStatement s;
				for (int i = 0; i < this.filteredStatements.size(); i++) {
					s = this.filteredStatements.get(i);
					out.newLine();
					out.write(Integer.valueOf(s.getId()).toString()); // statement ID as a string
					out.write(";" + s.getDateTime().format(formatter));
					for (int j = 0; j < statementType.getVariables().size(); j++) {
						key = statementType.getVariables().get(j).getKey();
						if (this.dataTypes.get(key).equals("short text")) {
							out.write(";\"" + (((Entity) s.get(key)).getValue()).replaceAll(";", ",").replaceAll("\"", "'") + "\"");
						} else if (this.dataTypes.get(key).equals("long text")) {
							out.write(";\"" + ((String) s.get(key)).replaceAll(";", ",").replaceAll("\"", "'") + "\"");
						} else {
							out.write(";" + s.get(key));
						}
					}
					out.write(";" + Integer.valueOf(s.getStart()).toString());
					out.write(";" + Integer.valueOf(s.getStop()).toString());
					out.write(";\"" + s.getText().replaceAll(";", ",").replaceAll("\"", "'") + "\"");
					out.write(";" + Integer.valueOf(s.getCoderId()).toString());
					out.write(";" + s.getDocumentIdAsString());
					out.write(";\"" + s.getTitle().replaceAll(";", ",").replaceAll("\"", "'") + "\"");
					out.write(";\"" + s.getAuthor().replaceAll(";", ",").replaceAll("\"", "'") + "\"");
					out.write(";\"" + s.getSource().replaceAll(";", ",").replaceAll("\"", "'") + "\"");
					out.write(";\"" + s.getSection().replaceAll(";", ",").replaceAll("\"", "'") + "\"");
					out.write(";\"" + s.getType().replaceAll(";", ",").replaceAll("\"", "'") + "\"");
					pb.stepTo(i + 1);
				}
				out.close();
				LogEvent l = new LogEvent(Logger.MESSAGE,
						"Event list has been exported.",
						"Event list has been exported to \"" + this.outfile + "\".");
				Dna.logger.log(l);
			} catch (IOException e) {
				LogEvent l = new LogEvent(Logger.ERROR,
						"Error while saving event list as CSV file.",
						"Tried to save an event list to CSV file \"" + this.outfile + "\", but an error occurred. See stack trace.",
						e);
				Dna.logger.log(l);
			}
			pb.stepTo(this.filteredStatements.size());
		}
	}

	/**
	 * Export {@link Matrix Matrix} to a CSV matrix file.
	 */
	private void exportCSV() {
		try (ProgressBar pb = new ProgressBar("Exporting networks", this.matrixResults.size())) {
			pb.stepTo(0);
			String filename2;
			String filename1 = this.outfile.substring(0, this.outfile.length() - 4);
			String filename3 = this.outfile.substring(this.outfile.length() - 4, this.outfile.length());
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			for (int k = 0; k < this.matrixResults.size(); k++) {
				// assemble file name for time window networks if necessary
				String filename = this.outfile;
				if (this.matrixResults.size() > 1) {
					filename2 = " " + String.format("%0" + String.valueOf(this.matrixResults.size()).length() + "d", k + 1) + " " + this.matrixResults.get(k).getDateTime().format(formatter);
					filename = filename1 + filename2 + filename3;
				}

				// get current data
				String[] rn = this.matrixResults.get(k).getRowNames();
				String[] cn = this.matrixResults.get(k).getColumnNames();
				int nr = rn.length;
				int nc = cn.length;
				double[][] mat = this.matrixResults.get(k).getMatrix();

				// export
				try {
					BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "UTF8"));
					out.write("\"\"");
					for (int i = 0; i < nc; i++) {
						out.write(";\"" + cn[i].replaceAll("\"", "'") + "\"");
					}
					for (int i = 0; i < nr; i++) {
						out.newLine();
						out.write("\"" + rn[i].replaceAll("\"", "'") + "\"");
						for (int j = 0; j < nc; j++) {
							if (this.matrixResults.get(k).getInteger()) {
								out.write(";" + (int) mat[i][j]);
							} else {
								out.write(";" + String.format(new Locale("en"), "%.6f", mat[i][j])); // six decimal places
							}
						}
					}
					out.close();
				} catch (IOException e) {
					LogEvent l = new LogEvent(Logger.ERROR,
							"Error while saving matrix as CSV file.",
							"Tried to save a matrix to CSV file \"" + this.outfile + "\", but an error occurred. See stack trace.",
							e);
					Dna.logger.log(l);
				}
				pb.stepTo(k + 1);
			}
			pb.stepTo(this.matrixResults.size());
		}
	}

	/**
	 * Export network to a DL fullmatrix file for the software UCINET.
	 */
	private void exportDL() {
		try (ProgressBar pb = new ProgressBar("Exporting networks", this.matrixResults.size())) {
			pb.stepTo(0);
			String filename2;
			String filename1 = this.outfile.substring(0, this.outfile.length() - 4);
			String filename3 = this.outfile.substring(this.outfile.length() - 4, this.outfile.length());
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			for (int k = 0; k < this.matrixResults.size(); k++) {
				// assemble file name for time window networks if necessary
				String filename = this.outfile;
				if (this.matrixResults.size() > 1) {
					filename2 = " " + String.format("%0" + String.valueOf(this.matrixResults.size()).length() + "d", k + 1) + " " + this.matrixResults.get(k).getDateTime().format(formatter);
					filename = filename1 + filename2 + filename3;
				}

				// get current data
				String[] rn = this.matrixResults.get(k).getRowNames();
				String[] cn = this.matrixResults.get(k).getColumnNames();
				int nr = rn.length;
				int nc = cn.length;
				double[][] mat = this.matrixResults.get(k).getMatrix();

				// export
				try {
					BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "UTF8"));
					out.write("dl ");
					if (this.networkType.equals("onemode")) {
						out.write("n = " + nr);
					} else if (this.networkType.equals("twomode")) {
						out.write("nr = " + nr + ", nc = " + nc);
					}
					out.write(", format = fullmatrix");
					out.newLine();
					if (this.networkType.equals("twomode")) {
						out.write("row labels:");
					} else {
						out.write("labels:");
					}
					for (int i = 0; i < nr; i++) {
						out.newLine();
						out.write("\"" + rn[i].replaceAll("\"", "'").replaceAll("'", "") + "\"");
					}
					if (this.networkType.equals("twomode")) {
						out.newLine();
						out.write("col labels:");
						for (int i = 0; i < nc; i++) {
							out.newLine();
							out.write("\"" + cn[i].replaceAll("\"", "'").replaceAll("'", "") + "\"");
						}
					}
					out.newLine();
					out.write("data:");
					for (int i = 0; i < nr; i++) {
						out.newLine();
						for (int j = 0; j < nc; j++) {
							if (this.matrixResults.get(k).getInteger()) {
								out.write(" " + (int) mat[i][j]);
							} else {
								out.write(" " + String.format(new Locale("en"), "%.6f", mat[i][j]));
							}
						}
					}
					out.close();
				} catch (IOException e) {
					LogEvent l = new LogEvent(Logger.ERROR,
							"Error while saving DL fullmatrix file.",
							"Tried to save a matrix to DL fullmatrix file \"" + this.outfile + "\", but an error occurred. See stack trace.",
							e);
					Dna.logger.log(l);
				}
				pb.stepTo(k + 1);
			}
			pb.stepTo(this.matrixResults.size());
		}
	}

	/**
	 * Export filter for graphML files.
	 */
	private void exportGraphml() {
		try (ProgressBar pb = new ProgressBar("Exporting networks", this.matrixResults.size())) {
			pb.stepTo(0);

			// set up file name components for time window (in case this will be required later)
			String filename2;
			String filename1 = this.outfile.substring(0, this.outfile.length() - 4);
			String filename3 = this.outfile.substring(this.outfile.length() - 4, this.outfile.length());
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

			// get variable IDs for variable 1 and variable 2
			int variable1Id = this.statementType.getVariables()
					.stream()
					.filter(v -> v.getKey().equals(this.variable1))
					.mapToInt(v -> v.getVariableId())
					.findFirst()
					.getAsInt();
			int variable2Id = this.statementType.getVariables()
					.stream()
					.filter(v -> v.getKey().equals(this.variable2))
					.mapToInt(v -> v.getVariableId())
					.findFirst()
					.getAsInt();

			// get attribute variable names for variable 1 and variable 2
			ArrayList<String> attributeVariables1 = Dna.sql.getAttributeVariables(variable1Id);
			ArrayList<String> attributeVariables2 = Dna.sql.getAttributeVariables(variable2Id);

			// get entities with attribute values for variable 1 and variable 2 and save in hash maps
			ArrayList<Integer> variableIds = new ArrayList<Integer>();
			variableIds.add(variable1Id);
			variableIds.add(variable2Id);
			ArrayList<ArrayList<Entity>> entities = Dna.sql.getEntities(variableIds, true);
			HashMap<String, Entity> entityMap1 = new HashMap<String, Entity>();
			HashMap<String, Entity> entityMap2 = new HashMap<String, Entity>();
			entities.get(0).stream().forEach(entity -> entityMap1.put(entity.getValue(), entity));
			entities.get(1).stream().forEach(entity -> entityMap2.put(entity.getValue(), entity));

			Matrix m;
			for (int k = 0; k < this.matrixResults.size(); k++) {
				m = this.matrixResults.get(k);

				// assemble file name for time window networks if necessary
				String filename = this.outfile;
				if (this.matrixResults.size() > 1) {
					filename2 = " " + String.format("%0" + String.valueOf(this.matrixResults.size()).length() + "d", k + 1) + " " + m.getDateTime().format(formatter);
					filename = filename1 + filename2 + filename3;
				}

				// frequencies
				String[] values1 = this.retrieveValues(this.filteredStatements, this.variable1, this.variable1Document);
				String[] values2 = this.retrieveValues(this.filteredStatements, this.variable2, this.variable2Document);
				int[] frequencies1 = this.countFrequencies(values1, m.getRowNames());
				int[] frequencies2 = this.countFrequencies(values2, m.getColumnNames());

				// join names, frequencies, and variable names into long arrays for both modes
				String[] rn = this.matrixResults.get(k).getRowNames();
				String[] cn = this.matrixResults.get(k).getColumnNames();
				String[] names;
				String[] variables;
				int[] frequencies;
				if (this.networkType.equals("twomode")) {
					names = new String[rn.length + cn.length];
					variables = new String[names.length];
					frequencies = new int[names.length];
				} else {
					names = new String[rn.length];
					variables = new String[rn.length];
					frequencies = new int[rn.length];
				}
				for (int i = 0; i < rn.length; i++) {
					names[i] = rn[i];
					variables[i] = this.variable1;
					frequencies[i] = frequencies1[i];
				}
				if (this.networkType.equals("twomode")) {
					for (int i = 0; i < cn.length; i++) {
						names[i + rn.length] = cn[i];
						variables[i + rn.length] = this.variable2;
						frequencies[i + rn.length] = frequencies2[i];
					}
				}

				// get id and color arrays
				int[] id = Arrays.stream(rn).mapToInt(s -> entityMap1.get(s).getId()).toArray();
				String[] color = Arrays.stream(rn).map(s -> {
					Color col = entityMap1.get(s).getColor();
					return String.format("#%02X%02X%02X", col.getRed(), col.getGreen(), col.getBlue());
				}).toArray(String[]::new);
				if (networkType.equals("twomode")) {
					id = IntStream.concat(IntStream.of(id), Arrays.stream(cn).mapToInt(s -> entityMap2.get(s).getId())).toArray();
					color = Stream.concat(Stream.of(color), Arrays.stream(cn).map(s -> {
						Color col = entityMap2.get(s).getColor();
						return String.format("#%02X%02X%02X", col.getRed(), col.getGreen(), col.getBlue());
					})).toArray(String[]::new);
				}

				// set up graph structure
				Namespace xmlns = Namespace.getNamespace("http://graphml.graphdrawing.org/xmlns");
				Element graphml = new Element("graphml", xmlns);
				Namespace visone = Namespace.getNamespace("visone", "http://visone.info/xmlns");
				graphml.addNamespaceDeclaration(visone);
				Namespace xsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
				graphml.addNamespaceDeclaration(xsi);
				Namespace yNs = Namespace.getNamespace("y", "http://www.yworks.com/xml/graphml");
				graphml.addNamespaceDeclaration(yNs);
				Attribute attSchema = new Attribute("schemaLocation", "http://graphml.graphdrawing.org/xmlns/graphml http://www.yworks.com/xml/schema/graphml/1.0/ygraphml.xsd ", xsi);
				graphml.setAttribute(attSchema);
				org.jdom.Document document = new org.jdom.Document(graphml);

				Comment dataSchema = new Comment(" data schema ");
				graphml.addContent(dataSchema);

				Element keyVisoneNode = new Element("key", xmlns);
				keyVisoneNode.setAttribute(new Attribute("for", "node"));
				keyVisoneNode.setAttribute(new Attribute("id", "d0"));
				keyVisoneNode.setAttribute(new Attribute("yfiles.type", "nodegraphics"));
				graphml.addContent(keyVisoneNode);

				Element keyVisoneEdge = new Element("key", xmlns);
				keyVisoneEdge.setAttribute(new Attribute("for", "edge"));
				keyVisoneEdge.setAttribute(new Attribute("id", "e0"));
				keyVisoneEdge.setAttribute(new Attribute("yfiles.type", "edgegraphics"));
				graphml.addContent(keyVisoneEdge);

				Element keyVisoneGraph = new Element("key", xmlns);
				keyVisoneGraph.setAttribute(new Attribute("for", "graph"));
				keyVisoneGraph.setAttribute(new Attribute("id", "prop"));
				keyVisoneGraph.setAttribute(new Attribute("visone.type", "properties"));
				graphml.addContent(keyVisoneGraph);

				Element keyId = new Element("key", xmlns);
				keyId.setAttribute(new Attribute("id", "id"));
				keyId.setAttribute(new Attribute("for", "node"));
				keyId.setAttribute(new Attribute("attr.name", "id"));
				keyId.setAttribute(new Attribute("attr.type", "string"));
				graphml.addContent(keyId);

				Element keyName = new Element("key", xmlns);
				keyName.setAttribute(new Attribute("id", "name"));
				keyName.setAttribute(new Attribute("for", "node"));
				keyName.setAttribute(new Attribute("attr.name", "name"));
				keyName.setAttribute(new Attribute("attr.type", "string"));
				graphml.addContent(keyName);

				ArrayList<String> addedAttributes = new ArrayList<String>();
				attributeVariables1.stream().forEach(v -> {
					Element keyAttribute = new Element("key", xmlns);
					keyAttribute.setAttribute(new Attribute("id", v));
					keyAttribute.setAttribute(new Attribute("for", "node"));
					keyAttribute.setAttribute(new Attribute("attr.name", v));
					keyAttribute.setAttribute(new Attribute("attr.type", "string"));
					graphml.addContent(keyAttribute);
					addedAttributes.add(v);
				});
				if (this.networkType.equals("twomode")) {
					attributeVariables2.stream().forEach(v -> {
						if (!addedAttributes.contains(v)) {
							Element keyAttribute = new Element("key", xmlns);
							keyAttribute.setAttribute(new Attribute("id", v));
							keyAttribute.setAttribute(new Attribute("for", "node"));
							keyAttribute.setAttribute(new Attribute("attr.name", v));
							keyAttribute.setAttribute(new Attribute("attr.type", "string"));
							graphml.addContent(keyAttribute);
							addedAttributes.add(v);
						}
					});
				}

				Element keyVariable = new Element("key", xmlns);
				keyVariable.setAttribute(new Attribute("id", "variable"));
				keyVariable.setAttribute(new Attribute("for", "node"));
				keyVariable.setAttribute(new Attribute("attr.name", "variable"));
				keyVariable.setAttribute(new Attribute("attr.type", "string"));
				graphml.addContent(keyVariable);

				Element keyFrequency = new Element("key", xmlns);
				keyFrequency.setAttribute(new Attribute("id", "frequency"));
				keyFrequency.setAttribute(new Attribute("for", "node"));
				keyFrequency.setAttribute(new Attribute("attr.name", "frequency"));
				keyFrequency.setAttribute(new Attribute("attr.type", "int"));
				graphml.addContent(keyFrequency);

				Element keyWeight = new Element("key", xmlns);
				keyWeight.setAttribute(new Attribute("id", "weight"));
				keyWeight.setAttribute(new Attribute("for", "edge"));
				keyWeight.setAttribute(new Attribute("attr.name", "weight"));
				keyWeight.setAttribute(new Attribute("attr.type", "double"));
				graphml.addContent(keyWeight);

				Element graphElement = new Element("graph", xmlns);
				graphElement.setAttribute(new Attribute("edgedefault", "undirected"));

				graphElement.setAttribute(new Attribute("id", "DNA"));
				int numEdges = rn.length * cn.length;
				if (this.networkType.equals("onemode")) {
					numEdges = (numEdges - rn.length) / 2;
				}
				int numNodes = rn.length;
				if (this.networkType.equals("twomode")) {
					numNodes = numNodes + cn.length;
				}
				graphElement.setAttribute(new Attribute("parse.edges", String.valueOf(numEdges)));
				graphElement.setAttribute(new Attribute("parse.nodes", String.valueOf(numNodes)));
				graphElement.setAttribute(new Attribute("parse.order", "free"));
				Element properties = new Element("data", xmlns);
				properties.setAttribute(new Attribute("key", "prop"));
				Element labelAttribute = new Element("labelAttribute", visone);
				labelAttribute.setAttribute("edgeLabel", "weight");
				labelAttribute.setAttribute("nodeLabel", "name");
				properties.addContent(labelAttribute);
				graphElement.addContent(properties);

				// add nodes
				Comment nodes = new Comment(" nodes ");
				graphElement.addContent(nodes);

				for (int i = 0; i < names.length; i++) {
					Element node = new Element("node", xmlns);
					node.setAttribute(new Attribute("id", "n" + id[i]));

					Element idElement = new Element("data", xmlns);
					idElement.setAttribute(new Attribute("key", "id"));
					idElement.setText(String.valueOf(id[i]));
					node.addContent(idElement);

					Element nameElement = new Element("data", xmlns);
					nameElement.setAttribute(new Attribute("key", "name"));
					nameElement.setText(names[i]);
					node.addContent(nameElement);

					for (int j = 0; j < attributeVariables1.size(); j++) {
						if (i < rn.length) { // first mode: rows
							Element element = new Element("data", xmlns);
							element.setAttribute(new Attribute("key", attributeVariables1.get(j)));
							element.setText(entityMap1.get(names[i]).getAttributeValues().get(attributeVariables1.get(j)));
							node.addContent(element);
						}
					}
					if (this.networkType.equals("twomode")) {
						for (int j = 0; j < attributeVariables2.size(); j++) {
							if (i >= rn.length) { // second mode: columns
								Element element = new Element("data", xmlns);
								element.setAttribute(new Attribute("key", attributeVariables2.get(j)));
								element.setText(entityMap2.get(names[i]).getAttributeValues().get(attributeVariables2.get(j)));
								node.addContent(element);
							}
						}
					}

					Element variableElement = new Element("data", xmlns);
					variableElement.setAttribute(new Attribute("key", "variable"));
					variableElement.setText(variables[i]);
					node.addContent(variableElement);

					Element frequency = new Element("data", xmlns);
					frequency.setAttribute(new Attribute("key", "frequency"));
					frequency.setText(String.valueOf(frequencies[i]));
					node.addContent(frequency);

					Element vis = new Element("data", xmlns);
					vis.setAttribute(new Attribute("key", "d0"));
					Element visoneShapeNode = new Element("shapeNode", visone);
					Element yShapeNode = new Element("ShapeNode", yNs);
					Element geometry = new Element("Geometry", yNs);
					geometry.setAttribute(new Attribute("height", "20.0"));
					geometry.setAttribute(new Attribute("width", "20.0"));
					geometry.setAttribute(new Attribute("x", String.valueOf(Math.random() * 800)));
					geometry.setAttribute(new Attribute("y", String.valueOf(Math.random() * 600)));
					yShapeNode.addContent(geometry);
					Element fill = new Element("Fill", yNs);
					fill.setAttribute(new Attribute("color", color[i]));

					fill.setAttribute(new Attribute("transparent", "false"));
					yShapeNode.addContent(fill);
					Element borderStyle = new Element("BorderStyle", yNs);
					borderStyle.setAttribute(new Attribute("color", "#000000"));
					borderStyle.setAttribute(new Attribute("type", "line"));
					borderStyle.setAttribute(new Attribute("width", "1.0"));
					yShapeNode.addContent(borderStyle);

					Element nodeLabel = new Element("NodeLabel", yNs);
					nodeLabel.setAttribute(new Attribute("alignment", "center"));
					nodeLabel.setAttribute(new Attribute("autoSizePolicy", "content"));
					nodeLabel.setAttribute(new Attribute("backgroundColor", "#FFFFFF"));
					nodeLabel.setAttribute(new Attribute("fontFamily", "Dialog"));
					nodeLabel.setAttribute(new Attribute("fontSize", "12"));
					nodeLabel.setAttribute(new Attribute("fontStyle", "plain"));
					nodeLabel.setAttribute(new Attribute("hasLineColor", "false"));
					nodeLabel.setAttribute(new Attribute("height", "19.0"));
					nodeLabel.setAttribute(new Attribute("modelName", "eight_pos"));
					nodeLabel.setAttribute(new Attribute("modelPosition", "n"));
					nodeLabel.setAttribute(new Attribute("textColor", "#000000"));
					nodeLabel.setAttribute(new Attribute("visible", "true"));
					nodeLabel.setText(names[i]);
					yShapeNode.addContent(nodeLabel);

					Element shape = new Element("Shape", yNs);
					if (i < rn.length) {
						shape.setAttribute(new Attribute("type", "ellipse"));
					} else {
						shape.setAttribute(new Attribute("type", "roundrectangle"));
					}
					yShapeNode.addContent(shape);
					visoneShapeNode.addContent(yShapeNode);
					vis.addContent(visoneShapeNode);
					node.addContent(vis);

					graphElement.addContent(node);
				}

				// add edges
				Comment edges = new Comment(" edges ");
				graphElement.addContent(edges);
				for (int i = 0; i < rn.length; i++) {
					for (int j = 0; j < cn.length; j++) {
						if (m.getMatrix()[i][j] != 0.0 && (this.networkType.equals("twomode") || (this.networkType.equals("onemode") && i < j))) {  // only lower triangle is used for one-mode networks
							Element edge = new Element("edge", xmlns);

							int currentId = id[i];
							edge.setAttribute(new Attribute("source", "n" + String.valueOf(currentId)));
							if (this.networkType.equals("twomode")) {
								currentId = id[j + rn.length];
							} else {
								currentId = id[j];
							}
							edge.setAttribute(new Attribute("target", "n" + String.valueOf(currentId)));

							Element weight = new Element("data", xmlns);
							weight.setAttribute(new Attribute("key", "weight"));
							weight.setText(String.valueOf(m.getMatrix()[i][j]));
							edge.addContent(weight);

							Element visEdge = new Element("data", xmlns);
							visEdge.setAttribute("key", "e0");
							Element visPolyLineEdge = new Element("polyLineEdge", visone);
							Element yPolyLineEdge = new Element("PolyLineEdge", yNs);

							Element yLineStyle = new Element("LineStyle", yNs);
							if (qualifierAggregation.equals("combine") && dataTypes.get(qualifier).equals("boolean")) {
								if (m.getMatrix()[i][j] == 1.0) {
									yLineStyle.setAttribute("color", "#00ff00");
								} else if (m.getMatrix()[i][j] == 2.0) {
									yLineStyle.setAttribute("color", "#ff0000");
								} else if (m.getMatrix()[i][j] == 3.0) {
									yLineStyle.setAttribute("color", "#0000ff");
								}
							} else if (qualifierAggregation.equals("subtract")) {
								if (m.getMatrix()[i][j] < 0) {
									yLineStyle.setAttribute("color", "#ff0000");
								} else if (m.getMatrix()[i][j] > 0) {
									yLineStyle.setAttribute("color", "#00ff00");
								}
							} else if (qualifierAggregation.equals("conflict")) {
								yLineStyle.setAttribute("color", "#ff0000");
							} else if (qualifierAggregation.equals("congruence")) {
								yLineStyle.setAttribute("color", "#00ff00");
							} else {
								yLineStyle.setAttribute("color", "#000000");
							}
							yLineStyle.setAttribute(new Attribute("type", "line"));
							yLineStyle.setAttribute(new Attribute("width", "2.0"));
							yPolyLineEdge.addContent(yLineStyle);
							visPolyLineEdge.addContent(yPolyLineEdge);
							visEdge.addContent(visPolyLineEdge);
							edge.addContent(visEdge);

							graphElement.addContent(edge);
						}
					}
				}

				graphml.addContent(graphElement);

				// write to file
				File dnaFile = new File(filename);
				try {
					FileOutputStream outStream = new FileOutputStream(dnaFile);
					XMLOutputter outToFile = new XMLOutputter();
					Format format = Format.getPrettyFormat();
					format.setEncoding("utf-8");
					outToFile.setFormat(format);
					outToFile.output(document, outStream);
					outStream.flush();
					outStream.close();
				} catch (IOException e) {
					LogEvent l = new LogEvent(Logger.ERROR,
							"Error while saving visone graphml file.",
							"Tried to save a matrix to graphml file \"" + dnaFile + "\", but an error occurred. See stack trace.",
							e);
					Dna.logger.log(l);
				}
				pb.stepTo(k + 1);
			}
			pb.stepTo(this.matrixResults.size());
		}
	}

	/**
	 * Return original (unfiltered) statements.
	 *
	 * @return Original (unfiltered) statements.
	 */
	public ArrayList<ExportStatement> getOriginalStatements() {
		return this.originalStatements;
	}

	/**
	 * Return filtered statements.
	 *
	 * @return Filtered statements.
	 */
	public ArrayList<ExportStatement> getFilteredStatements() {
		return this.filteredStatements;
	}

	/**
	 * Compute data for creating a barplot with value frequencies by qualifier value.
	 *
	 * @return Barplot data for the filtered statements.
	 */
	public BarplotResult generateBarplotData() {
		// what variable ID corresponds to variable 1?
		int variableId = this.statementType.getVariables()
				.stream()
				.filter(v -> v.getKey().equals(this.variable1))
				.mapToInt(v -> v.getVariableId())
				.findFirst()
				.getAsInt();

		// what attribute variables exist for this variable?
		String[] attributeVariables = Stream.concat(Stream.of("Color"), Dna.sql.getAttributeVariables(variableId).stream()).toArray(String[]::new); // include "color" as first element

		// extract distinct entities from filtered statements
		Set<Integer> nameSet = new HashSet<>();
		ArrayList<Entity> entities = this.filteredStatements
				.stream()
				.map(s -> (Entity) s.get(this.variable1))
				.filter(e -> nameSet.add(e.getId())) // .distinct() has a bug, so add to a name set instead
				.sorted()
				.collect(Collectors.toCollection(ArrayList::new));
		String[] values = entities
				.stream()
				.map(e -> e.getValue())
				.toArray(String[]::new);

		// create attribute 2D String array (entity label x (color + attribute variable))
		String[][] attributes = new String[entities.size()][attributeVariables.length]; // attribute variables including "color" as first element
		String[] colors = entities
				.stream()
				.map(e -> String.format("#%02X%02X%02X", e.getColor().getRed(), e.getColor().getGreen(), e.getColor().getBlue()))
				.toArray(String[]::new);
		for (int i = 0; i < entities.size(); i ++) {
			attributes[i][0] = colors[i];
			for (int j = 1; j < attributeVariables.length; j++) {
				attributes[i][j] = entities.get(i).getAttributeValues().get(attributeVariables[j]);
			}
		}

		// create an int array of all distinct qualifier values that occur in at least one statement
		int[] intScale = new int[] {1};
		if (this.qualifier != null) {
			intScale = new int[] {0, 1};
			boolean integer = this.statementType.getVariables()
					.stream()
					.filter(v -> v.getKey().equals(this.qualifier))
					.map(v -> v.getDataType().equals("integer"))
					.findFirst()
					.get();
			if (integer) {
				intScale = this.filteredStatements
						.stream()
						.mapToInt(s -> (int) s.get(this.qualifier))
						.distinct()
						.sorted()
						.toArray();
			}
		}


		// count qualifier occurrences per value
		int[][] counts = new int[entities.size()][intScale.length];
		for (int i = 0; i < entities.size(); i++) {
			for (int j = 0; j < intScale.length; j++) {
				final int entityId = entities.get(i).getId();
				final int q = intScale[j];
				counts[i][j] = (int) this.filteredStatements
						.stream()
						.filter(s -> ((Entity) s.get(this.variable1)).getId() == entityId && (this.qualifier == null || (int) s.get(this.qualifier) == q))
						.count();
			}
		}

		// assemble and return data
		return new BarplotResult(this.variable1, values, counts, attributes, intScale, attributeVariables);
	}

	/**
	 * Get the current iteration {@code t} of the simulated annealing algorithm.
	 *
	 * @return Current iteration {@code t}.
	 */
	public int getCurrentT() {
		return this.t;
	}

	/**
	 * Set the current iteration {@code t} of the simulated annealing algorithm.
	 *
	 * @return Current iteration {@code t}.
	 */
	public void setCurrentT(int t) {
		this.t = t;
	}

	/**
	 * Reduce the dimensions of a candidate matrix with all isolate nodes to the dimensions of the full matrix, which
	 * does not contain isolate nodes.
	 *
	 * @param candidateMatrix The candidate matrix with isolates (to be reduced to smaller dimensions).
	 * @param fullLabels The node labels of the full matrix without isolates.
	 * @return A reduced candidate matrix with the same dimensions as the full matrix and the same node order.
	 */
	private Matrix reduceCandidateMatrix(Matrix candidateMatrix, String[] fullLabels) {
		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		for (int i = 0; i < fullLabels.length; i++) {
			for (int j = 0; j < candidateMatrix.getRowNames().length; j++) {
				if (fullLabels[i].equals(candidateMatrix.getRowNames()[j])) {
					map.put(i, j);
				}
			}
		}
		double[][] mat = new double[fullLabels.length][fullLabels.length];
		for (int i = 0; i < fullLabels.length; i++) {
			for (int j = 0; j < fullLabels.length; j++) {
				mat[i][j] = candidateMatrix.getMatrix()[map.get(i)][map.get(j)];
			}
		}
		candidateMatrix.setMatrix(mat);
		candidateMatrix.setRowNames(fullLabels);
		candidateMatrix.setColumnNames(fullLabels);
		return candidateMatrix;
	}

	/**
	 * Compute matrix after final backbone iteration, collect results, and save in class.
	 */
	public void saveSimulatedAnnealingBackboneResult(boolean penalty) {
		Collections.sort(finalBackboneList);
		Collections.sort(finalRedundantList);

		// create redundant matrix
		ArrayList<ExportStatement> redundantStatementList = this.filteredStatements
				.stream()
				.filter(s -> currentRedundantList.contains(((Entity) s.get(this.variable2)).getValue()))
				.collect(Collectors.toCollection(ArrayList::new));
		Matrix redundantMatrix = this.computeOneModeMatrix(redundantStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime);

		String method = "penalty";
		if (!penalty) {
			method = "fixed";
			p = 0;
		}
		this.simulatedAnnealingBackboneResult = new SimulatedAnnealingBackboneResult(method,
				finalBackboneList.toArray(String[]::new),
				finalRedundantList.toArray(String[]::new),
				spectralLoss(eigenvaluesFull, eigenvaluesCurrent),
				spectralLoss(eigenvaluesFull, computeNormalizedEigenvalues(redundantMatrix.getMatrix(), "ojalgo")),
				p,
				T,
				temperatureLog.stream().mapToDouble(v -> v.doubleValue()).toArray(),
				acceptanceProbabilityLog.stream().mapToDouble(v -> v.doubleValue()).toArray(),
				acceptedLog.stream().mapToInt(v -> v.intValue()).toArray(),
				penalizedBackboneLossLog.stream().mapToDouble(v -> v.doubleValue()).toArray(),
				proposedBackboneSizeLog.stream().mapToInt(v -> v.intValue()).toArray(),
				acceptedBackboneSizeLog.stream().mapToInt(v -> v.intValue()).toArray(),
				finalBackboneSizeLog.stream().mapToInt(v -> v.intValue()).toArray(),
				acceptanceRatioLastHundredIterationsLog.stream().mapToDouble(v -> v.doubleValue()).toArray(),
				fullMatrix.getMatrix(),
				currentMatrix.getMatrix(),
				redundantMatrix.getMatrix(),
				fullMatrix.getRowNames(),
				fullMatrix.getStart().toEpochSecond(ZoneOffset.UTC),
				fullMatrix.getStop().toEpochSecond(ZoneOffset.UTC),
				fullMatrix.getNumStatements());
		this.nestedBackboneResult = null;
	}

	/**
	 * Get the penalty backbone result that is saved in the class.
	 *
	 * @return The penalty backbone result (can be null if backbone function has not been executed).
	 */
	public SimulatedAnnealingBackboneResult getSimulatedAnnealingBackboneResult() {
		return this.simulatedAnnealingBackboneResult;
	}

	/**
	 * Use tools from the {@code ojalgo} library to compute eigenvalues of a symmetric matrix.
	 *
	 * @param matrix The matrix as a two-dimensional double array.
	 * @param library The linear algebra Java library to use as a back-end: {@code "ojalgo"} or {@code "apache"}.
	 * @return One-dimensional double array of eigenvalues.
	 */
	private double[] computeNormalizedEigenvalues(double[][] matrix, String library) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[0].length; j++) {
				if (matrix[i][j] < 0) {
					matrix[i][j] = 0.0;
				}
			}
		}
		double[] eigenvalues;
		if (library.equals("apache")) {
			RealMatrix realMatrix = new Array2DRowRealMatrix(matrix); // create a real matrix from the 2D array
			EigenDecomposition decomposition = new EigenDecomposition(realMatrix); // perform eigen decomposition
			eigenvalues = decomposition.getRealEigenvalues(); // get the real parts of the eigenvalues
			// normalize the eigenvalues
			double eigenvaluesSum = Arrays.stream(eigenvalues).sum();
			if (eigenvaluesSum > 0.0) {
				for (int i = 0; i < eigenvalues.length; i++) {
					eigenvalues[i] /= eigenvaluesSum;
				}
			}
		} else if (library.equals("ojalgo")) {
			Primitive64Matrix matrixPrimitive = Primitive64Matrix.FACTORY.rows(matrix); // create matrix
			DenseArray<Double> rowSums = Primitive64Array.FACTORY.make(matrix.length); // container for row sums
			matrixPrimitive.reduceRows(Aggregator.SUM, rowSums); // populate row sums into rowSums
			Primitive64Matrix.SparseReceiver sr = Primitive64Matrix.FACTORY.makeSparse(matrix.length, matrix.length); // container for degree matrix
			sr.fillDiagonal(rowSums); // put row sums onto diagonal
			Primitive64Matrix laplacian = sr.get(); // put row sum container into a new degree matrix (the future Laplacian matrix)
			laplacian.subtract(matrixPrimitive); // subtract adjacency matrix from degree matrix to create Laplacian matrix
			Eigenvalue<Double> eig = Eigenvalue.PRIMITIVE.make(laplacian); // eigenvalues
			eig.decompose(laplacian); // decomposition
			eigenvalues = eig.getEigenvalues().toRawCopy1D(); // extract eigenvalues and convert to double[]
			double eigenvaluesSum = Arrays.stream(eigenvalues).sum(); // compute sum of eigenvalues
			if (eigenvaluesSum > 0.0) {
				eigenvalues = DoubleStream.of(eigenvalues).map(v -> v / eigenvaluesSum).toArray(); // normalize/scale to one
			}
		} else {
			eigenvalues = new double[matrix.length]; // return zeroes if library not recognized; don't log error because it would be very slow
		}
		return eigenvalues;
	}

	/**
	 * Compute penalized Euclidean spectral distance.
	 *
	 * @param eigenvalues1 Normalized eigenvalues of the full matrix.
	 * @param eigenvalues2 Normalized eigenvalues of the current or candidate matrix.
	 * @param p The penalty parameter. Typical values could be {@code 5.5}, {@code 7.5}, or {@code 12}, for example.
	 * @param candidateBackboneSize The number of entities in the current or candidate backbone.
	 * @param numEntitiesTotal The number of second-mode entities (e.g., concepts) in total.
	 * @return Penalized loss.
	 */
	private double penalizedLoss(double[] eigenvalues1, double[] eigenvalues2, double p, int candidateBackboneSize, int numEntitiesTotal) {
		double distance = 0.0; // Euclidean spectral distance
		for (int i = 0; i < eigenvalues1.length; i++) {
			distance = distance + Math.sqrt((eigenvalues1[i] - eigenvalues2[i]) * (eigenvalues1[i] - eigenvalues2[i]));
		}
		double penalty = Math.exp(-p * (((double) (numEntitiesTotal - candidateBackboneSize)) / ((double) numEntitiesTotal))); // compute penalty factor
		return distance * penalty; // return penalised distance
	}

	/**
	 * Write the backbone results to a JSON or XML file
	 *
	 * @param filename File name with absolute path as a string.
	 */
	public void writeBackboneToFile(String filename) {
		File file = new File(filename);
		String s = "";

		if (filename.toLowerCase().endsWith(".xml")) {
			XStream xstream = new XStream(new StaxDriver());
			xstream.processAnnotations(SimulatedAnnealingBackboneResult.class);
			StringWriter stringWriter = new StringWriter();
			if (this.nestedBackboneResult != null) {
				xstream.marshal(this.nestedBackboneResult, new PrettyPrintWriter(stringWriter));
			} else if (this.simulatedAnnealingBackboneResult != null) {
				xstream.marshal(this.simulatedAnnealingBackboneResult, new PrettyPrintWriter(stringWriter));
			}
			s = stringWriter.toString();
		} else if (filename.toLowerCase().endsWith(".json")) {
			Gson prettyGson = new GsonBuilder()
					.setPrettyPrinting()
					.serializeNulls()
					.disableHtmlEscaping()
					.create();
			if (this.nestedBackboneResult != null) {
				s = prettyGson.toJson(this.nestedBackboneResult);
			} else if (this.simulatedAnnealingBackboneResult != null) {
				s = prettyGson.toJson(this.simulatedAnnealingBackboneResult);
			}
		}
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.write(s);
			LogEvent l = new LogEvent(Logger.MESSAGE,
					"Backbone result was saved to disk.",
					"Backbone result was saved to file: " + filename + ".");
			Dna.logger.log(l);
		} catch (IOException exception) {
			LogEvent l = new LogEvent(Logger.ERROR,
					"Backbone result could not be saved to disk.",
					"Attempted to save backbone results to file: \"" + filename + "\". The file saving operation did not work, possibly because the file could not be written to disk or because the results could not be converted to the final data format.",
					exception);
			Dna.logger.log(l);
		}
	}

	/**
	 * Compute penalized Euclidean spectral distance.
	 *
	 * @param eigenvalues1 Normalized eigenvalues of the full matrix.
	 * @param eigenvalues2 Normalized eigenvalues of the current or candidate matrix.
	 * @return Spectral loss.
	 */
	private double spectralLoss(double[] eigenvalues1, double[] eigenvalues2) {
		double distance = 0.0; // Euclidean spectral distance
		for (int i = 0; i < eigenvalues1.length; i++) {
			distance = distance + Math.sqrt((eigenvalues1[i] - eigenvalues2[i]) * (eigenvalues1[i] - eigenvalues2[i]));
		}
		return distance;
	}

	/**
	 * Get the size of the current backbone list.
	 *
	 * @return Backbone size at current iteration.
	 */
	public int getBackboneSize() {
		return this.currentBackboneList.size();
	}

	/**
	 * Get the number of variable 2 entities used for computing the full matrix (i.e., after filtering).
	 *
	 * @return Number of entities.
	 */
	public int getFullSize() {
		return this.extractLabels(this.filteredStatements, this.variable2, this.variable2Document).length;
	}

	/**
	 * Initialize the nested backbone algorithm by setting up the data structures.
	 */
	public void initializeNestedBackbone() {
		this.isolates = false; // no isolates initially for full matrix; will be set to true after full matrix has been computed

		// initial values before iterations start
		this.originalStatements = this.filteredStatements; // to ensure not all isolates are included later

		// full set of concepts C
		fullConcepts = this.extractLabels(this.filteredStatements, this.variable2, this.variable2Document);

		// full network matrix Y against which we compare in every iteration
		fullMatrix = this.computeOneModeMatrix(this.filteredStatements, this.qualifierAggregation, this.startDateTime, this.stopDateTime);
		this.isolates = true; // include isolates in the iterations but not in the full matrix; will be adjusted to smaller full matrix dimensions without isolates manually each time in the iterations; necessary because some actors may be deleted in the backbone matrix otherwise after deleting their concepts

		// compute normalized eigenvalues for the full matrix; no need to recompute every time as they do not change
		eigenvaluesFull = computeNormalizedEigenvalues(fullMatrix.getMatrix(), "ojalgo");
		iteration = new int[fullConcepts.length];
		backboneLoss = new double[fullConcepts.length];
		redundantLoss = new double[fullConcepts.length];
		entity = new String[fullConcepts.length];
		ArrayList<String> allConcepts = new ArrayList<>(); // convert fullConcepts to array to populate backbone concepts
		for (int i = 0; i < fullConcepts.length; i++) {
			allConcepts.add(fullConcepts[i]);
		}
		currentBackboneList = new ArrayList<>(allConcepts);
		currentRedundantList = new ArrayList<>();
		backboneMatrices = new ArrayList<>();
		redundantMatrices = new ArrayList<>();
		numStatements = new int[fullConcepts.length];
		counter = 0;
	}

	/**
	 * One iteration in the nested backbone algorithm. Needs to be called in a while loop until the backbone set is empty ({@code while (currentBackboneSet.size() > 0)}).
	 */
	public void iterateNestedBackbone() {
		ArrayList<Matrix> candidateMatrices = new ArrayList<>();
		double[] currentLosses = new double[currentBackboneList.size()];
		int[] numStatementsCandidates = new int[currentBackboneList.size()];
		for (int i = 0; i < currentBackboneList.size(); i++) {
			ArrayList<String> candidate = new ArrayList<>(currentBackboneList);
			candidate.remove(i);
			final ArrayList<String> finalCandidate = new ArrayList<String>(candidate); // make it final, so it can be used in a stream
			candidateStatementList = this.filteredStatements
					.stream()
					.filter(s -> finalCandidate.contains(((Entity) s.get(this.variable2)).getValue()))
					.collect(Collectors.toCollection(ArrayList::new));
			numStatementsCandidates[i] = candidateStatementList.size();
			candidateMatrix = this.computeOneModeMatrix(candidateStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime);
			candidateMatrix = this.reduceCandidateMatrix(candidateMatrix, fullMatrix.getRowNames()); // ensure it has the right dimensions by purging isolates relative to the full matrix
			candidateMatrices.add(candidateMatrix);
			eigenvaluesCandidate = computeNormalizedEigenvalues(candidateMatrix.getMatrix(), "ojalgo"); // normalized eigenvalues for the candidate matrix
			currentLosses[i] = spectralLoss(eigenvaluesFull, eigenvaluesCandidate);
		}
		double smallestLoss = 0.0;
		if (currentBackboneList.size() > 0) {
			smallestLoss = Arrays.stream(currentLosses).min().getAsDouble();
		}
		for (int i = currentBackboneList.size() - 1; i >= 0; i--) {
			if (currentLosses[i] == smallestLoss) {
				iteration[counter] = counter + 1;
				entity[counter] = currentBackboneList.get(i);
				backboneLoss[counter] = smallestLoss;
				currentRedundantList.add(currentBackboneList.get(i));
				currentBackboneList.remove(i);
				backboneMatrices.add(candidateMatrices.get(i));

				// compute redundant matrix and loss at this level
				final ArrayList<String> finalRedundantCandidate = new ArrayList<String>(currentRedundantList);
				candidateStatementList = this.filteredStatements
						.stream()
						.filter(s -> finalRedundantCandidate.contains(((Entity) s.get(this.variable2)).getValue()))
						.collect(Collectors.toCollection(ArrayList::new));
				Matrix redundantMatrix = this.computeOneModeMatrix(candidateStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime);
				redundantMatrix = this.reduceCandidateMatrix(redundantMatrix, fullMatrix.getRowNames());
				redundantMatrices.add(redundantMatrix);
				eigenvaluesCandidate = computeNormalizedEigenvalues(redundantMatrix.getMatrix(), "ojalgo");
				redundantLoss[counter] = spectralLoss(eigenvaluesFull, eigenvaluesCandidate);
				numStatements[counter] = numStatementsCandidates[i];
				counter++;
			}
		}
	}

	/**
	 * Get the nested backbone result that is saved in the class.
	 *
	 * @return The nested backbone result (can be null if backbone function has not been executed).
	 */
	public NestedBackboneResult getNestedBackboneResult() {
		return this.nestedBackboneResult;
	}

	/**
	 * Compute matrix after final backbone iteration, collect results, and save in class.
	 */
	public void saveNestedBackboneResult() {
		Exporter.this.nestedBackboneResult = new NestedBackboneResult("nested",
				iteration,
				entity,
				backboneLoss,
				redundantLoss,
				numStatements,
				this.filteredStatements.size(),
				fullMatrix.getStart().toEpochSecond(ZoneOffset.UTC),
				fullMatrix.getStop().toEpochSecond(ZoneOffset.UTC));
		Exporter.this.simulatedAnnealingBackboneResult = null;
	}

	/**
	 * Partition the discourse network into a backbone and redundant set of second-mode entities using penalised
	 * spectral distances and simulated annealing. This method prepares the data before the algorithm starts.
	 *
	 * @param penalty Use penalty parameter? False if fixed backbone set.
	 * @param p Penalty parameter. Only used if penalty parameter is true.
	 * @param T Number of iterations.
	 * @param size The (fixed) size of the backbone set. Only used if no penalty.
	 */
	public void initializeSimulatedAnnealingBackbone(boolean penalty, double p, int T, int size) {
		this.p = p;
		this.T = T;
		this.backboneSize = size;
		this.isolates = false; // no isolates initially for full matrix; will be set to true after full matrix has been computed

		// initial values before iterations start
		this.originalStatements = this.filteredStatements; // to ensure not all isolates are included later

		// full set of concepts C
		fullConcepts = this.extractLabels(this.filteredStatements, this.variable2, this.variable2Document);

		// full network matrix Y against which we compare in every iteration
		fullMatrix = this.computeOneModeMatrix(this.filteredStatements, this.qualifierAggregation, this.startDateTime, this.stopDateTime);
		this.isolates = true; // include isolates in the iterations; will be adjusted to full matrix without isolates manually each time

		// compute normalized eigenvalues for the full matrix; no need to recompute every time as they do not change
		eigenvaluesFull = computeNormalizedEigenvalues(fullMatrix.getMatrix(), "ojalgo");

		if (penalty) { // simulated annealing with penalty: initially one randomly chosen entity in the backbone set
			// pick a random concept c_j from C and save its index
			int randomConceptIndex = ThreadLocalRandom.current().nextInt(0, fullConcepts.length);

			// final backbone list B, which contains only one random concept initially but will contain the final backbone set in the end
			finalBackboneList = new ArrayList<String>();

			// add the one uniformly sampled concept c_j to the backbone as the initial solution at t = 0: B <- {c_j}
			finalBackboneList.add(fullConcepts[randomConceptIndex]);

			// final redundant set R, which is initially C without c_j
			finalRedundantList = Arrays
					.stream(fullConcepts)
					.filter(c -> !c.equals(fullConcepts[randomConceptIndex]))
					.collect(Collectors.toCollection(ArrayList::new));
		} else { // simulated annealing without penalty and fixed backbone set size: randomly sample as many initial entities as needed
			// sample initial backbone set randomly
			if (this.backboneSize > fullConcepts.length) {
				LogEvent l = new LogEvent(Logger.ERROR,
						"Backbone size parameter too large",
						"The backbone size parameter of " + this.backboneSize + " is larger than the number of entities on the second mode, " + fullConcepts.length + ". It is impossible to choose a backbone set of that size. Please choose a smaller backbone size.");
				Dna.logger.log(l);
			} else if (this.backboneSize < 1) {
				LogEvent l = new LogEvent(Logger.ERROR,
						"Backbone size parameter too small",
						"The backbone size parameter of " + size + " is smaller than 1. It is impossible to choose a backbone set of that size. Please choose a larger backbone size.");
				Dna.logger.log(l);
			}
			finalBackboneList = new ArrayList<>();
			while (finalBackboneList.size() < this.backboneSize) {
				int randomConceptIndex = ThreadLocalRandom.current().nextInt(0, fullConcepts.length);
				String entity = fullConcepts[randomConceptIndex];
				if (!finalBackboneList.contains(entity)) {
					finalBackboneList.add(entity);
				}
			}
			finalRedundantList = Stream.of(fullConcepts).filter(c -> !finalBackboneList.contains(c)).collect(Collectors.toCollection(ArrayList::new));
		}

		// final statement list: filter the statement list by only retaining those statements that are in the final backbone set B
		finalStatementList = this.filteredStatements
				.stream()
				.filter(s -> finalBackboneList.contains(((Entity) s.get(this.variable2)).getValue()))
				.collect(Collectors.toCollection(ArrayList::new));

		// final matrix based on the initial final backbone set, Y^B, which is initially identical to the previous matrix
		finalMatrix = this.computeOneModeMatrix(finalStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime);
		finalMatrix = this.reduceCandidateMatrix(finalMatrix, fullMatrix.getRowNames()); // ensure it has the right dimensions by purging isolates relative to the full matrix

		// eigenvalues for final matrix
		eigenvaluesFinal = computeNormalizedEigenvalues(finalMatrix.getMatrix(), "ojalgo"); // normalized eigenvalues for the candidate matrix

		// create an initial current backbone set B_0, also with the one c_j concept like in B: B_0 <- {c_j}
		currentBackboneList = new ArrayList<String>(finalBackboneList);

		// create an initial current redundant set R_t, which is C without c_j
		currentRedundantList = new ArrayList<String>(finalRedundantList);

		// filter the statement list by only retaining those statements that are in the initial current backbone set B_0
		currentStatementList = this.filteredStatements
				.stream()
				.filter(s -> currentBackboneList.contains(((Entity) s.get(this.variable2)).getValue()))
				.collect(Collectors.toCollection(ArrayList::new));

		// create initial current matrix at t = 0
		currentMatrix = new Matrix(finalMatrix);

		// initial current eigenvalues
		eigenvaluesCurrent = eigenvaluesFinal;

		// initialise (empty) action set S
		this.actionList = new ArrayList<String>();

		// initialise selected action s
		selectedAction = "";

		// initialise the candidate backbone set at t, B^*_t
		candidateBackboneList = new ArrayList<String>();

		// initialise the candidate redundant set at t, R^*_t
		candidateRedundantList = new ArrayList<String>();

		// declare loss comparison result variables
		if (penalty) {
			finalLoss = penalizedLoss(eigenvaluesFull, eigenvaluesFinal, p, currentBackboneList.size(), fullConcepts.length); // spectral distance between full and initial matrix
		} else {
			finalLoss = spectralLoss(eigenvaluesFull, eigenvaluesFinal); // spectral distance between full and initial matrix
		}
		oldLoss = finalLoss;
		newLoss = 0.0;
		accept = false;

		// reporting
		temperatureLog = new ArrayList<Double>();
		acceptanceProbabilityLog = new ArrayList<Double>();
		acceptedLog = new ArrayList<Integer>();
		penalizedBackboneLossLog = new ArrayList<Double>(); // penalised or not penalised, depending on algorithm
		proposedBackboneSizeLog = new ArrayList<Integer>();
		acceptedBackboneSizeLog = new ArrayList<Integer>();
		finalBackboneSizeLog = new ArrayList<Integer>();
		acceptanceRatioLastHundredIterationsLog = new ArrayList<Double>();

		// matrix algebra declarations
		eigenvaluesCurrent = new double[0];

		// set to first iteration before starting simulated annealing
		t = 1;
	}

	/**
	 * Execute the next iteration of the simulated annealing backbone algorithm.
	 */
	public void iterateSimulatedAnnealingBackbone(boolean penalty) {
		// calculate temperature
		temperature = 1 - (1 / (1 + Math.exp(-(-5 + (12.0 / T) * t)))); // temperature
		temperatureLog.add(temperature);

		// make a random move by adding, removing, or swapping a concept and computing a new candidate
		actionList.clear(); // clear the set of possible actions and repopulate, depending on solution size
		if (currentBackboneList.size() < 2 && penalty) { // if there is only one concept, don't remove it because empty backbones do not work
			actionList.add("add");
			actionList.add("swap");
		} else if (currentBackboneList.size() > fullConcepts.length - 2 && penalty) { // do not create a backbone with all concepts because it would be useless
			actionList.add("remove");
			actionList.add("swap");
		} else if (penalty) { // everything in between one and |C| - 1 concepts: add all three possible moves to the action set
			actionList.add("add");
			actionList.add("remove");
			actionList.add("swap");
		} else { // with fixed backbone set (i.e., no penalty), only allow horizontal swaps
			actionList.add("swap");
		}
		Collections.shuffle(actionList); // randomly re-order the action set...
		selectedAction = actionList.get(0); // and draw the first action (i.e., pick a random action)
		candidateBackboneList.clear(); // create a candidate copy of the current backbone list, to be modified
		candidateBackboneList.addAll(currentBackboneList);
		candidateRedundantList.clear(); // create a candidate copy of the current redundant list, to be modified
		candidateRedundantList.addAll(currentRedundantList);
		if (selectedAction.equals("add")) { // if we add a concept...
			Collections.shuffle(candidateRedundantList); // randomly re-order the current redundant list...
			candidateBackboneList.add(candidateRedundantList.get(0)); // add the first concept from the redundant list to the backbone...
			candidateRedundantList.remove(0); // and delete it in turn from the redundant list
		} else if (selectedAction.equals("remove")) { // if we remove a concept...
			Collections.shuffle(candidateBackboneList); // randomly re-order the backbone list to pick a random concept for removal as the first element...
			candidateRedundantList.add(candidateBackboneList.get(0)); // add the selected concept to the redundant list...
			candidateBackboneList.remove(0); // and remove it from the backbone list
		} else if (selectedAction.equals("swap")) { //if we swap out a concept...
			Collections.shuffle(candidateBackboneList); // re-order the backbone list...
			Collections.shuffle(candidateRedundantList); // re-order the redundant list...
			candidateBackboneList.add(candidateRedundantList.get(0)); // add the first (random) redundant concept to the backbone list...
			candidateRedundantList.remove(0); // then remove it from the redundant list...
			candidateRedundantList.add(candidateBackboneList.get(0)); // add the first (random) backbone concept to the redundant list...
			candidateBackboneList.remove(0); // then remove it from the backbone list
		}
		proposedBackboneSizeLog.add(candidateBackboneList.size()); // log number of concepts in candidate backbone in the current iteration

		// after executing the action, filter the statement list based on the candidate backbone set B^*_t in order to create the candidate matrix, then compute eigenvalues and loss for the candidate
		candidateStatementList = this.filteredStatements
				.stream()
				.filter(s -> candidateBackboneList.contains(((Entity) s.get(this.variable2)).getValue()))
				.collect(Collectors.toCollection(ArrayList::new));
		candidateMatrix = this.computeOneModeMatrix(candidateStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime); // create candidate matrix after filtering the statements based on the action that was executed
		candidateMatrix = this.reduceCandidateMatrix(candidateMatrix, fullMatrix.getRowNames()); // ensure it has the right dimensions by purging isolates relative to the full matrix
		eigenvaluesCandidate = computeNormalizedEigenvalues(candidateMatrix.getMatrix(), "ojalgo"); // normalized eigenvalues for the candidate matrix
		if (penalty) {
			newLoss = penalizedLoss(eigenvaluesFull, eigenvaluesCandidate, p, candidateBackboneList.size(), fullConcepts.length); // spectral distance between full and candidate matrix
		} else {
			newLoss = spectralLoss(eigenvaluesFull, eigenvaluesCandidate); // spectral distance between full and candidate matrix
		}
		penalizedBackboneLossLog.add(newLoss); // log the penalised spectral distance between full and candidate solution

		// compare loss between full and previous (current) matrix to loss between full and candidate matrix and accept or reject candidate
		accept = false;
		if (newLoss < oldLoss) { // if candidate is better than previous matrix, adopt it as current solution
			accept = true; // flag this solution for acceptance
			acceptanceProbabilityLog.add(-1.0); // log the acceptance probability as -1.0; technically it should be 1.0 because the solution was better and hence accepted, but it would be useless for plotting the acceptance probabilities as a diagnostic tool
			if (newLoss <= finalLoss) { // if better than the best solution, adopt candidate as new final backbone solution
				finalBackboneList.clear(); // clear the best solution list
				finalBackboneList.addAll(candidateBackboneList); // and populate it with the concepts from the candidate solution instead
				finalRedundantList.clear(); // same with the redundant list
				finalRedundantList.addAll(candidateRedundantList);
				finalStatementList.clear(); // same with the final list of statements
				finalStatementList.addAll(candidateStatementList);
				finalMatrix = new Matrix(candidateMatrix); // save the candidate matrix as best solution matrix
				eigenvaluesFinal = eigenvaluesCandidate;
				finalLoss = newLoss; // save the candidate loss as the globally optimal loss so far
			}
		} else { // if the solution is worse than the previous one, apply Hastings ratio and temperature and compare with random number
			r = Math.random(); // random double between 0 and 1
			acceptance = Math.exp(-(newLoss - oldLoss)) * temperature; // acceptance probability
			acceptanceProbabilityLog.add(acceptance); // log the acceptance probability
			if (r < acceptance) { // apply probability rule
				accept = true;
			}
		}
		if (accept) { // if candidate is better than previous matrix...
			currentBackboneList.clear(); // create candidate copy and save as new current matrix
			currentBackboneList.addAll(candidateBackboneList);
			currentRedundantList.clear(); // also save the redundant candidate as new current redundant list
			currentRedundantList.addAll(candidateRedundantList);
			currentStatementList.clear(); // save candidate statement list as new current statement list
			currentStatementList.addAll(candidateStatementList);
			currentMatrix = new Matrix(candidateMatrix); // save candidate matrix as new current matrix
			eigenvaluesCurrent = eigenvaluesCandidate;
			oldLoss = newLoss; // save the corresponding candidate loss as the current/old loss
			acceptedLog.add(1); // log the acceptance of the proposed candidate
		} else {
			acceptedLog.add(0); // log the non-acceptance of the proposed candidate
		}
		acceptedBackboneSizeLog.add(currentBackboneList.size()); // log how many concepts are in the current iteration after the decision
		finalBackboneSizeLog.add(finalBackboneList.size()); // log how many concepts are in the final backbone solution in the current iteration
		log = 0.0; // compute ratio of acceptances in last up to 100 iterations
		for (int i = t - 1; i >= t - Math.min(100, t); i--) {
			log = log + acceptedLog.get(i);
		}
		acceptanceRatioLastHundredIterationsLog.add(log / Math.min(100, t)); // log ratio of accepted candidates in the last 100 iterations
		t = t + 1; // go to next iteration
	}

	/**
	 * Compute the spectral distance between the full network and the network based only on the backbone set and only the redundant set. The penalty parameter can be switched off by setting it to zero.
	 *
	 * @param backboneEntities An array of entities (e.g., concepts) to construct a backbone set for computing the spectral distance.
	 * @param p The penalty parameter. Can be \code{0} to switch off the penalty parameter.
	 * @return A double array with the penalized loss for the backbone set and the redundant set.
	 */
	public double[] evaluateBackboneSolution(String[] backboneEntities, int p) {
		this.p = p;
		double[] results = new double[2];
		this.isolates = false; // no isolates initially for full matrix; will be set to true after full matrix has been computed

		// initial values before iterations start
		this.originalStatements = this.filteredStatements; // to ensure not all isolates are included later

		// full set of concepts C
		fullConcepts = this.extractLabels(this.filteredStatements, this.variable2, this.variable2Document);

		// full network matrix Y against which we compare in every iteration
		fullMatrix = this.computeOneModeMatrix(this.filteredStatements, this.qualifierAggregation, this.startDateTime, this.stopDateTime);
		this.isolates = true; // include isolates in the iterations; will be adjusted to full matrix without isolates manually each time

		// compute normalized eigenvalues for the full matrix; no need to recompute every time as they do not change
		eigenvaluesFull = computeNormalizedEigenvalues(fullMatrix.getMatrix(), "ojalgo");

		// create copy of filtered statements and remove redundant entities
		ArrayList<String> entityList = Stream.of(backboneEntities).collect(Collectors.toCollection(ArrayList<String>::new));
		ArrayList<String> backboneSet = new ArrayList<>();
		ArrayList<String> redundantSet = new ArrayList<>();
		for (int i = 0; i < fullConcepts.length; i++) {
			if (entityList.contains(fullConcepts[i])) {
				backboneSet.add(fullConcepts[i]);
			} else {
				redundantSet.add(fullConcepts[i]);
			}
		}

		// spectral distance between full and backbone set
		candidateStatementList = this.filteredStatements
				.stream()
				.filter(s -> backboneSet.contains(((Entity) s.get(this.variable2)).getValue()))
				.collect(Collectors.toCollection(ArrayList::new));
		candidateMatrix = this.computeOneModeMatrix(candidateStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime); // create candidate matrix after filtering the statements based on the action that was executed
		candidateMatrix = this.reduceCandidateMatrix(candidateMatrix, fullMatrix.getRowNames()); // ensure it has the right dimensions by purging isolates relative to the full matrix
		eigenvaluesCandidate = computeNormalizedEigenvalues(candidateMatrix.getMatrix(), "ojalgo"); // normalized eigenvalues for the candidate matrix
		results[0] = penalizedLoss(eigenvaluesFull, eigenvaluesCandidate, p, backboneSet.size(), fullConcepts.length); // spectral distance between full and candidate matrix

		// spectral distance between full and redundant set
		candidateStatementList = this.filteredStatements
				.stream()
				.filter(s -> redundantSet.contains(((Entity) s.get(this.variable2)).getValue()))
				.collect(Collectors.toCollection(ArrayList::new));
		candidateMatrix = this.computeOneModeMatrix(candidateStatementList, this.qualifierAggregation, this.startDateTime, this.stopDateTime); // create candidate matrix after filtering the statements based on the action that was executed
		candidateMatrix = this.reduceCandidateMatrix(candidateMatrix, fullMatrix.getRowNames()); // ensure it has the right dimensions by purging isolates relative to the full matrix
		eigenvaluesCandidate = computeNormalizedEigenvalues(candidateMatrix.getMatrix(), "ojalgo"); // normalized eigenvalues for the candidate matrix
		results[1] = penalizedLoss(eigenvaluesFull, eigenvaluesCandidate, p, redundantSet.size(), fullConcepts.length); // spectral distance between full and candidate matrix

		return results;
	}
}