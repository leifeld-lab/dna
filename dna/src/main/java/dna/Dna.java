package dna;

import java.io.*;
import java.net.URISyntaxException;

import gui.Gui;
import logger.LogEvent;
import logger.Logger;
import sql.Sql;

/**
 * Main class of the Discourse Network Analyzer (DNA).
 * 
 * @author Philip Leifeld {@literal <philip.leifeld@manchester.ac.uk>}
 */
public class Dna {
	public static Dna dna;
	public static Logger logger;
	public static Sql sql;
	public static final String date = "2026-01-24";
	public static final String version = "3.0.11.7";
	public static final String operatingSystem = System.getProperty("os.name");
	public static File workingDirectory = null;
	public static Gui gui;
	public HeadlessDna headlessDna;
	
	/**
	 * Create a new instance of DNA including the GUI.
	 */
	public Dna(String[] args) {
		logger = new Logger();

		sql = new Sql();

		LogEvent l = new LogEvent(Logger.MESSAGE,
				"DNA started. Version " + version + " (" + date + ").",
				"DNA started. Version " + version + " (" + date + "). Operating system: " + operatingSystem + ".");
		Dna.logger.log(l);

		// determine JAR working directory as starting directory for file dialogs
		String currentDir = "~/";
		try {
			currentDir = new File(HeadlessDna.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
		} catch (URISyntaxException ex) {
			LogEvent le = new LogEvent(Logger.WARNING,
					"Current JAR working directory cannot be detected.",
					"Tried to detect the current JAR working directory, but the path could not be detected. Will use the home user directory instead.",
					ex);
			logger.log(le);
		}
		workingDirectory = new File(currentDir);

		// start GUI or headless DNA
		if (args != null && args.length > 0 && args[0].equals("headless")) {
			headlessDna = new HeadlessDna();
			Dna.logger.addListener(headlessDna);

			LogEvent l2 = new LogEvent(Logger.MESSAGE,
					"DNA started in headless mode.",
					"DNA started in headless mode to work with rDNA.");
			Dna.logger.log(l2);
		} else {
			gui = new Gui();
		}
	}

	/**
	 * Start DNA.
	 * 
	 * @param args Any arguments from the terminal/command line/shell.
	 */
	public static void main(String[] args) {
		Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());
		dna = new Dna(args);
	}

	/**
	 * Default exception handler. This catches all uncaught exceptions and logs
	 * them in the logger, instead of printing them on the terminal.
	 */
	private static final class DefaultExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
    		LogEvent le = new LogEvent(Logger.ERROR,
            		"Uncaught exception.",
            		"An uncaught exception occurred. This is most likely a bug. Please open a new issue at https://github.com/leifeld/dna/issues/ and paste the log event stack trace and the exception stack trace there as part of your error description, along with details of what happened and under what circumstances. Thank you.",
            		e);
            logger.log(le);
        }
    }
}
