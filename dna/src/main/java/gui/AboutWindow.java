package gui;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URISyntaxException;

import javax.swing.*;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

import dna.Dna;
import logger.LogEvent;
import logger.Logger;

/**
 * This class shows an about window with instructions.
 */
@SuppressWarnings("serial")
public class AboutWindow extends JDialog {

	private JPanel aboutContents;
	private JEditorPane aboutText;
	private JScrollPane aboutScrollPane;

	/**
	 * Create and show a new About DNA window.
	 * 
	 * @param version DNA version.
	 * @param date    Date of the DNA version.
	 */
	public AboutWindow(String version, String date) {
		this.setTitle("About DNA");
		ImageIcon dnaSmallIcon = new SvgIcon("/icons/dna.svg", 32).getImageIcon();
		this.setIconImage(dnaSmallIcon.getImage());
		this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		aboutContents = new JPanel(new BorderLayout());

		ImageIcon dnaTextIcon = new SvgIcon("/icons/dna.svg", 96).getImageIcon();
		JLabel dnaIcon = new JLabel(dnaTextIcon);
		JPanel dnaIconPanel = new JPanel(new FlowLayout());
		dnaIconPanel.add(dnaIcon);
		aboutContents.add(dnaIconPanel, BorderLayout.NORTH);

		aboutText = new JEditorPane();
		aboutText.setEditorKit(JEditorPane.createEditorKitForContentType("text/html"));
		aboutText.setEditable(false);

		aboutText.setText("<p><b>Current version</b><br/>" + version + " (" + date + ")</p>"
				+ "<p><b>DNA project homepage</b><br/>"
				+ "Documentation, source code, publications, a forum, and a bug tracker can be found "
				+ "here: <a href=\"http://github.com/leifeld-lab/dna/\">http://github.com/leifeld-lab/dna/</a>.</p>"
				+ "<p><b>Icons</b><br/> from <a href=\"https://tabler-icons.io/\">"
				+ "https://tabler-icons.io/</a> (MIT license) <br/> "
				+ "and <a href=\"https://fonts.google.com/icons/\">https://fonts.google.com/icons/</a> (Apache License 2.0)</p>"
				+ "<p><b>JRI</b><br/>To display output in R, this software project uses JRI by Simon Urbanek (under LGPL license), "
				+ "which can be downloaded at <a href=\"https://github.com/s-u/rJava\">https://github.com/s-u/rJava</a>.</p>"
				);
		aboutText.addHyperlinkListener(e -> {
			if(e.getEventType() == HyperlinkEvent.EventType.ACTIVATED) {
				if(Desktop.isDesktopSupported()) {
					try {
						Desktop.getDesktop().browse(e.getURL().toURI());
						LogEvent l = new LogEvent(Logger.MESSAGE,
								"[GUI] URL in About DNA window opened.",
								"A link from the About DNA window was successfully opened in a browser.");
						Dna.logger.log(l);
					} catch (UnsupportedOperationException e1) {
						LogEvent l = new LogEvent(Logger.WARNING,
								"[GUI] URL in About DNA window could not be opened.",
								"On some operating systems (notably Linux), Java does not support opening URLs by clicking on a hyperlink, such as the one in the About DNA window. The link you clicked on was not opened.",
								e1);
						Dna.logger.log(l);
					} catch (IOException e1) {
						LogEvent l = new LogEvent(Logger.WARNING,
								"[GUI] URL in About DNA window could not be opened.",
								"An input-output exception occurred when you tried to click on a link in the About DNA window. The link was not opened in a browser.",
								e1);
						Dna.logger.log(l);
					} catch (URISyntaxException e1) {
						LogEvent l = new LogEvent(Logger.WARNING,
								"[GUI] URL in About DNA window could not be opened.",
								"A URI syntax exception occurred when you tried to click on a link in the About DNA window. Something must have been wrong with the URL you clicked on; perhaps a bug in the code? The link was not opened in a browser.",
								e1);
						Dna.logger.log(l);
					}
				}
			}
		});
		aboutScrollPane = new JScrollPane(aboutText);
		aboutScrollPane.setPreferredSize(new Dimension(500, 310));
		aboutContents.add(aboutScrollPane, BorderLayout.CENTER);

		// close button at the bottom of the window
		ImageIcon closeIcon = new SvgIcon("/icons/google_close.svg", 14).getImageIcon();
		JButton closeButton = new JButton("Close", closeIcon);
		closeButton.addActionListener(al -> dispose());
		JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
		buttonPanel.add(closeButton);
		aboutContents.add(buttonPanel, BorderLayout.SOUTH);
		
		this.add(aboutContents);
		
		LogEvent l = new LogEvent(Logger.MESSAGE,
				"[GUI] About DNA window was opened.",
				"[GUI] About DNA window was successfully opened.");
		Dna.logger.log(l);

		// set location and pack window
		this.setModal(true);
		this.pack();
		this.setLocationRelativeTo(null);
		this.setVisible(true);
		this.setResizable(false);
	}
}