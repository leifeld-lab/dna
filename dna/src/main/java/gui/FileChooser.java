package gui;

import dna.Dna;

import javax.swing.*;
import javax.swing.filechooser.FileFilter;
import java.awt.*;
import java.io.File;
import java.io.FilenameFilter;

/**
 * A file chooser. Uses native dialog on MacOS or Java dialog on other operating systems.
 */
public class FileChooser {
    /**
     * The chosen files. Stays {@code null} if cancelled. Multiple files or just one element.
     */
    private File[] f = null;
    private JDialog parentDialog = null;
    private JFrame parentFrame = null;
    private String title;
    private boolean save;
    private String extension;
    private String description;
    private boolean multipleFiles;

    /**
     * File chooser constructor with a dialog back-reference.
     *
     * @param parentDialog A parent {@link JDialog} reference.
     * @param title The title of the file chooser, to be displayed in the window title bar.
     * @param save Save dialog or load dialog?
     * @param extension File extension, for example {@code .dna}.
     * @param description File type description, for example {@code "DNA SQLite database (*.dna)"}.
     * @param multipleFiles Allow selection of multiple files? If false, the array of files has just one element.
     */
    public FileChooser(JDialog parentDialog, String title, boolean save, String extension, String description, boolean multipleFiles) {
        this.parentDialog = parentDialog;
        this.title = title;
        this.save = save;
        this.extension = extension;
        this.description = description;
        this.multipleFiles = multipleFiles;
        createDialog();
    }

    /**
     * File chooser constructor with a frame back-reference.
     *
     * @param parentFrame A parent {@link JFrame} reference.
     * @param title The title of the file chooser, to be displayed in the window title bar.
     * @param save Save dialog or load dialog?
     * @param extension File extension, for example {@code .dna}.
     * @param description File type description, for example {@code "DNA SQLite database (*.dna)"}.
     * @param multipleFiles Allow selection of multiple files? If false, the array of files has just one element.
     */
    public FileChooser(JFrame parentFrame, String title, boolean save, String extension, String description, boolean multipleFiles) {
        this.parentFrame = parentFrame;
        this.title = title;
        this.save = save;
        this.extension = extension;
        this.description = description;
        this.multipleFiles = multipleFiles;
        createDialog();
    }

    /**
     * Create the file chooser dialog with the values provided.
     */
    private void createDialog() {
        if (Dna.operatingSystem.toLowerCase().contains("mac")) { // if on MacOS, use the native file dialog because JFileChooser does not display any files on MacOS due to excessive user rights restrictions
            int load = FileDialog.LOAD;
            if (save) {
                load = FileDialog.SAVE;
            }
            FileDialog fd;
            if (parentDialog == null) {
                fd = new FileDialog(parentFrame, title, load);
            } else {
                fd = new FileDialog(parentDialog, title, load);
            }

            FilenameFilter filter = (dir, name) -> name.toLowerCase().endsWith(extension); // constrain to .dna files or similar (does not work on Windows)
            fd.setFilenameFilter(filter);

            fd.setMultipleMode(multipleFiles);

            fd.setDirectory(Dna.workingDirectory.getAbsolutePath()); // try setting the working directory as a starting directory for the file dialog
            fd.setVisible(true);
            this.f = fd.getFiles();
            if (fd.getFiles() != null && fd.getFiles().length > 0) {
                Dna.workingDirectory = new File(fd.getDirectory());
            }
        } else { // use JFileChooser on non-MacOS systems
            JFileChooser fc = new JFileChooser();
            fc.setDialogTitle(title);
            fc.setApproveButtonText("OK");
            fc.setCurrentDirectory(Dna.workingDirectory); // try setting the working directory as a starting directory for the file chooser

            fc.setFileFilter(new FileFilter() {
                public boolean accept(File f) {
                    return f.getName().toLowerCase().endsWith(extension) || f.isDirectory();
                }
                public String getDescription() {
                    return description;
                }
            });

            fc.setMultiSelectionEnabled(multipleFiles);

            int returnVal;
            if (parentDialog == null) {
                if (save) {
                    returnVal = fc.showSaveDialog(parentFrame);
                } else {
                    returnVal = fc.showOpenDialog(parentFrame);
                }
            } else {
                if (save) {
                    returnVal = fc.showSaveDialog(parentDialog);
                } else {
                    returnVal = fc.showOpenDialog(parentDialog);
                }
            }

            // extract chosen file name and check its validity
            if (returnVal == JFileChooser.APPROVE_OPTION) {
                if (multipleFiles) {
                    this.f = fc.getSelectedFiles();
                } else {
                    this.f = new File[] { fc.getSelectedFile() };
                }
                Dna.workingDirectory = fc.getCurrentDirectory();
            } else {
                this.f = new File[0];
            }
        }
        for (int i = 0; i < this.f.length; i++) {
            if (!this.f[i].getAbsolutePath().toLowerCase().endsWith(extension)) {
                this.f[i] = new File(this.f[i].getAbsolutePath() + extension);
            }
        }
    }

    public File[] getFiles() {
        return this.f;
    }
}