![](./dna/src/main/resources/icons/dna128.png)

## Discourse Network Analyzer (DNA)

The Java software Discourse Network Analyzer (DNA) is a qualitative content analysis tool with network export facilities. You import text files and annotate statements that persons or organizations make, and the program will return network matrices of actors connected by shared concepts.

- Download the latest [release](https://github.com/leifeld/dna/releases) of the software.

- Annotate documents, such as newspaper articles or speeches, with statements of what actors say; then export network data.

- You can use the stand-alone software [visone](https://visone.ethz.ch/) (or any other network analysis software) for analyzing the resulting networks.

- The software comes with an R package called rDNA for remote controlling DNA and for further ways of analyzing the networks.

[DNA 3.0](https://github.com/leifeld/dna/releases) was first released on 12 June 2022. It constitutes a major rewrite from the previous version DNA 2.0 beta 25. DNA 3 comes with many new features and improvements. The [release](https://github.com/leifeld/dna/releases) page contains all the details (scroll to version 3.0.7 for the first DNA 3 release).

[![build-check-test](https://github.com/leifeld/dna/actions/workflows/build-check-test.yml/badge.svg)](https://github.com/leifeld/dna/actions/workflows/build-check-test.yml)

## Installation of DNA

### Starting DNA
To use DNA, simply download the latest `.jar` file under "[Releases](https://github.com/leifeld/dna/releases)" on the right along with the `sample.dna` file, a toy database you can load in DNA (password: `sample`) to play with the software. You can double-click on the `.jar` file to open DNA. No installation is required.

If your system does not want to start DNA with a double-click on the `.jar` file, you can also open it from the terminal. To do so, navigate to the directory where the `.jar` file is stored on your computer using the `cd` command, for example `cd ~/Documents/`. Once there, type in something like this, with the `.jar` file corresponding to the file version you downloaded:

``` shell
java -jar dna-3.0.11.jar
```

Along with DNA, it is recommended to download [visone](https://visone.ethz.ch/), which can be opened in the same way as DNA. You can open `.graphml` files from DNA's network export in visone.

### Java installation and version check
DNA was written in Java and requires Java 11 or higher on your system. You can check if you have Java >= 11 on your system by opening the terminal of your operating system (e.g., typing `cmd` in your Windows start menu or using the terminal app on MacOS) and typing:

``` shell
java --version
```

If this indicates a version below 11 (or 1.11), installing the latest version of [Adoptium Temurin OpenJDK](https://adoptium.net) or any other Java >= 11 is recommended before you proceed. Once installed, restart your system and check the version again.

### MacOS
MacOS users may need to give the program authorization to be executed by opening the system settings and activating "Anywhere" rather than "App store and identified developers" under "Privacy & Security" -> "Security" -> "Allow applications downloaded from" (see [here](https://macpaw.com/how-to/fix-macos-cannot-verify-that-app-is-free-from-malware) for instructions).

Some MacOS users reported issues with opening files. These issues should have been fixed in version 3.0.11.

You can also browse the [issue tracker](https://github.com/leifeld/issues) (including closed issues) and the [commit messages](https://github.com/leifeld/dna/commits/master/) for more information on MacOS issues. Get in touch via the issue tracker or Matrix (infos below) if you are unable to solve these issues.

### Compiling from source using Gradle
If you require the latest (non-release) version of the DNA jar file from GitHub, you can clone the git repository to your computer and execute `./gradlew build` on your terminal or command line. This will build the jar file, the rDNA R package, and the bibliography, and store them in the `build/` directory of the cloned repository. If you only want to build the jar file, you can also execute `./gradlew :dna:build` (omit `./` on Windows).

Alternatively, if you need the latest non-release version, you can try to download the latest artifact from the build process under [GitHub Actions](https://github.com/leifeld/dna/actions) by clicking on the latest build and scrolling down to "Artifacts". However, it is usually recommended to use the most recent [release](https://github.com/leifeld/dna/releases/) version.

## rDNA 3.0: Connecting DNA to R

The R package rDNA connects DNA to R for data exchange and analysis.

rDNA offers functionality such as:
- plotting networks with different layouts,
- applying many kinds of cluster analysis/community detection/subgroup analysis and selecting the best solution automatically to identify coalitions,
- detection of phase transitions,
- partitioning of concepts into backbones and redundant sets,
- ideological scaling via item response theory,
- barplots etc.

The old 2.0 version additionally supported data exchange between DNA databases and rDNA by adding or retrieving documents, statements, attributes, statement types, coders etc. The 3.0 version does not support most of this data exchange functionality yet, so if you need to batch-import documents, for example, use DNA 2.0 first and then import everything into a DNA 3.0 database for further analysis (note that you cannot import from 3.0 into 2.0). More functions for data exchange and analysis will be added in the future.

To install the new rDNA 3 directly from GitHub, try the following code in R:

``` r
# install.packages("remotes")
remotes::install_github("leifeld/dna/rDNA/rDNA@*release",
                        INSTALL_opts = "--no-multiarch")
```

Note that the package relies on `rJava`, which needs to be installed first.

## Installation of the old rDNA 2.1.18
For data management, you may still want to use the old rDNA 2.1.18 with DNA 2.0 beta 25. You can install the package directly from GitHub as well. However, you will need to download the correct JAR file and store it either in your working directory or (recommended) in the library path of the installed R package in the "extdata" subdirectory. The following code can do this for you:
```r
# install.packages("remotes")
remotes::install_github("leifeld/dna/rDNA@v2.0-beta.25",
                        INSTALL_opts = "--no-multiarch")

# find out where to store the JAR file
dest <- paste0(dirname(system.file(".", package = "rDNA")),
               "/extdata/dna-2.0-beta25.jar")

# download JAR file and store in library path
u <- "https://github.com/leifeld/dna/releases/download/v2.0-beta.25/dna-2.0-beta25.jar"
download.file(url = u, destfile = dest, mode = "wb")

```

## Documentation and community

- This **tutorial on YouTube** describes installation of DNA, basic data coding, network export, and network analysis using visone. The video clip is 18 minutes long and based on DNA 3.0.10.
  
  [![DNA tutorial](https://img.youtube.com/vi/u3hc86Tcs9A/0.jpg)](https://www.youtube.com/watch?v=u3hc86Tcs9A)

- See the [bibliography](./build/bibliography.md) for several hundred publications and theses using discourse network analysis or the DNA software.

- The **introductory chapter** (Leifeld 2017) in the *Oxford Handbook of Political Networks* is recommended as a primer ([chapter](https://doi.org/10.1093/oxfordhb/9780190228217.013.25); [preprint](http://eprints.gla.ac.uk/121525/)).

- The previous version of DNA and rDNA came with a detailed [manual](https://github.com/leifeld/dna/releases/download/v2.0-beta.25/dna-manual.pdf) of more than 100 pages. It is outdated, but perhaps still useful.

- If you have questions or want to report bugs, please create an issue in the [issue tracker](https://github.com/leifeld/dna/issues).

- [Discussions](https://github.com/leifeld-lab/dna/discussions): Ask questions, introduce yourself or your DNA application, or engage with the DNA community. We used to have a Matrix channel for real-time chats about DNA but recently retired it. The Discussion forum will take its place. It currently looks pretty empty because it's new, but feel free to give it a try to get the ball rolling.

## Support the project

Please consider contributing to the project by:
- telling other people about the software,
- citing our underlying [research](https://www.philipleifeld.com/publications) in your publications,
- reporting or fixing [issues](https://github.com/leifeld/issues), or
- starting pull requests to contribute bug fixes or new functionality.

Some suggestions of new functionality you could add via pull requests:
- Import filters for loading data from Nvivo, MaxQDA, and other software into DNA.
- Export filters for exporting networks to Gephi and other network analysis software.
- Analysis functions or unit tests for the rDNA package.
- Publications for the bibliography.
- Bug fixes.
