![](./dna/src/main/resources/icons/dna_horizontal.svg)

## Discourse Network Analyzer (DNA)

The Java software Discourse Network Analyzer (DNA) is a qualitative content analysis tool with network export facilities. You import text files and annotate statements that persons or organizations make, and the program will return network matrices of actors connected by shared concepts.

- Download the latest [release](https://github.com/leifeld/dna/releases) of the software.

- Annotate documents, such as newspaper articles or speeches, with statements of what actors say; then export network data.

- You can use the stand-alone software [visone](https://visone.ethz.ch/) (or any other network analysis software) for analyzing the resulting networks.

- The software comes with an R package called rDNA for remote controlling DNA and for further ways of analyzing the networks.

[![build-check-test](https://github.com/leifeld/dna/actions/workflows/build-check-test.yml/badge.svg)](https://github.com/leifeld/dna/actions/workflows/build-check-test.yml)

## Installation of DNA

### Starting DNA
To use DNA, simply download the latest `.jar` file under "[Releases](https://github.com/leifeld/dna/releases)" on the right along with the `sample.dna` file, a toy database you can load in DNA (password: `sample`) to play with the software. You can double-click on the `.jar` file to open DNA. No installation is required.

If your system does not want to start DNA with a double-click on the `.jar` file, you can also open it from the terminal. To do so, navigate to the directory where the `.jar` file is stored on your computer using the `cd` command, for example `cd ~/Documents/`. Once there, type in something like this, with the `.jar` file corresponding to the file version you downloaded:

``` shell
java -jar dna-3.1.0.jar
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

Alternatively, if you need the latest non-release version, you can try to download the latest artifact from the build process under [GitHub Actions](https://github.com/leifeld/dna/actions) by clicking on the latest build and scrolling down to "Artifacts". (You may need to be logged in to GitHub to access artifacts.) However, it is usually recommended to use the most recent [release](https://github.com/leifeld/dna/releases/) version.

## rDNA: Connecting DNA to R

The R package rDNA connects DNA to R for data exchange and analysis.

rDNA offers functionality such as:
- plotting networks with different layouts,
- batch-adding, deleting, and retrieving documents, statements, and attributes,
- applying many kinds of cluster analysis/community detection/subgroup analysis and selecting the best solution automatically to identify coalitions,
- detection of phase transitions ([Vandenhole et al., 2025](https://doi.org/10.1016/j.erss.2025.104020); [Leifeld and Garic, 2026](https://doi.org/10.1111/jcms.70119)),
- partitioning of concepts into backbones and redundant sets ([Leifeld and Henrichsen, 2023](https://www.sg.ethz.ch/talks/2023_mmm/leifeld/Leifeld.pdf)),
- ideological scaling via item response theory ([Leifeld et al., 2022](https://doi.org/10.1080/13501763.2021.1945131); [Henrichsen et al., 2025](https://doi.org/10.1177/20531680241307940)),
- polarization analysis ([Leifeld and Fisher, 2025](https://doi.org/10.17645/pag.9933)),
- barplots ([Leifeld and Haunss, 2012](https://doi.org/10.1111/j.1475-6765.2011.02003.x); [Leifeld and Henrichsen, 2023](https://www.sg.ethz.ch/talks/2023_mmm/leifeld/Leifeld.pdf)), etc.

Previous advice recommended installing DNA 2.0 for data exchange (e.g., adding documents or statements), but this is no longer recommended as a lot of this functionality was added back into version 3.1.0.

To install the new rDNA directly from GitHub, try the following code in R:

``` r
# install.packages("remotes")
remotes::install_github("leifeld/dna/rDNA/rDNA@*release",
                        INSTALL_opts = "--no-multiarch")
```

Note that the package relies on `rJava`, which needs to be installed first.

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
