# Package startup --------------------------------------------------------------

dnaEnvironment <- new.env(hash = TRUE, parent = emptyenv())

#' Display version number and date when the package is loaded.
#' @importFrom utils packageDescription
#' @noRd
.onAttach <- function(libname, pkgname) {
  desc <- packageDescription(pkgname, libname)
  packageStartupMessage(
    'Version:      ', desc$Version, '\n',
    'Date:         ', desc$Date, '\n',
    'Author:       Philip Leifeld (University of Manchester)\n',
    'Contributors: Tim Henrichsen (University of Birmingham),\n',
    '              Johannes B. Gruber (Vrije Universiteit Amsterdam)\n',
    '              Kristijan Garic (University of Essex)\n',
    'Project home: github.com/leifeld/dna'
  )
}

#' Initialize the connection with DNA
#'
#' Establish a connection between \pkg{rDNA} and the DNA software.
#'
#' To use \pkg{rDNA}, DNA first needs to be initialized. This means that
#' \pkg{rDNA} needs to be told where the DNA executable file, i.e., the jar
#' file, is located. When the \code{dna_init} function is used, the connection
#' to the DNA software is established, and this connection is valid for the rest
#' of the \R session. To initialize a connection with a different DNA version or
#' path, the \R session would need to be restarted first.
#'
#' @param jarfile The file name of the DNA jar file, e.g.,
#'   \code{"dna-3.0.7.jar"}. Can be auto-detected using the
#'   \code{\link{dna_jar}} function, which looks for a version matching the
#'   installed \pkg{rDNA} version in the library path and working directory.
#' @param memory The amount of memory in megabytes to allocate to DNA, for
#'   example \code{1024} or \code{4096}.
#' @param returnString Return a character object representing the jar file name?
#'
#' @author Philip Leifeld
#'
#' @examples
#' \dontrun{
#' dna_init()
#' }
#'
#' @family startup
#'
#' @export
#' @importFrom rJava .jinit .jnew .jarray
dna_init <- function(jarfile = dna_jar(), memory = 1024, returnString = FALSE) {
  if (is.null(jarfile) || length(jarfile) == 0 || is.na(jarfile)) {
    stop("Invalid jar file name.")
    if (isTRUE(returnString)) {
      return(NULL)
    }
  }
  if (!is.character(jarfile) || length(jarfile) > 1 || !grepl("^dna-.+\\.jar$", basename(jarfile))) {
    stop("'jarfile' must be a character object of length 1 that points to the DNA jar file.")
  }
  if (!file.exists(jarfile)) {
    stop(paste0("jarfile '", jarfile, "' could not be located."))
  }
  assign("jar", jarfile, pos = dnaEnvironment)
  message(paste("Jar file:", dnaEnvironment[["jar"]]))
  .jinit(dnaEnvironment[["jar"]],
         force.init = TRUE,
         parameters = paste0("-Xmx", memory, "m"))
  dnaEnvironment[["dna"]] <- .jnew("dna.Dna", .jarray("headless"))
  message("DNA connection established.")
  if (isTRUE(returnString)) {
    return(jarfile)
  }
}

#' Find the DNA jar file
#'
#' Find the DNA jar file in the library path or working directory.
#'
#' rDNA requires the installation of a DNA jar file to run properly. The jar
#' file is shipped with the rDNA package and is installed in the \code{java/}
#' directory of the package installation directory in the R library tree. The
#' version number of the jar file and the rDNA package must match for DNA and
#' rDNA to be able to work together. The \code{dna_jar} function looks for
#' the jar file in the package installation directory sub-directory and
#' returns its file name with its absolute path. If it cannot be found in the
#' installation directory, the function looks in the current working
#' directory. The function is also called by \code{\link{dna_init}} if the
#' location of the jar file is not provided explicitly. Users do not normally
#' need to use the \code{dna_jar} function and are instead asked to use
#' \code{\link{dna_init}}.
#'
#' @return The file name of the jar file that matches the installed \pkg{rDNA}
#'   version, including full path.
#'
#' @author Philip Leifeld
#'
#' @family startup
#'
#' @importFrom utils download.file unzip packageVersion
#' @export
dna_jar <- function() {
  # detect package version
  v <- as.character(packageVersion("rDNA"))

  # try to locate jar file in library path under java/ and return jar file path
  tryCatch({
    rdna_dir <- dirname(system.file(".", package = "rDNA"))
    jar <- paste0(rdna_dir, "/java/dna-", v, ".jar")
    if (file.exists(jar)) {
      message("Jar file found in library path.")
      return(jar)
    }
  }, error = function(e) {success <- FALSE})

  # try to locate jar file in library path under inst/java/ and return jar file path
  tryCatch({
    rdna_dir <- dirname(system.file(".", package = "rDNA"))
    jar <- paste0(rdna_dir, "/inst/java/dna-", v, ".jar")
    if (file.exists(jar)) {
      message("Jar file found in library path.")
      return(jar)
    }
  }, error = function(e) {success <- FALSE})

  # try to locate jar file inside package using system file
  tryCatch({
    jar <- system.file(
      "java",
      paste0("dna-", v, ".jar"),
      package = "rDNA"
    )
    if (nzchar(jar)) {
      message("Jar file found in package.")
      return(jar)
    }
  }, error = function(e) {success <- FALSE})

  # try to locate jar file in working directory and return jar file path
  tryCatch({
    jar <- paste0(getwd(), "/dna-", v, ".jar")
    if (file.exists(jar)) {
      message("Jar file found in working directory.")
      return(jar)
    }
  }, error = function(e) {success <- FALSE})

  stop("DNA jar file could not be found in the library path, package, or ",
       "working directory. Your current rDNA version is ", v, ".")
}

#' Provides a small sample database
#'
#' A small sample database to test the functions of rDNA.
#'
#' Copies a small .dna sample file to the current working directory and returns
#' the location of this newly created file.
#'
#' @param overwrite Logical. Should \code{sample.dna} be overwritten if found in
#'   the current working directory?
#'
#' @examples
#' \dontrun{
#' dna_init()
#' s <- dna_sample()
#' dna_openDatabase(s)
#' }
#'
#' @author Johannes B. Gruber, Philip Leifeld
#'
#' @family startup
#'
#' @export
dna_sample <- function(overwrite = FALSE) {
  if (file.exists(paste0(getwd(), "/sample.dna")) & overwrite == FALSE) {
    warning("Sample file already exists in working directory. ",
            "Use 'overwrite = TRUE' to revert changes in the sample file.")
  } else {
    file.copy(from = system.file("extdata", "sample.dna", package = "rDNA"),
              to = paste0(getwd(), "/sample.dna"),
              overwrite = overwrite)
  }
  return(paste0(getwd(), "/sample.dna"))
}


# Database connections ---------------------------------------------------------

#' Open a database
#'
#' Open a database in DNA.
#'
#' Open a database in DNA. This can be a SQLite, MySQL, or PostgreSQL database.
#' The database must already have the table structure required for DNA. You must
#' provide the coder ID and password along with the database credentials. To
#' look up coder IDs, use the \code{\link{dna_queryCoders}} function.
#'
#' @param coderId The coder ID of the coder who is opening the database. If an
#'   invalid coder ID is supplied (i.e., \code{-1} or similar), the coder ID is
#'   queried interactively from the user.
#' @param coderPassword The coder password of the coder who is opening the
#'   database. If an empty password is provided (e.g., \code{""}), the password
#'   is queried interactively from the user.
#' @param db_url The URL for accessing the database (for remote databases) or
#'   the path of the SQLite database file, including file extension.
#' @param db_type The type of database. Valid values are \code{"sqlite"},
#'   \code{postgresql}, and \code{postgresql}.
#' @param db_name The name of the database at the given URL or path. Can be a
#'   zero-length character object (\code{""}) for file-based SQLite databases.
#' @param db_port The connection port for the database connection. No port is
#'   required (\code{db_port = -1}) for SQLite databases. MySQL databases often
#'   use port \code{3306}. PostgreSQL databases often use port \code{5432}. If
#'   \code{db_port = NULL}, one of these default values will be selected based
#'   on the \code{db_type} argument.
#' @param db_login The login user name for the database. This is the database
#'   login user name, not the coder name. Can be a zero-length character object
#'   (\code{""}) for SQLite databases.
#' @param db_password The password for the database. This is the database
#'   password, not the coder password. Can be a zero-length character object
#'   (\code{""}) for SQLite databases.
#'
#' @author Philip Leifeld
#'
#' @family database
#' @seealso \code{\link{dna_queryCoders}}
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' }
#'
#' @export
#' @importFrom rJava .jcall
dna_openDatabase <- function(db_url,
                             coderId = 1,
                             coderPassword = "",
                             db_type = "sqlite",
                             db_name = "",
                             db_port = -1,
                             db_login = "",
                             db_password = "") {
  if (is.null(db_port) && !is.null(db_type)) {
    if (db_type == "sqlite") {
      db_port <- as.integer(-1)
    } else if (db_type == "mysql") {
      db_port <- as.integer(3306)
    } else if (db_type == "postgresql") {
      db_port <- as.integer(5432)
    }
  } else {
    db_port <- as.integer(db_port)
  }
  if (db_type == "sqlite") {
    if (file.exists(db_url)) {
      db_url <- normalizePath(db_url)
    } else {
      stop("Database file not found.")
    }
  }
  if (is.null(coderId) || !is.numeric(coderId) || coderId < 1) {
    if (!requireNamespace("askpass", quietly = TRUE)) {
      coderId <- as.integer(readline("Coder ID: "))
    } else {
      coderId <- as.integer(askpass::askpass("Coder ID: "))
    }
  }
  if (is.null(coderId) || length(coderId) == 0) {
    coderId <- -1
  }
  if (is.null(coderPassword) || !is.character(coderPassword) || coderPassword == "") {
    if (!requireNamespace("askpass", quietly = TRUE)) {
      coderPassword <- readline("Coder password: ")
    } else {
      coderPassword <- askpass::askpass("Coder password: ")
    }
  }
  if (is.null(coderPassword) || length(coderPassword) == 0) {
    coderPassword <- ""
  }
  q <- .jcall(dna_api(),
              "Z",
              "openDatabase",
              as.integer(coderId),
              coderPassword,
              db_type,
              db_url,
              db_name,
              db_port,
              db_login,
              db_password)
}

#' Close the open DNA database (if any).
#'
#' Close the DNA database that is currently active (if any).
#'
#' Close the currently active DNA database and display a message confirming that
#' the database was closed.
#'
#' @author Philip Leifeld
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' dna_closeDatabase()
#' }
#'
#' @family database
#'
#' @export
#' @importFrom rJava .jcall
dna_closeDatabase <- function() {
  .jcall(dna_api(), "V", "closeDatabase")
}

#' Open a connection profile
#'
#' Open a connection profile and establish a connection to the database.
#'
#' Load a connection profile from a \code{.dnc} file. The file contains
#' connection details for a database (like a bookmark) along with the coder ID
#' of the coder who saved the connection profile. By loading the connection
#' profile, a connection to the database will be established by DNA, and the
#' coder saved in the connection profile will be activated. The coder password
#' the user needs to provide is the coder password for the coder saved in the
#' connection profile. It serves to decrypt the information stored in the file
#' and activate the coder in the database connection. If an empty character
#' object is provided as the password (\code{""}), the user will be prompted
#' interactively for a password. If the \pkg{askpass} package is installed, this
#' package will be used to mask the user input; otherwise the password is
#' visible in clear text. Installing the \pkg{askpass} package is strongly
#' recommended.
#'
#' @param file The file name of the connection profile to open.
#' @param coderPassword The clear text coder password. If a zero-length
#'   character object (\code{""}) is provided, the user will be prompted
#'   for a password interactively.
#'
#' @author Philip Leifeld
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' dna_saveConnectionProfile(file = "my profile.dnc", coderPassword = "sample")
#' dna_closeDatabase()
#' dna_openConnectionProfile(file = "my profile.dnc", coderPassword = "sample")
#' }
#'
#' @family database
#'
#' @export
#' @importFrom rJava .jcall
dna_openConnectionProfile <- function(file, coderPassword = "") {
  if (is.null(file) || !is.character(file) || length(file) != 1) {
    stop("Please provide a file name for the connection profile.")
  }
  if (!file.exists(file)) {
    stop("File does not exist.")
  } else {
    file <- normalizePath(file)
  }
  if (is.null(coderPassword) || !is.character(coderPassword) || coderPassword == "") {
    if (!requireNamespace("askpass", quietly = TRUE)) {
      coderPassword <- readline("Coder password: ")
    } else {
      coderPassword <- askpass::askpass("Coder password: ")
    }
  }
  if (is.null(coderPassword) || length(coderPassword) == 0) {
    coderPassword <- ""
  }
  s <- .jcall(dna_api(),
              "Z",
              "openConnectionProfile",
              file,
              coderPassword)
}

#' Save a connection profile to a file
#'
#' Save connection profile for the current coder and database to disk
#'
#' Save the current database URL/path, user name, password, port, database name,
#' and coder to an encrypted JSON file with the extension \code{.dnc}. This file
#' is called a connection profile. It serves as a bookmark and saves you from
#' having to enter and store the full connection details each time you want to
#' access the database. Please make sure you enter the file name with the
#' extension. You are asked to provide the coder password of the currently
#' active coder again, for whom the connection profile is saved. This is just
#' for security reasons. If you do not provide a coder password (e.g., your
#' password is a zero-length character object \code{""}), you are asked to enter
#' the password interactively. If the \pkg{askpass} package is installed, this
#' package will be used to mask the user input; otherwise the password is
#' visible in clear text. Installing the \pkg{askpass} package is strongly
#' recommended.
#'
#' @param file The file name of the connection profile to save.
#' @param coderPassword The clear text coder password. If a zero-length
#'   character object (\code{""}) is provided, the user will be prompted
#'   for a password interactively.
#'
#' @author Philip Leifeld
#'
#' @family database
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' dna_saveConnectionProfile(file = "my profile.dnc", coderPassword = "sample")
#' }
#'
#' @export
#' @importFrom rJava .jcall
dna_saveConnectionProfile <- function(file, coderPassword = "") {
  if (is.null(file) || !is.character(file) || length(file) != 1) {
    stop("Please provide a file name for the connection profile.")
  }
  if (is.null(coderPassword) || !is.character(coderPassword) || coderPassword == "") {
    if (!requireNamespace("askpass", quietly = TRUE)) {
      coderPassword <- readline("Coder password: ")
    } else {
      coderPassword <- askpass::askpass("Coder password: ")
    }
  }
  if (is.null(coderPassword) || length(coderPassword) == 0) {
    coderPassword <- ""
  }
  s <- .jcall(dna_api(),
              "Z",
              "saveConnectionProfile",
              file,
              coderPassword)
}

#' Print database details
#'
#' Print number of documents and statements and active coder.
#'
#' For the DNA database that is currently open, print the number of documents
#' and statements, the URL, statement types (and their statement counts), and
#' the active coder to the console.
#'
#' @author Philip Leifeld
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' dna_printDetails()
#' }
#'
#' @family database
#'
#' @export
#' @importFrom rJava .jcall
dna_printDetails <- function() {
  .jcall(dna_api(), "V", "printDatabaseDetails")
}

#' Get a reference to the headless Java class for R (API)
#'
#' Get a reference to the headless Java class for R (API).
#'
#' This function returns a Java object reference to the instance of the
#' \code{Dna/HeadlessDna} class in the DNA JAR file that is held in the rDNA
#' package environment and used by the functions in the package to exchange data
#' with the Java application. You can use the \pkg{rJava} package to access the
#' available functions in this class directly. API access requires detailed
#' knowledge of the DNA JAR classes and functions and is recommended for
#' developers and advanced users only.
#'
#' @return A Java object reference to the \code{Dna/HeadlessDna} class.
#'
#' @author Philip Leifeld
#'
#' @examples
#' \dontrun{
#' library("rJava") # load rJava package to use functions in the Java API
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' api <- dna_api()
#'
#' # use the \code{getVariables} function to retrieve variables
#' variable_references <- api$getVariables("DNA Statement")
#'
#' # iterate through variable references and print their data type
#' for (i in seq(variable_references$size()) - 1) {
#'   print(variable_references$get(as.integer(i))$getDataType())
#' }
#' }
#'
#' @family database
#'
#' @export
dna_api <- function() {
  return(dnaEnvironment[["dna"]]$headlessDna)
}

# Coder management--------------------------------------------------------------

#' Query the coders in a database
#'
#' Display the coder IDs, names, and colors present in a DNA database.
#'
#' Some functions require knowing the coder ID with which changes should be
#' made. This function queries any database, which does not have to be opened,
#' for their coder IDs, names, and colors, and returns them as a data frame.
#'
#' @param db_url The URL or full path of the database.
#' @param db_type The type of database. Valid values are \code{"sqlite"},
#'   \code{postgresql}, and \code{postgresql}.
#' @param db_name The name of the database at the given URL or path. Can be a
#'   zero-length character object (\code{""}) for file-based SQLite databases.
#' @param db_port The connection port for the database connection. No port is
#'   required (\code{db_port = -1}) for SQLite databases. MySQL databases often
#'   use port \code{3306}. PostgreSQL databases often use port \code{5432}. If
#'   \code{db_port = NULL}, one of these default values will be selected based
#'   on the \code{db_type} argument.
#' @param db_login The login user name for the database. This is the database
#'   login user name, not the coder name. Can be a zero-length character object
#'   (\code{""}) for SQLite databases.
#' @param db_password The password for the database. This is the database
#'   password, not the coder password. Can be a zero-length character object
#'   (\code{""}) for SQLite databases.
#'
#' @author Philip Leifeld
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_queryCoders("sample.dna")
#' }
#'
#' @export
#' @importFrom rJava .jcall .jevalArray
dna_queryCoders <- function(db_url,
                            db_type = "sqlite",
                            db_name = "",
                            db_port = NULL,
                            db_login = "",
                            db_password = "") {
  if (is.null(db_port) && !is.null(db_type)) {
    if (db_type == "sqlite") {
      db_port <- as.integer(-1)
    } else if (db_type == "mysql") {
      db_port <- as.integer(3306)
    } else if (db_type == "postgresql") {
      db_port <- as.integer(5432)
    }
  } else {
    db_port <- as.integer(db_port)
  }
  q <- .jcall(dna_api(),
              "[Ljava/lang/Object;",
              "queryCoders",
              db_type,
              ifelse(db_type == "sqlite", normalizePath(db_url), db_url),
              db_name,
              db_port,
              db_login,
              db_password)
  names(q) <- c("ID", "Name", "Color")
  q <- lapply(q, .jevalArray)
  q <- as.data.frame(q, stringsAsFactors = FALSE)
  return(q)
}


# Variables --------------------------------------------------------------------

#' Retrieve a dataframe with all variables for a statement type
#'
#' Retrieve a dataframe with all variables defined in a given statement type.
#'
#' For a given statement type ID or label, this function creates a data frame
#' with one row per variable and contains columns for the variable ID, name and
#' data type.
#'
#' @param statementType The statement type for which statements should be
#'   retrieved. The statement type can be supplied as an integer or character
#'   string, for example \code{1} or \code{"DNA Statement"}.
#'
#' @examples
#' \dontrun{
#' dna_init()
#' samp <- dna_sample()
#' dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
#' variables <- dna_getVariables("DNA Statement")
#' variables
#' }
#'
#' @author Philip Leifeld
#'
#' @importFrom rJava J .jcall
#' @export
dna_getVariables <- function(statementType) {
  if (is.null(statementType) || is.na(statementType) || length(statementType) != 1) {
    stop("'statementType' must be an integer or character object of length 1.")
  }
  if (is.numeric(statementType) && !is.integer(statementType)) {
    statementType <- as.integer(statementType)
  } else if (!is.character(statementType) && !is.integer(statementType)) {
    stop("'statementType' must be an integer or character object of length 1.")
  }

  v <- J(dna_api(), "getVariables", statementType) # get an array list of Value objects representing the variables
  l <- list()
  for (i in seq(.jcall(v, "I", "size")) - 1) { # iterate through array list of Value objects
    vi <- v$get(as.integer(i)) # save current Value as vi
    row <- list() # create a list for the different slots
    row$id <- .jcall(vi, "I", "getVariableId")
    row$label <- .jcall(vi, "S", "getKey")
    row$type <- .jcall(vi, "S", "getDataType")
    l[[i + 1]] <- row # add the row to the list
  }
  d <- do.call(rbind.data.frame, l) # convert the list of lists to data frame
  attributes(d)$statementType <- statementType
  return(d)
}


# Attributes -------------------------------------------------------------------

#' Get the entities and attributes for a variable
#'
#' Retrieve the entities and their attributes for a variable in DNA
#'
#' This function retrieves the entities and their attributes for a given
#' variable from the DNA database as a \code{dna_attributes} object. Such an
#' object is an extension of a data frame and can be treated as such.
#'
#' There are three ways to use this function: by specifying only the variable
#' ID; by specifying the variable name and its statement type ID; and by
#' specifying the variable name and its statement type name.
#'
#' @param statementType The name of the statement type in which the variable is
#'   defined for which entities and values should be retrieved. Only required if
#'   \code{variableId} is not supplied. Either \code{statementType} or
#'   \code{statementTypeId} must be specified in this case.
#' @param variable The name of the variable for which the entities and
#'   attributes should be returned. In addition to this argument, either the
#'   statement type name or statement type ID must be supplied to identify the
#'   variable correctly. If the \code{variableId} a specified, the
#'   \code{variable} argument is unnecessary and the statement type need not be
#'   supplied.
#' @param statementTypeId The ID of the statement type in which the variable is
#'   defined for which entities and values should be retrieved. Only required if
#'   \code{variableId} is not supplied. Either \code{statementType} or
#'   \code{statementTypeId} must be specified in this case.
#' @param variableId The ID of the variable for which the entities and
#'   attributes should be returned. If this argument is supplied, the other
#'   three arguments are unnecessary.
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase("sample.dna", coderId = 1, coderPassword = "sample")
#'
#' dna_getAttributes(variableId = 1)
#' dna_getAttributes(statementTypeId = 1, variable = "organization")
#' dna_getAttributes(statementType = "DNA Statement", variable = "concept")
#' }
#'
#' @author Philip Leifeld
#'
#' @family attributes
#'
#' @importFrom rJava .jcall
#' @importFrom rJava J
#' @export
dna_getAttributes <- function(statementType = NULL,
                              variable = NULL,
                              statementTypeId = NULL,
                              variableId = NULL) {

  # check if the arguments are valid
  statementTypeValid <- TRUE
  if (is.null(statementType) || !is.character(statementType) || length(statementType) != 1 || is.na(statementType) || statementType == "") {
    statementTypeValid <- FALSE
  }

  statementTypeIdValid <- TRUE
  if (is.null(statementTypeId) || !is.numeric(statementTypeId) || length(statementTypeId) != 1 || is.na(statementTypeId) || statementTypeId %% 1 != 0) {
    statementTypeIdValid <- FALSE
  }

  variableValid <- TRUE
  if (is.null(variable) || !is.character(variable) || length(variable) != 1 || is.na(variable) || variable == "") {
    variableValid <- FALSE
  }

  variableIdValid <- TRUE
  if (is.null(variableId) || !is.numeric(variableId) || length(variableId) != 1 || is.na(variableId) || variableId %% 1 != 0) {
    variableIdValid <- FALSE
  }

  errorString <- "Please supply 1) a variable ID or 2) a statement type name and a variable name or 3) a statement type ID and a variable name."
  if ((!variableValid && !variableIdValid) || (!statementTypeIdValid && !statementTypeValid && !variableIdValid)) {
    stop(errorString)
  }

  if (variableIdValid && variableValid) {
    variable <- NULL
    variableValid <- FALSE
    warning("Both a variable ID and a variable name were supplied. Ignoring the 'variable' argument.")
  }

  if (statementTypeIdValid && statementTypeValid && !variableIdValid && variableValid) {
    statementType <- NULL
    statementTypeValid <- FALSE
    warning("Both a statement type ID and a statement type name were supplied. Ignoring the 'statementType' argument.")
  }

  if (variableIdValid && (statementTypeIdValid || statementTypeValid)) {
    statementTypeId <- NULL
    statementTypeIdValid <- FALSE
    statementType <- NULL
    statementTypeValid <- FALSE
    warning("If a variable ID is provided, a statement type is not necessary. Ignoring the 'statementType' and 'statementTypeId' arguments.")
  }

  # get the data from the DNA database using rJava
  if (variableIdValid) {
    a <- .jcall(dna_api(),
                "Ldna/export/DataFrame;",
                "getAttributes",
                as.integer(variableId))
  } else if (variableValid && statementTypeIdValid) {
    a <- .jcall(dna_api(),
                "Ldna/export/DataFrame;",
                "getAttributes",
                as.integer(statementTypeId),
                variable)
  } else if (variableValid && statementTypeValid) {
    a <- .jcall(dna_api(),
                "Ldna/export/DataFrame;",
                "getAttributes",
                statementType,
                variable)
  } else {
    stop(errorString)
  }

  # extract the relevant information from the Java reference
  varNames <- .jcall(a, "[S", "getVariableNamesArray")
  nr <- .jcall(a, "I", "nrow")
  nc <- .jcall(a, "I", "ncol")

  # create an empty data frame with the first (integer) column for IDs
  dat <- cbind(data.frame(ID = integer(nr)),
               matrix(character(nr), nrow = nr, ncol = nc - 1))
  # populate the data frame
  for (i in 0:(nr - 1)) {
    for (j in 0:(nc - 1)) {
      dat[i + 1, j + 1] <- J(a, "getValue", as.integer(i), as.integer(j))
    }
  }
  rownames(dat) <- NULL
  colnames(dat) <- varNames
  class(dat) <- c("dna_attributes", class(dat))
  return(dat)
}

#' Set entities and attributes for a variable
#'
#' Update entities (values, colors, and attribute values for a variable in DNA
#' by supplying a data frame. The data frame must contain the same structure as
#' returned by \code{\link{dna_getAttributes}}.
#'
#' The first three columns must be:
#' \enumerate{
#'   \item \code{ID} – integer entity ID
#'   \item \code{value} – character entity value
#'   \item \code{color} – character hex RGB color (e.g. \code{"#AABBCC"})
#' }
#' All remaining columns are interpreted as attribute variables.
#'
#' Identify which variable to update by supplying one of the following:
#' \enumerate{
#'   \item the variable ID
#'   \item the variable name as a character object and the statement type ID
#'   \item the variable name and statement type as character objects
#' }
#'
#' @param data A data frame of class \code{dna_attributes} or compatible
#'   \code{data.frame}.
#' @param statementType The name of the statement type in which the variable is
#'   defined. Only required if \code{variableId} and \code{statementTypeId} are
#'   not supplied.
#' @param statementTypeId The ID of the statement type in which the variable is
#'   defined. Only required if \code{variableId} and \code{statementType} are
#'   not supplied.
#' @param variable The name of the variable whose entities and attributes should
#'   be updated. Required unless \code{variableId} is supplied.
#' @param variableId The ID of the variable whose entities and attributes should
#'   be updated. Required unless \code{variable} and either \code{statementType}
#'   or \code{statementTypeId} is supplied.
#' @param simulate Logical; if \code{TRUE}, all changes are rolled back after
#'   execution. Use this as a precaution to test how changes would affect the
#'   database before repeating the function without simulation.
#'
#' @author Philip Leifeld
#'
#' @family attributes
#'
#' @importFrom rJava .jcall .jnew
#' @export
dna_setAttributes <- function(data,
                              statementType = NULL,
                              variable = NULL,
                              statementTypeId = NULL,
                              variableId = NULL,
                              simulate = TRUE) {

  # basic checks
  if (!is.data.frame(data)) {
    stop("'data' must be a data frame.")
  }

  if (ncol(data) < 3) {
    stop("'data' must contain at least three columns: ID, value, and color.")
  }

  if (colnames(data)[1] != "ID") {
    stop("The first column of 'data' must be named 'ID'.")
  }

  if (colnames(data)[2] != "value") {
    stop("The second column of 'data' must be named 'value'.")
  }

  if (colnames(data)[3] != "color") {
    stop("The third column of 'data' must be named 'color'.")
  }

  if (!is.logical(simulate) || length(simulate) != 1 || is.na(simulate)) {
    stop("'simulate' must be a single logical value.")
  }

  # argument validity
  statementTypeValid <- is.character(statementType) && length(statementType) == 1 && !is.na(statementType) && statementType != ""
  statementTypeIdValid <- is.numeric(statementTypeId) && length(statementTypeId) == 1 && !is.na(statementTypeId) && statementTypeId %% 1 == 0
  variableValid <- is.character(variable) && length(variable) == 1 && !is.na(variable) && variable != ""
  variableIdValid <- is.numeric(variableId) && length(variableId) == 1 && !is.na(variableId) && variableId %% 1 == 0

  if ((!variableIdValid && !variableValid) || (!variableIdValid && !statementTypeValid && !statementTypeIdValid)) {
    stop("Please supply either: 1) a variable ID, or 2) a statement type name and variable name, or 3) a statement type ID and variable name.")
  }

  if (variableIdValid && variableValid) {
    warning("Both 'variableId' and 'variable' supplied. Ignoring 'variable'.")
    variable <- NULL
    variableValid <- FALSE
  }

  if (statementTypeValid && statementTypeIdValid && !variableIdValid) {
    warning("Both 'statementType' and 'statementTypeId' supplied. Ignoring 'statementType'.")
    statementType <- NULL
    statementTypeValid <- FALSE
  }

  if (variableIdValid) {
    statementType <- NULL
    statementTypeId <- NULL
  }

  # create Java DataFrame
  df <- .jnew("dna/export/DataFrame")
  for (j in 1:ncol(data)) {
    varName <- colnames(data)[j]
    if (is.character(data[, j]) || is.factor(data[, j])) {
      #l <- lapply(data[, j], function(x) .jcast(.jnew("java/lang/String", as.character(x)), new.class = "java/lang/Object")) # cast each character object to String, then Object, and save them in a list
      l <- lapply(data[[j]], function(x) .jnew("java/lang/String", as.character(x))) # create Java Strings and save in R list
      dataType <- "String"
    } else if (is.numeric(data[, j])) {
      #l <- lapply(data[, j], function(x) .jcast(.jnew("java/lang/Integer", as.integer(x)), new.class = "java/lang/Object")) # cast each character object to Integer, then Object, and save them in a list
      l <- lapply(data[[j]], function(x) .jnew("java/lang/Integer", as.integer(x))) # create Java Integers and save in R list
      dataType <- "Integer"
    }
    al <- .jnew("java/util/ArrayList") # create array list (generic type is inferred when it's populated)
    for (i in 1:length(l)) {
      al$add(l[[i]]) # add elements to array list
    }
    df$addColumn(varName, dataType, al)
    #.jcall(df, "V", "addColumn", varName, dataType, al) # add the array list as a column to the Java DataFrame
  }

  # call Java API
  if (variableIdValid) {
    dna_api()$setAttributes(as.integer(variableId), df, simulate)
    #.jcall(
    #  dna_api(),
    #  "V",
    #  "setAttributes",
    #  as.integer(variableId),
    #  df,
    #  simulate
    #)
  } else if (statementTypeIdValid) {
    dna_api()$setAttributes(as.integer(statementTypeId), variable, df, simulate)
    #.jcall(
    #  dna_api(),
    #  "V",
    #  "setAttributes",
    #  as.integer(statementTypeId),
    #  variable,
    #  df,
    #  simulate
    #)
  } else {
    dna_api()$setAttributes(statementType, variable, df, simulate)
    #.jcall(
    #  dna_api(),
    #  "V",
    #  "setAttributes",
    #  statementType,
    #  variable,
    #  df,
    #  simulate
    #)
  }
}

# Statements -------------------------------------------------------------------

#' Retrieve statements for a given statement type
#'
#' Retrieve statements for a given statement type.
#'
#' This function retrieves statements from the DNA database for a given
#' statement type and returns them as a data frame. The statement type can be
#' specified by its ID or by its name. If no statement IDs are specified, all
#' statements of the given type are returned. If statement IDs are specified,
#' only those statements are returned. The function returns a data frame with
#' one row per statement and columns for the statement ID, document ID, start
#' and end positions, coder ID, and the values of the variables defined in the
#' statement type.
#'
#' @param statementType The statement type for which statements should be
#   retrieved. The statement type can be supplied as an integer or character
#   string, for example \code{1} or \code{"DNA Statement"}.
#' @param statementIds A vector of statement IDs to retrieve. If this argument
#   is not supplied or is an empty vector, all statements of the given type are
#   returned. If this argument is supplied, only the statements with the given
#   IDs are returned.
#'
#' @return A data frame with the statements of the given type. The data frame
#'   has one row per statement and columns for the statement ID, document ID,
#'   start and end positions, coder ID, and the values of the variables defined
#'   in the statement type.
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase(coderId = 1,
#'                  coderPassword = "sample",
#'                  db_url = "sample.dna")
#' statements <- dna_getStatements(statementType = "DNA Statement")
#' statements
#' statements <- dna_getStatements(statementType = 1, statementIds = c(1, 2))
#' statements
#' }
#' @author Philip Leifeld
#'
#' @family statements
#' @importFrom rJava .jcall .jarray
#' @export
dna_getStatements <- function(statementType = 1, statementIds = integer()) {

  if (is.numeric(statementType) && !is.integer(statementType) && length(statementType) == 1) {
    statementType <- as.integer(statementType)
  }
  if (is.null(statementType) || (!is.integer(statementType) && !is.character(statementType)) || length(statementType) != 1 || is.na(statementType)) {
    statementType <- 1
    warning("'statementType' must be an integer or character object of length 1. Using default value of 1.")
  }
  if (is.null(statementIds) || !is.numeric(statementIds)) {
    statementIds <- integer(0)
    warning("'statementIds' must be an integer vector. Using default value of integer(0) to include all statements.") # nolint: line_length_linter.
  } else if (is.numeric(statementIds) && !is.integer(statementIds)) {
    statementIds <- as.integer(statementIds)
  }

  # get the statements from the DNA database using rJava
  s <- .jcall(dna_api(),
              "Ldna/export/DataFrame;",
              "getStatements",
              statementType,
              .jarray(statementIds))
  if (is.jnull(s)) {
    warning("No statements were returned from the DNA database.")
    return(data.frame())
  }

  var_names <- .jcall(s, "[S", "getVariableNamesArray")
  data_types <- .jcall(s, "[S", "getDataTypesArray")

  nr <- .jcall(s, "I", "nrow")
  if (nr == 0) {
    return(data.frame())
  }

  l <- list()
  for (j in seq_along(var_names)) {
    if (data_types[j] == "int") {
      v <- integer(nr)
      for (i in 0:(nr - 1)) {
        v[i + 1] <- J(s, "getValue", as.integer(i), as.integer(j - 1))
      }
      l[[var_names[j]]] <- v
    } else if (data_types[j] == "String") {
      v <- character(nr)
      for (i in 0:(nr - 1)) {
        v[i + 1] <- J(s, "getValue", as.integer(i), as.integer(j - 1))
      }
      l[[var_names[j]]] <- v
    }
  }

  dat <- as.data.frame(l, stringsAsFactors = FALSE)
  rownames(dat) <- NULL
  colnames(dat) <- var_names
  class(dat) <- c("dna_statements", class(dat))
  return(dat)
}

#' Add a statement to the DNA database
#'
#' Add a new statement to the DNA database.
#'
#' The \code{dna_addStatement} function can add a new statement to an existing
#' DNA database. The user supplies a document ID, the location of the statement
#' in the document, and the variables and their values. As different statement
#' types have different variables, the \code{...} argument catches all
#' variables and their values supplied by the user. The statement ID will be
#' automatically generated and returned.
#'
#' @param documentID An integer specifying the ID of the document for which the
#' statement should be added.
#' @param startCaret An integer for the start location of the statement in the
#' document text. Must be non-negative and not larger than the number of
#' characters minus one in the document.
#' @param endCaret An integer for the stop location of the statement in the
#' document text. Must be non-negative, greater than \code{startCaret}, and not
#' larger than the number of characters in the document.
#' @param statementType The statement type of the statement that will be added.
#' Can be provided as an integer ID of the statement type or as a character
#' object representing the name of the statement type (if there is no
#' ambiguity).
#' @param coder An integer value indicating which coder created the document.
#' @param ... Values of the variables contained in the statement, for example
#' \code{organization = "some actor", concept = "my concept", agreement = 1}.
#' Values for Boolean variables can be provided as \code{logical} values
#' (\code{TRUE} or \code{FALSE}) or \code{numeric} values (\code{1} or
#' \code{0}).
#' @return The ID of the newly created statement in the DNA database. If the
#' statement could not be added, the function returns \code{-1}.
#'
#' @author Philip Leifeld
#'
#' @importFrom rJava .jarray
#' @importFrom rJava .jcall
#' @export
dna_addStatement <- function(documentID,
                             startCaret = 0,
                             endCaret = 1,
                             statementType = "DNA Statement",
                             coder = 1,
                             ...) {
  if (!is.integer(documentID)) {
    if (is.numeric(documentID)) {
      documentID <- as.integer(documentID)
    } else {
      stop("'documentID' must be a numeric value specifying the ID of the document to which the statement should be added. You can look up document IDs using the 'dna_getDocuments' function.")
    }
  }
  if (!is.integer(startCaret)) {
    if (is.numeric(startCaret)) {
      startCaret <- as.integer(startCaret)
    } else {
      stop("'startCaret' must be a single numeric value specifying the start location in of the statement in the document.")
    }
  }
  if (!is.integer(endCaret)) {
    if (is.numeric(endCaret)) {
      endCaret <- as.integer(endCaret)
    } else {
      stop("'endCaret' must be a single numeric value specifying the end location in of the statement in the document.")
    }
  }
  if (!is.character(statementType) && !is.numeric(statementType)) {
    stop("'statementType' must be a numeric ID of the statement type or a character object indicating the name of the statement type.")
  } else if (is.numeric(statementType) && !is.integer(statementType)) {
    statementType <- as.integer(statementType)
  }
  if (!is.integer(coder)) {
    if (is.numeric(coder)) {
      coder <- as.integer(coder)
    } else {
      stop("The coder must be provided as a numeric object (see dna_getCoders).")
    }
  }
  ellipsis <- list(...)
  ellipsis <- lapply(ellipsis, function(x) {
    if (is.logical(x)) {
      if (x == TRUE) {
        x <- 1
      } else if (x == FALSE) {
        x <- 0
      }
    }
    if (is.numeric(x)) {
      x <- as.integer(x)
    }
    if (!class(x) %in% c("character", "integer", "logical")) {
      stop("All supplied values must be character, integer, or logical.")
    }
    if (length(x) != 1) {
      stop("All supplied values must be of length 1.")
    }
    return(x)
  })

  varNames <- names(ellipsis)
  values <- .jarray(
    lapply(
      ellipsis,
      function(x) {
        if (is.character(x)) {
          .jnew("java/lang/String", x)
        } else if (is.integer(x)) {
          .jnew("java/lang/Integer", x)
        } else {
          stop("Unsupported type")
        }
      }
    ),
    contents.class = "java/lang/Object"
  )

  id <- .jcall(dna_api(),
               "I",
               "addStatement",
               documentID,
               startCaret,
               endCaret,
               statementType,
               coder,
               varNames,
               values)
  return(id)
}
