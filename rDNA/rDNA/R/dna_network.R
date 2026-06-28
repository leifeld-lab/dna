#' Compute and retrieve a network
#'
#' Compute and retrieve a network from DNA.
#'
#' This function serves to compute a one-mode or two-mode network or an event
#' list in DNA and retrieve it as a matrix or data frame, respectively. The
#' arguments resemble the export options in DNA. It is also possible to compute
#' a temporal sequence of networks using the moving time window approach, in
#' which case the networks are retrieved as a list of matrices.
#'
#' @param networkType The kind of network to be computed. Can be
#'   \code{"twomode"}, \code{"onemode"}, or \code{"eventlist"}.
#' @param statementType The name of the statement type in which the variable
#'   of interest is nested. For example, \code{"DNA Statement"}.
#' @param variable1 The first variable for network construction. In a one-mode
#'   network, this is the variable for both the rows and columns. In a
#'   two-mode network, this is the variable for the rows only. In an event
#'   list, this variable is only used to check for duplicates (depending on
#'   the setting of the \code{duplicates} argument).
#' @param variable1Document A boolean value indicating whether the first
#'   variable is at the document level (i.e., \code{"author"},
#'   \code{"source"}, \code{"section"}, \code{"type"}, \code{"id"}, or
#'   \code{"title"}).
#' @param variable2 The second variable for network construction. In a one-mode
#'   network, this is the variable over which the ties are created. For
#'   example, if an organization x organization network is created, and ties
#'   in this network indicate co-reference to a concept, then the second
#'   variable is the \code{"concept"}. In a two-mode network, this is the
#'   variable used for the columns of the network matrix. In an event list,
#'   this variable is only used to check for duplicates (depending on the
#'   setting of the \code{duplicates} argument).
#' @param variable2Document A boolean value indicating whether the second
#'   variable is at the document level (i.e., \code{"author"},
#'   \code{"source"}, \code{"section"}, \code{"type"}, \code{"id"}, or
#'   \code{"title"}
#' @param qualifier The qualifier variable. In a one-mode network, this
#'   variable can be used to count only congruence or conflict ties. For
#'   example, in an organization x organization network via common concepts,
#'   a binary \code{"agreement"} qualifier could be used to record only ties
#'   where both organizations have a positive stance on the concept or where
#'   both organizations have a negative stance on the concept. With an
#'   integer qualifier, the tie weight between the organizations would be
#'   proportional to the similarity or distance between the two organizations
#'   on the scale of the integer variable. With a short text variable as a
#'   qualifier, agreement on common categorical values of the qualifier is
#'   required, for example a tie is established (or a tie weight increased) if
#'   two actors both refer to the same value on the second variable AND match on
#'   the categorical qualifier, for example the type of referral.
#'
#'   In a two-mode network, the qualifier variable can be used to retain only
#'   positive or only negative statements or subtract negative from positive
#'   mentions. All of this depends on the setting of the
#'   \code{qualifierAggregation} argument. For event lists, the qualifier
#'   variable is only used for filtering out duplicates (depending on the
#'   setting of the \code{duplicates} argument.
#'
#'   The qualifier can also be \code{NULL}, in which case it is ignored, meaning
#'   that values in \code{variable1} and \code{variable2} are unconditionally
#'   associated with each other in the network when they co-occur. This is
#'   identical to selecting a qualifier variable and setting
#'   \code{qualifierAggregation = "ignore"}.
#' @param qualifierDocument A boolean value indicating whether the qualifier
#'   variable is at the document level (i.e., \code{"author"},
#'   \code{"source"}, \code{"section"}, \code{"type"}, \code{"id"}, or
#'   \code{"title"}
#' @param qualifierAggregation The aggregation rule for the \code{qualifier}
#'   variable. In one-mode networks, this must be \code{"ignore"} (for
#'   ignoring the qualifier variable), \code{"congruence"} (for recording a
#'   network tie only if both nodes have the same qualifier value in the
#'   binary case or for recording the similarity between the two nodes on the
#'   qualifier variable in the integer case), \code{"conflict"} (for
#'   recording a network tie only if both nodes have a different qualifier
#'   value in the binary case or for recording the distance between the two
#'   nodes on the qualifier variable in the integer case), or
#' \code{"subtract"} (for subtracting the conflict tie value from the
#'   congruence tie value in each dyad). In two-mode networks, this must be
#' \code{"ignore"}, \code{"combine"} (for creating multiplex combinations,
#'   e.g., 1 for positive, 2 for negative, and 3 for mixed), or
#' \code{subtract} (for subtracting negative from positive ties). In event
#'   lists, this setting is ignored.
#' @param normalization Normalization of edge weights. Valid settings for
#'   one-mode networks are \code{"no"} (for switching off normalization),
#'   \code{"average"} (for average activity normalization), \code{"jaccard"}
#'   (for Jaccard coefficient normalization), and \code{"cosine"} (for
#'   cosine similarity normalization). Valid settings for two-mode networks
#'   are \code{"no"}, \code{"activity"} (for activity normalization), and
#'   \code{"prominence"} (for prominence normalization).
#' @param isolates Should all nodes of the respective variable be included in
#'   the network matrix (\code{isolates = TRUE}), or should only those nodes
#'   be included that are active in the current time period and are not
#'   excluded (\code{isolates = FALSE})?
#' @param duplicates Setting for excluding duplicate statements before network
#'   construction. Valid settings are \code{"include"} (for including all
#'   statements in network construction), \code{"document"} (for counting
#'   only one identical statement per document), \code{"week"} (for counting
#'   only one identical statement per calendar week), \code{"month"} (for
#'   counting only one identical statement per calendar month), \code{"year"}
#'   (for counting only one identical statement per calendar year), and
#'   \code{"acrossrange"} (for counting only one identical statement across
#'   the whole time range).
#' @param start.date The start date for network construction in the format
#'   \code{"dd.mm.yyyy"}. All statements before this date will be excluded.
#' @param start.time The start time for network construction on the specified
#'   \code{start.date}. All statements before this time on the specified date
#'   will be excluded.
#' @param stop.date The stop date for network construction in the format
#'   \code{"dd.mm.yyyy"}. All statements after this date will be excluded.
#' @param stop.time The stop time for network construction on the specified
#'   \code{stop.date}. All statements after this time on the specified date
#'   will be excluded.
#' @param timeWindow Possible values are \code{"no"}, \code{"events"},
#'   \code{"seconds"}, \code{"minutes"}, \code{"hours"}, \code{"days"},
#'   \code{"weeks"}, \code{"months"}, and \code{"years"}. If \code{"no"} is
#'   selected (= the default setting), no time window will be used. If any of
#'   the time units is selected, a moving time window will be imposed, and
#'   only the statements falling within the time period defined by the window
#'   will be used to create the network. The time window will then be moved
#'   forward by one time unit at a time, and a new network with the new time
#'   boundaries will be created. This is repeated until the end of the overall
#'   time span is reached. All time windows will be saved as separate
#'   networks in a list. The duration of each time window is defined by the
#'   \code{windowSize} argument. For example, this could be used to create a
#'   time window of 6 months which moves forward by one month each time, thus
#'   creating time windows that overlap by five months. If \code{"events"} is
#'   used instead of a natural time unit, the time window will comprise
#'   exactly as many statements as defined in the \code{windowSize} argument.
#'   However, if the start or end statement falls on a date and time where
#'   multiple events happen, those additional events that occur simultaneously
#'   are included because there is no other way to decide which of the
#'   statements should be selected. Therefore the window size is sometimes
#'   extended when the start or end point of a time window is ambiguous in
#'   event time.
#' @param windowSize The number of time units of which a moving time window is
#'   comprised. This can be the number of statement events, the number of days
#'   etc., as defined in the \code{"timeWindow"} argument.
#' @param kernel Use kernel smoothing for computing time windows? This option
#'   only matters if the \code{timeWindow} argument has a value other than
#'   \code{"no"} or \code{"event"}. The default value \code{kernel = "no"}
#'   switches off kernel smoothing, which means all statements within a time
#'   window are weighted equally. Other values down-weight statements the
#'   farther they are temporally away from the mid-point of the time window.
#'   Several kernel smoothing functions are available, similar to kernel density
#'   estimation: \code{"uniform"} is similar to \code{"no"} and weights all
#'   statements with a value of \code{0.5}. \code{"gaussian"} uses a standard
#'   normal distribution as a kernel smoother. \code{"epanechnikov"} uses an
#'   Epanechnikov kernel smoother. \code{"triangular"} uses a triangular kernel
#'   function. If in doubt, do not use kernel smoothing.
#' @param excludeValues A list of named character vectors that contains entries
#'   which should be excluded during network construction. For example,
#'   \code{list(concept = c("A", "B"), organization = c("org A", "org B"))}
#'   would exclude all statements containing concepts "A" or "B" or
#'   organizations "org A" or "org B" when the network is constructed. This
#'   is irrespective of whether these values appear in \code{variable1},
#'   \code{variable2}, or the \code{qualifier}. Note that only variables at
#'   the statement level can be used here. There are separate arguments for
#'   excluding statements nested in documents with certain meta-data.
#' @param excludeAuthors A character vector of authors. If a statement is
#'   nested in a document where one of these authors is set in the "Author"
#'   meta-data field, the statement is excluded from network construction.
#' @param excludeSources A character vector of sources. If a statement is
#'   nested in a document where one of these sources is set in the "Source"
#'   meta-data field, the statement is excluded from network construction.
#' @param excludeSections A character vector of sections. If a statement is
#'   nested in a document where one of these sections is set in the "Section"
#'   meta-data field, the statement is excluded from network construction.
#' @param excludeTypes A character vector of types. If a statement is
#'   nested in a document where one of these types is set in the "Type"
#'   meta-data field, the statement is excluded from network construction.
#' @param invertValues A boolean value indicating whether the entries provided
#'   by the \code{excludeValues} argument should be excluded from network
#'   construction (\code{invertValues = FALSE}) or if they should be the only
#'   values that should be included during network construction
#'   (\code{invertValues = TRUE}).
#' @param invertAuthors A boolean value indicating whether the entries provided
#'   by the \code{excludeAuthors} argument should be excluded from network
#'   construction (\code{invertAuthors = FALSE}) or if they should be the
#'   only values that should be included during network construction
#'   (\code{invertAuthors = TRUE}).
#' @param invertSources A boolean value indicating whether the entries provided
#'   by the \code{excludeSources} argument should be excluded from network
#'   construction (\code{invertSources = FALSE}) or if they should be the
#'   only values that should be included during network construction
#'   (\code{invertSources = TRUE}).
#' @param invertSections A boolean value indicating whether the entries
#'   provided by the \code{excludeSections} argument should be excluded from
#'   network construction (\code{invertSections = FALSE}) or if they should
#'   be the only values that should be included during network construction
#'   (\code{invertSections = TRUE}).
#' @param invertTypes A boolean value indicating whether the entries provided
#'   by the \code{excludeTypes} argument should be excluded from network
#'   construction (\code{invertTypes = FALSE}) or if they should be the
#'   only values that should be included during network construction
#'   (\code{invertTypes = TRUE}).
#' @param fileFormat An optional file format specification for saving the
#'   resulting network(s) to a file instead of returning an object. Valid values
#'   are \code{"csv"} (for network matrices or event lists), \code{"dl"} (for
#'   UCINET DL full-matrix files), and \code{"graphml"} (for visone .graphml
#'   files).
#' @param outfile An optional output file name for saving the resulting
#'   network(s) to a file instead of returning an object.
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase("sample.dna", coderId = 1, coderPassword = "sample")
#' nw <- dna_network(networkType = "onemode",
#'   variable1 = "organization",
#'   variable2 = "concept",
#'   qualifier = "agreement",
#'   qualifierAggregation = "congruence",
#'   normalization = "average",
#'   excludeValues = list("concept" =
#'     c("There should be legislation to regulate emissions.")))
#' }
#'
#' @author Philip Leifeld
#'
#' @family networks
#'
#' @importFrom rJava .jarray
#' @importFrom rJava .jcall
#' @importFrom rJava .jnull
#' @importFrom rJava J
#' @export
dna_network <- function(networkType = "twomode",
                        statementType = "DNA Statement",
                        variable1 = "organization",
                        variable1Document = FALSE,
                        variable2 = "concept",
                        variable2Document = FALSE,
                        qualifier = "agreement",
                        qualifierDocument = FALSE,
                        qualifierAggregation = "ignore",
                        normalization = "no",
                        isolates = FALSE,
                        duplicates = "include",
                        start.date = "01.01.1900",
                        stop.date = "31.12.2099",
                        start.time = "00:00:00",
                        stop.time = "23:59:59",
                        timeWindow = "no",
                        windowSize = 100,
                        kernel = "no",
                        excludeValues = list(),
                        excludeAuthors = character(),
                        excludeSources = character(),
                        excludeSections = character(),
                        excludeTypes = character(),
                        invertValues = FALSE,
                        invertAuthors = FALSE,
                        invertSources = FALSE,
                        invertSections = FALSE,
                        invertTypes = FALSE,
                        fileFormat = NULL,
                        outfile = NULL) {

  # wrap the vectors of exclude values for document variables into Java arrays
  excludeAuthors <- .jarray(excludeAuthors)
  excludeSources <- .jarray(excludeSources)
  excludeSections <- .jarray(excludeSections)
  excludeTypes <- .jarray(excludeTypes)

  # compile exclude variables and values vectors
  dat <- matrix("", nrow = length(unlist(excludeValues)), ncol = 2)
  count <- 0
  if (length(excludeValues) > 0) {
    for (i in 1:length(excludeValues)) {
      if (length(excludeValues[[i]]) > 0) {
        for (j in 1:length(excludeValues[[i]])) {
          count <- count + 1
          dat[count, 1] <- names(excludeValues)[i]
          dat[count, 2] <- excludeValues[[i]][j]
        }
      }
    }
    var <- dat[, 1]
    val <- dat[, 2]
  } else {
    var <- character()
    val <- character()
  }
  var <- .jarray(var) # array of variable names of each excluded value
  val <- .jarray(val) # array of values to be excluded

  # encode R NULL as Java null value if necessary
  if (is.null(qualifier) || is.na(qualifier)) {
    qualifier <- .jnull(class = "java/lang/String")
  }
  if (is.null(fileFormat)) {
    fileFormat <- .jnull(class = "java/lang/String")
  }
  if (is.null(outfile)) {
    outfile <- .jnull(class = "java/lang/String")
  }

  # call rNetwork function to compute results
  .jcall(dna_api(),
         "V",
         "rNetwork",
         networkType,
         statementType,
         variable1,
         variable1Document,
         variable2,
         variable2Document,
         qualifier,
         qualifierDocument,
         qualifierAggregation,
         normalization,
         isolates,
         duplicates,
         start.date,
         stop.date,
         start.time,
         stop.time,
         timeWindow,
         as.integer(windowSize),
         kernel,
         var,
         val,
         excludeAuthors,
         excludeSources,
         excludeSections,
         excludeTypes,
         invertValues,
         invertAuthors,
         invertSources,
         invertSections,
         invertTypes,
         outfile,
         fileFormat
  )

  exporter <- .jcall(dna_api(), "Ldna/export/Exporter;", "getExporter") # get a reference to the Exporter object, in which results are stored

  if (networkType == "eventlist") { # assemble an event list in the form of a data frame of filtered statements
    f <- J(exporter, "getFilteredStatements", simplify = TRUE) # array list of filtered export statements; use J because array list return type not recognized using .jcall
    l <- list() # create a list for filtered statements, later to be converted to data frame, with one row per statement
    for (i in seq(.jcall(f, "I", "size")) - 1) { # loop through filtered statements, starting at 0
      fi <- f$get(as.integer(i)) # retrieve filtered statement i
      row <- list() # each filtered export statement is represented by a list, with multiple slots for the variables etc.
      row$statement_id <- .jcall(fi, "I", "getId") # store the statement ID
      row$time <- .jcall(fi, "J", "getDateTimeLong") # store the date/time in seconds since 1 January 1970; will be converted to POSIXct later because the conversion to data frame otherwise converts it back to long anyway
      values <- J(fi, "getValues") # array list of variables with values; use J instead of .jcall because array list return type not recognized using .jcall
      for (j in seq(.jcall(values, "I", "size")) - 1) { # loop through the variables
        vi <- values$get(as.integer(j)) # save variable/value j temporarily to access its contents
        dataType <- .jcall(vi, "S", "getDataType") # the data type of value j
        if (dataType == "long text") {
          row[[.jcall(vi, "S", "getKey")]] <- .jcall(vi, "S", "getValue") # store as character object under variable name if long text
        } else if (dataType == "short text") {
          row[[.jcall(vi, "S", "getKey")]] <- vi$getValue()$getValue() # extract character object from Entity object and store under variable name if short text
        } else {
          row[[.jcall(vi, "S", "getKey")]] <- vi$getValue() # store as integer under variable name if boolean or integer data type
        }
      }
      row$start_position <- .jcall(fi, "I", "getStart") # store start caret in document text
      row$stop_position <- .jcall(fi, "I", "getStop") # store end caret in document text
      row$text <- .jcall(fi, "S", "getText") # text of the statement between start and end caret
      row$coder <- .jcall(fi, "I", "getCoderId") # store coder ID; the user can merge this with other coder details like name and color later if needed
      row$document_id <- .jcall(fi, "I", "getDocumentId") # store the document ID of the document the statement is contained in
      row$document_title <- .jcall(fi, "S", "getTitle") # store the document title
      row$document_author <- .jcall(fi, "S", "getAuthor") # store the document author
      row$document_source <- .jcall(fi, "S", "getSource") # store the document source
      row$document_section <- .jcall(fi, "S", "getSection") # store the document section
      row$document_type <- .jcall(fi, "S", "getType") # store the document type
      l[[i + 1]] <- row # add the row to the list
    }
    d <- do.call(rbind.data.frame, l) # convert the list of lists to data frame
    d$time <- as.POSIXct(d$time, origin = "1970-01-01 00:00:00") # convert long date/time to POSIXct
    return(d)
  } else { # assemble a one-mode or two-mode matrix with attributes or a list of matrices (if time window)
    m <- .jcall(exporter, "[Ldna/export/Matrix;", "getMatrixResultsArray") # get list of Matrix objects from Exporter object
    l <- list() # create a list in which each result is stored; can be of length 1 if no time window is used
    for (t in 1:length(m)) { # loop through the matrices
      mat <- .jcall(m[[t]], "[[D", "getMatrix", simplify = TRUE) # get the resulting matrix at step t as a double[][] object and save as matrix
      rownames(mat) <- .jcall(m[[t]], "[S", "getRowNames", simplify = TRUE) # add the row names to the matrix
      colnames(mat) <- .jcall(m[[t]], "[S", "getColumnNames", simplify = TRUE) # add the column names to the matrix
      attributes(mat)$start <- as.POSIXct(.jcall(m[[t]], "J", "getStartLong"), origin = "1970-01-01") # add the start date/time of the result as an attribute to the matrix
      attributes(mat)$stop <- as.POSIXct(.jcall(m[[t]], "J", "getStopLong"), origin = "1970-01-01") # add the end date/time of the result as an attribute to the matrix
      if (length(m) > 1) {
        attributes(mat)$middle <- as.POSIXct(.jcall(m[[t]], "J", "getDateTimeLong"), origin = "1970-01-01") # add the mid-point date/time around which the time window is centered if the time window algorithm was used
      }
      attributes(mat)$numStatements <- .jcall(m[[t]], "I", "getNumStatements") # add the number of filtered statements the matrix is based on as an attribute to the matrix
      attributes(mat)$call <- match.call() # add the arguments of the call as an attribute to the matrix
      class(mat) <- c(paste0("dna_network_", networkType), class(mat)) # add "dna_network_onemode" or "dna_network_twomode" as a class label in addition to "matrix"
      l[[t]] <- mat # add the matrix to the list
    }
    if (length(m) == 1) {
      return(l[[1]]) # return the first matrix in the list if no time window was used
    } else {
      attributes(l)$call <- match.call() # add arguments of the call as an attribute also to the list, not just each network matrix
      class(l) <- c(paste0("dna_network_", networkType, "_timewindows"), class(l)) # add "dna_network_onemode_timewindows" or "dna_network_twomode_timewindows" to class label
      return(l) # return the list of network matrices
    }
  }
}

#' Convert a \code{dna_network_onemode} object to a matrix
#'
#' Convert a \code{dna_network_onemode} object to a matrix.
#'
#' Remove the attributes and \code{"dna_network_onemode"} class label from a
#' \code{dna_network_onemode} object and return it as a numeric matrix.
#'
#' @param x The \code{dna_network_onemode} object, as returned by the
#'   \code{\link{dna_network}} function.
#' @param ... Additional arguments. Currently not in use.
#'
#' @author Philip Leifeld
#'
#' @family networks
#'
#' @export
as.matrix.dna_network_onemode <- function(x, ...) {
  attr(x, "start") <- NULL
  attr(x, "stop") <- NULL
  attr(x, "numStatements") <- NULL
  attr(x, "call") <- NULL
  attr(x, "class") <- NULL
  return(x)
}

#' Convert a \code{dna_network_twomode} object to a matrix
#'
#' Convert a \code{dna_network_twomode} object to a matrix.
#'
#' Remove the attributes and \code{"dna_network_twomode"} class label from a
#' \code{dna_network_twomode} object and return it as a numeric matrix.
#'
#' @param x The \code{dna_network_twomode} object, as returned by the
#'   \code{\link{dna_network}} function.
#' @param ... Additional arguments. Currently not in use.
#'
#' @author Philip Leifeld
#'
#' @family networks
#'
#' @export
as.matrix.dna_network_twomode <- as.matrix.dna_network_onemode

#' Print a \code{dna_network_onemode} object
#'
#' Show details of a \code{dna_network_onemode} object.
#'
#' Print a one-mode network matrix and its attributes.
#'
#' @param x A \code{dna_network_onemode} object, as returned by the
#'   \code{\link{dna_network}} function.
#' @param trim Number of maximum characters to display in row and column labels
#'   of the matrix. Labels with more characters are truncated, and the last
#'   character is replaced by an asterisk (\code{*}).
#' @param attr Display attributes, such as the start and stop date and time, the
#'   number of statements on which the matrix is based, the function call and
#'   arguments on which the network matrix is based, and the full labels without
#'   truncation.
#' @param ... Additional arguments. Currently not in use.
#'
#' @author Philip Leifeld
#'
#' @family networks
#'
#' @export
print.dna_network_onemode <- function(x, trim = 5, attr = TRUE, ...) {
  rn <- rownames(x)
  cn <- colnames(x)
  rownames(x) <- sapply(rownames(x), function(r) if (nchar(r) > trim) paste0(substr(r, 1, trim - 1), "*") else r)
  colnames(x) <- sapply(colnames(x), function(r) if (nchar(r) > trim) paste0(substr(r, 1, trim - 1), "*") else r)
  x <- round(x, 2)
  if ("dna_network_onemode" %in% class(x)) {
    onemode <- TRUE
    class(x) <- class(x)[class(x) != "dna_network_onemode"]
  } else {
    onemode <- FALSE
    class(x) <- class(x)[class(x) != "dna_network_twomode"]
  }
  start <- attr(x, "start")
  attr(x, "start") <- NULL
  stop <- attr(x, "stop")
  attr(x, "stop") <- NULL
  ns <- attr(x, "numStatements")
  attr(x, "numStatements") <- NULL
  cl <- deparse(attr(x, "call"))
  attr(x, "call") <- NULL
  attr(x, "class") <- NULL
  print(x)
  if (attr) {
    cat("\nStart:", format(start, tz = "UTC"))
    cat("\nStop: ", format(stop, tz = "UTC"))
    cat("\nStatements:", ns)
    cat("\nCall:", trimws(cl))
    if (onemode) {
      cat("\n\nLabels:\n")
      cat(paste(1:length(rn), rn), sep = "\n")
    } else {
      cat("\n\nRow labels:\n")
      cat(paste(1:length(rn), rn), sep = "\n")
      cat("\nColumn labels:\n")
      cat(paste(1:length(cn), cn), sep = "\n")
    }
  }
}

#' Print a \code{dna_network_twomode} object
#'
#' Show details of a \code{dna_network_twomode} object.
#'
#' Print a two-mode network matrix and its attributes.
#'
#' @inheritParams print.dna_network_onemode
#'
#' @author Philip Leifeld
#'
#' @family networks
#'
#' @export
print.dna_network_twomode <- print.dna_network_onemode

#' Plot networks created using rDNA.
#'
#' Plot a network generated using \code{\link{dna_network}}.
#'
#' These functions plot \code{dna_network_onemode} and
#' \code{dna_network_onemode} objects generated by the \code{\link{dna_network}}
#' function. In order to use this function, please install the \code{igraph} and
#' \code{ggraph} packages. Different layouts for one- and two-mode networks are
#' available.
#'
#' @param object A \code{dna_network} object.
#' @param ... Additional arguments; currently not in use.
#' @param atts A \code{dna_attributes} object generated by
#'   \code{\link{dna_getAttributes}}. Provide this object and matching
#'   attributes when plotting custom node colors, node labels and/or node sizes.
#' @param layout The type of node layout to use. The following layouts are
#'   available from the \code{igraph} and \code{ggraph} packages at the time of
#'   writing:
#'   \itemize{
#'    \item \code{"stress"} (the default layout)
#'    \item \code{"bipartite"} (only for two-mode networks)
#'    \item \code{"backbone"}
#'    \item \code{"circle"}
#'    \item \code{"dh"}
#'    \item \code{"drl"}
#'    \item \code{"fr"}
#'    \item \code{"gem"}
#'    \item \code{"graphopt"}
#'    \item \code{"kk"}
#'    \item \code{"lgl"}
#'    \item \code{"mds"}
#'    \item \code{"nicely"}
#'    \item \code{"randomly"}
#'    \item \code{"star"}
#'   }
#'   See \link[ggraph]{layout_tbl_graph_igraph} for the current list of layouts.
#' @param edge_size_range Two values indicating the minimum and maximum value
#'   to scale edge widths.
#' @param edge_color Provide the name of a color for edge colors. The default
#'   \code{"NULL"} colors edges in line with the specified
#'   \code{qualifierAggregation} in \code{\link{dna_network}}.
#' @param edge_alpha Takes numeric values to control the alpha-transparency of
#'   edges. Possible values range from \code{0} (fully transparent) to \code{1}
#'   (fully visible).
#' @param node_size Takes positive numeric values to control the size of nodes.
#'   Also accepts numeric values matching an attribute of the \code{atts} object
#'   (see examples).
#' @param node_colors Provide the name of a color or use an attribute from the
#'   \code{atts} object for node colors (see examples). Defaults to
#'   \code{"black"}.
#' @param node_label If \code{TRUE}, the row names (in a one-mode network) or
#'   the row and column names (in a two-mode network) of the network matrix are
#'   used for node labels. Also accepts character objects matching one of the
#'   attribute variables of the \code{atts} object (see examples). \code{FALSE}
#'   turns off node labels.
#' @param font_size Controls the font size of the node labels.
#' @param truncate Sets the number of characters to which node labels should be
#'   truncated.
#' @param threshold Minimum threshold for which edges should be plotted.
#' @param giant_component Only plot the giant component (the biggest connected
#'   cluster) of the network. Defaults to \code{FALSE}.
#' @param exclude_isolates Exclude isolates (nodes with no connection to other
#'   nodes) from the plot. Defaults to \code{FALSE}.
#' @param max_overlaps Value to exclude node labels that overlap with too many
#'   other node labels (see \code{\link[ggrepel]{geom_label_repel}}. Defaults
#'   to \code{10}.
#' @param seed Numeric value passed to \link{set.seed}. Ensures that plots are
#'   reproducible.
#'
#' @examples
#' \dontrun{
#' dna_init()
#' dna_sample()
#' dna_openDatabase("sample.dna", coderId = 1, coderPassword = "sample")
#'
#' ## one-mode network examples
#'
#' # compute network matrix (subtract + normalization)
#' nw <- dna_network(networkType = "onemode",
#'                   qualifierAggregation = "subtract",
#'                   normalization = "average")
#'
#' # plot network
#' library("ggplot2")
#' autoplot(nw)
#'
#' # plot only positively weighted edges
#' autoplot(nw, threshold = 0)
#'
#' # congruence network
#' nw <- dna_network(networkType = "onemode",
#'                   qualifierAggregation = "congruence",
#'                   excludeValues = list("concept" =
#'                     c("There should be legislation to regulate emissions.")))
#' autoplot(nw)
#'
#' # use entity colors (here: colors of organizations) from attributes
#' atts <- dna_getAttributes(variableId = 2)
#' autoplot(nw, atts = atts, node_colors = "color", layout = "fr")
#'
#' # use colors from attributes (after editing some of them)
#' atts$color[atts$Type == "NGO"] <- "red" # change NGO color to red
#' atts$color[atts$Type == "Government"] <- "blue" # change government to blue
#' autoplot(nw, atts = atts, node_colors = "color") # plot with custom colors
#'
#' # use an attribute, such as type, to plot node labels
#' autoplot(nw, atts = atts, node_label = "Type")
#'
#' # plot node sizes according to the number of statements of entities;
#' # first, compute additional matrix to calculate the number of statements
#' nw_freq <- dna_network(networkType = "twomode",
#'                        qualifierAggregation = "ignore",
#'                        normalization = "no")
#' # then add frequency of statements as an attribute
#' atts$freq <- rowSums(nw_freq)[match(atts$value, rownames(nw_freq))]
#' # plot network with node sizes matching statement frequencies
#' autoplot(nw, atts = atts, node_size = "freq", node_colors = "color")
#'
#' # use igraph community detection for identification of network clusters;
#' # remove negative edge weights
#' nw[nw < 0] <- 0
#' # convert dna_network to igraph object
#' graph <- igraph::graph_from_adjacency_matrix(nw,
#'                                              mode = "undirected",
#'                                              weighted = TRUE,
#'                                              diag = FALSE,
#'                                              add.colnames = NULL,
#'                                              add.rownames = NA)
#' # compute communities using igraph cluster algorithms
#' # (here: fast and greedy as an illustration))
#' com <- igraph::cluster_fast_greedy(graph)
#' # add node community membership as an attribute
#' atts$membership <- com$membership[match(atts$value, com$names)]
#' # use community membership as node color
#' autoplot(nw, atts = atts, node_colors = "membership")
#' # or plot ellipses using ggforce package
#' library("ggforce")
#' autoplot(nw, atts = atts, node_colors = "color") +
#'   geom_mark_ellipse(aes(x = x,
#'                         y = y,
#'                         group = com$membership,
#'                         fill = com$membership),
#'                     show.legend = FALSE)
#'
#' # add legend to the network plot (here: colors mapped to type attribute)
#' autoplot(nw, atts = atts, node_colors = "color") +
#'   scale_color_identity(name = "",
#'                        labels = c("Government", "NGO", "Business"),
#'                        guide = "legend") +
#'   theme(legend.position = "bottom", # change legend position
#'         legend.text = element_text(size = 10)) # change legend font size
#'
#' ## two-mode network examples
#'
#' # compute two-mode network and plot it
#' nw <- dna_network(networkType = "twomode",
#'                   qualifierAggregation = "combine")
#' library("ggplot2")
#' autoplot(nw)
#'
#' # use entity colours (here: colors of organizations);
#' # first, retrieve attributes for first-mode entities (organizations)
#' atts <- dna_getAttributes(variableId = 2)
#' # then, retrieve attributes for second-mode entities (concepts)
#' atts2 <- dna_getAttributes(variableId = 3)
#' # combine both attribute objects
#' atts <- rbind(atts, atts2)
#' # plot the network using the attributes of both variables
#' autoplot(nw,
#'          atts = atts,
#'          node_colors = "color",
#'          layout = "bipartite",
#'          max_overlaps = 20)
#' # edit the colors before plotting
#' atts$color[atts$Type == "NGO"] <- "red" # change NGO color to red
#' atts$color[atts$Type == "Government"] <- "blue" # government actors in blue
#' # plot the network with custom colors
#' autoplot(nw, atts = atts, node_colors = "color")
#'
#' # use an attribute, such as type, to plot node labels
#' nw <- dna_network(networkType = "twomode",
#'                   qualifierAggregation = "subtract",
#'                   normalization = "activity")
#' autoplot(nw, atts = atts, node_label = "Type")
#'
#' # plot node sizes according the number of statements of entities;
#' # first, compute network matrix for plotting
#' nw <- dna_network(networkType = "twomode",
#'                   qualifierAggregation = "subtract",
#'                   normalization = "activity")
#' # compute dna_attributes objects
#' atts <- dna_getAttributes(variableId = 2)
#' atts2 <- dna_getAttributes(variableId = 3)
#' # compute additional matrix to calculate the number of statements
#' nw_freq <- dna_network(networkType = "twomode",
#'                        qualifierAggregation = "ignore",
#'                        normalization = "no")
#' # add frequency of statements as attribute
#' # compute statement frequencies of first-mode entities
#' atts$freq <- rowSums(nw_freq)[match(atts$value, rownames(nw_freq))]
#' # compute statement frequencies of second-mode entities
#' atts2$freq <- colSums(nw_freq)[match(atts2$value, colnames(nw_freq))]
#' # combine both attribute objects
#' atts <- rbind(atts, atts2)
#' # plot network with node sizes matching statement frequencies
#' autoplot(nw, atts = atts, node_size = "freq", node_colors = "color")
#'
#' # use igraph community detection for identification of network clusters
#' nw <- dna_network(networkType = "twomode",
#'                   qualifierAggregation = "subtract",
#'                   normalization = "activity")
#' # compute dna_attributes objects and combine them
#' atts <- dna_getAttributes(variableId = 2)
#' atts2 <- dna_getAttributes(variableId = 3)
#' atts <- rbind(atts, atts2)
#' # remove negative edge weights
#' nw[nw < 0] <- 0
#' # convert dna_network to igraph object
#' graph <- igraph::graph_from_incidence_matrix(nw,
#'                                              directed = FALSE,
#'                                              weighted = TRUE,
#'                                              add.names = NULL)
#' # compute communities using igraph cluster algorithms
#' # (here: fast and greedy as an illustration))
#' com <- igraph::cluster_fast_greedy(graph)
#' # add node community membership as an attribute
#' atts$membership <- com$membership[match(atts$value, com$names)]
#' # use community membership as node color
#' autoplot(nw, atts = atts, node_colors = "membership")
#' # or plot ellipses using ggforce
#' library("ggforce")
#' autoplot(nw, atts = atts, node_colors = "color") +
#'   geom_mark_ellipse(aes(x = x,
#'                     y = y,
#'                     group = com$membership,
#'                     fill = com$membership),
#'                     show.legend = FALSE)
#' }
#'
#' @author Tim Henrichsen
#'
#' @family networks
#'
#' @importFrom ggplot2 autoplot
#' @importFrom ggplot2 aes
#' @importFrom ggplot2 scale_color_identity
#' @importFrom rlang .data
#' @name autoplot.dna_network
NULL

#' @rdname autoplot.dna_network
#' @export
autoplot.dna_network_onemode <- function(object,
                                         ...,
                                         atts = NULL,
                                         layout = "auto",
                                         edge_size_range = c(0.2, 2),
                                         edge_color = NULL,
                                         edge_alpha = 1,
                                         node_size = 3,
                                         node_colors = "black",
                                         node_label = TRUE,
                                         font_size = 6,
                                         truncate = 50,
                                         threshold = NULL,
                                         giant_component = FALSE,
                                         exclude_isolates = FALSE,
                                         max_overlaps = 10,
                                         seed = 12345) {
  set.seed(seed)

  if (!grepl("dna_network", class(object)[1])) {
    stop("Invalid data object. Please compute a dna_network object with the ",
         "dna_network() function before plotting.")
  }

  if (!requireNamespace("igraph", quietly = TRUE)) {
    stop("The autoplot function requires the 'igraph' package to be installed.\n",
         "To do this, enter 'install.packages(\"igraph\")'.")
  }

  if (!requireNamespace("ggraph", quietly = TRUE)) {
    stop("The autoplot function requires the 'ggraph' package to be installed.\n",
         "To do this, enter 'install.packages(\"ggraph\")'.")
  }

  if (!is.null(atts) & !"dna_attributes" %in% class(atts)) {
    stop("Object provided in 'atts' is not a dna_attributes object. Please ",
         "provide a dna_attributes object using dna_getAttributes() or set atts ",
         "to NULL if you do not want to use DNA attributes.")
  }

  if (!is.numeric(truncate)) {
    truncate <- Inf
    warning("No numeric value provided for trimming of entities. Truncation ",
            "will be ignored.")
  }

  # Convert network matrix to igraph network
  if ("dna_network_onemode" %in% class(object)) {
    graph <- igraph::graph_from_adjacency_matrix(object,
                                                 mode = "undirected",
                                                 weighted = TRUE,
                                                 diag = FALSE,
                                                 add.colnames = NULL,
                                                 add.rownames = NA)
    igraph::V(graph)$shape <- "circle"
  } else if ("dna_network_twomode" %in% class(object)) {
    graph <- igraph::graph_from_incidence_matrix(object,
                                                 directed = FALSE,
                                                 weighted = TRUE,
                                                 add.names = NULL)
    igraph::V(graph)$shape <- ifelse(igraph::V(graph)$type, "square", "circle")
  }

  # Check if all entities are included in attributes object (if provided)
  if (!is.null(atts) & !(all(igraph::V(graph)$name %in% atts$value))) {
    miss <- which(!igraph::V(graph)$name %in% atts$value)
    stop("Some network entities are missing in the attributes object:\n",
         paste(igraph::V(graph)$name[miss], collapse = "\n"))
  }

  # Remove tie weights below threshold
  if (!is.null(threshold)) {
    graph <- igraph::delete_edges(graph, which(!igraph::E(graph)$weight >= threshold))
  }

  # Add node colors
  if (is.character(node_colors)) {
    if (!is.null(atts) & length(node_colors) == 1 && node_colors %in% colnames(atts)) {
      col_pos <- which(colnames(atts) == node_colors)
      igraph::V(graph)$color <- atts[match(igraph::V(graph)$name, atts$value), col_pos]
    } else if (length(node_colors) > 1 & length(node_colors) != igraph::vcount(graph)) {
      stop("Number of custom colors does not equal number of nodes in the network.")
    } else {
      igraph::V(graph)$color <- node_colors
    }
  } else {
    igraph::V(graph)$color <- "black"
  }

  # Add edge colors
  if (is.null(edge_color)) {
    if ("combine" %in% as.character(attributes(object)$call)) {
      igraph::E(graph)$color <- "green"
      igraph::E(graph)$color[igraph::E(graph)$weight == 2] <- "red"
      igraph::E(graph)$color[igraph::E(graph)$weight == 3] <- "blue"
      # Change edge weight for networks with combine aggregation
      igraph::E(graph)$weight[igraph::E(graph)$weight > 0] <- 1
    } else if ("subtract" %in% as.character(attributes(object)$call)) {
      igraph::E(graph)$color <- "green"
      igraph::E(graph)$color[igraph::E(graph)$weight < 0] <- "red"
    } else if ("congruence" %in% as.character(attributes(object)$call)) {
      igraph::E(graph)$color <- "green"
    } else if ("conflict" %in% as.character(attributes(object)$call)) {
      igraph::E(graph)$color <- "red"
    } else {
      igraph::E(graph)$color <- "gray"
    }
  } else if (!all(is.na(edge_color))) {
    if (length(edge_color) > 1 & length(edge_color) != igraph::ecount(graph)) {
      igraph::E(graph)$color <- "gray"
      warning("Number of custom edge_colors does not match number of edges ",
              "in the network. Will set edge_color to default (gray).")
    } else {
      igraph::E(graph)$color <- edge_color
    }
  } else {
    igraph::E(graph)$color <- "gray"
  }

  # Add node size(s)
  if (length(node_size) > 1 & length(node_size) != igraph::vcount(graph)) {
    igraph::V(graph)$size <- 7
    warning("Number of provided node size values does not equal number of ",
            "nodes in the network. node_size will be set to default value (7).")
  } else if (is.character(node_size) & length(node_size) == 1 & !is.null(atts) && node_size %in% colnames(atts)) {
    col_pos <- which(colnames(atts) == node_size)
    igraph::V(graph)$size <- atts[match(igraph::V(graph)$name, atts$value), col_pos]
  } else if (is.numeric(node_size)) {
    igraph::V(graph)$size <- node_size
  }

  # Add labels
  if (!is.logical(node_label)) {
    if (is.character(node_label) & length(node_label) == 1 & !is.null(atts) && node_label %in% colnames(atts)) {
      col_pos <- which(colnames(atts) == node_label)
      igraph::V(graph)$name <- atts[match(igraph::V(graph)$name, atts$value), col_pos]
    } else if (!is.null(node_label)) {
      if (length(node_label) > 1 & length(node_label) != igraph::vcount(graph)) {
        stop("Number of custom labels does not equal number of nodes in the network.")
      }
      igraph::V(graph)$name <- node_label
    }
  }

  # Remove isolates
  if (exclude_isolates) {
    graph <- igraph::delete_vertices(graph, igraph::degree(graph) == 0)
  }

  # Only plot giant component of network. Useful for some plotting algorithms.
  if (giant_component) {
    # Get giant component
    components <- igraph::clusters(graph)
    biggest_cluster_id <- which.max(components$csize)

    # Get members of giant component
    vert_ids <- igraph::V(graph)[components$membership == biggest_cluster_id]

    # Create subgraph
    graph <- igraph::induced_subgraph(graph, vert_ids)
  }


  # Truncate labels of entities
  igraph::V(graph)$name <- sapply(igraph::V(graph)$name, function(e) if (nchar(e) > truncate) paste0(substr(e, 1, truncate - 1), "*") else e)

  # Use absolute edge weight values for plotting
  igraph::E(graph)$weight <- abs(igraph::E(graph)$weight)

  # Start network plot
  g <- ggraph::ggraph(graph, layout = layout, ...) +
    suppressWarnings(ggraph::geom_edge_link(ggplot2::aes(edge_width = igraph::E(graph)$weight, edge_colour = igraph::E(graph)$color),
                                            alpha = edge_alpha,
                                            show.legend = FALSE)) + # add edges
    ggraph::scale_edge_width(range = edge_size_range) + # add edge scale
    ggraph::geom_node_point(ggplot2::aes(colour = igraph::V(graph)$color), # add nodes
                            size = igraph::V(graph)$size,
                            shape = igraph::V(graph)$shape,
                            show.legend = NA)
  # Add labels
  if ((!is.null(node_label) && !all(is.na(node_label))) && (is.character(node_label) || node_label == TRUE)) {
    g <- g +
      ggraph::geom_node_text(ggplot2::aes(label = igraph::V(graph)$name),
                             repel = TRUE,
                             max.overlaps = max_overlaps,
                             show.legend = FALSE)
  }

  # Add theme and set node colors and edges to identity
  g <- g +
    ggraph::theme_graph(base_family = "", base_size = font_size) +
    ggplot2::scale_color_identity() +
    ggraph::scale_edge_color_identity()

  return(g)
}

#' @rdname autoplot.dna_network
#' @export
autoplot.dna_network_twomode <- autoplot.dna_network_onemode

#' Convert a DNA network into a \code{tbl_graph} or \code{graph} object
#'
#' Convert a DNA network into a \code{tbl_graph} or \code{graph} object.
#'
#' Convert a \code{dna_network_onemode} or \code{dna_network_twomode} object
#' into a \code{tbl_graph} object as defined in the tidygraph package. These
#' objects can then be plotted using the ggraph package, which contains many
#' network layouts.
#'
#' \code{tbl_graph} objects are an extension of \code{graph}
#' objects defined in the igraph package. Functions for manipulating or plotting
#' the resulting objects from either the tidygraph or igraph package or both
#' can be used.
#'
#' The resulting objects can also be converted to \code{network} objects as
#' defined in the network package (part of the statnet suite of packages) using
#' the \code{asNetwork} function in the intergraph package.
#'
#' @param network A \code{dna_network_onemode} or \code{dna_network_twomode}
#'   object to be converted into a \code{tbl_graph} object. Can also be a matrix
#'   with edge weights and row and column names for the node labels.
#' @param attributes A \code{dna_attributes} object created using the
#'   \link{dna_getAttributes} function with attributes for the nodes in the
#'   network. Can also be a data frame with a \code{values} column that contains
#'   the node labels and further columns containing the attributes. The
#'   attributes are saved as node attributes in the \code{tbl_graph} object. If
#'   \code{NULL}, no attributes are included.
#' @param ... Further arguments. Currently not in use.
#'
#' @examples
#' \dontrun{
#' # prepare toy data
#' dna_sample()
#' dna_openDatabase("sample.dna", coderPassword = "sample")
#' nw <- dna_network(networkType = "onemode",
#'                   qualifierAggregation = "congruence",
#'                   excludeValues = list(concept =
#'                     "There should be legislation to regulate emissions."))
#' at <- dna_getAttributes(variableId = 2)
#'
#' # convert to tbl_graph object
#' g <- dna_tidygraph(nw, at)
#'
#' # basic visualization
#' ggraph::ggraph(g, layout = "fr") +
#'   ggraph::geom_edge_link() +
#'   ggraph::geom_node_point()
#'
#' # visualization with more bells and whistles
#' ggraph::ggraph(g, layout = "graphopt") +
#'   ggraph::geom_edge_link(ggplot2::aes(color = weight, width = weight)) +
#'   ggraph::geom_node_point(ggplot2::aes(color = color), size = 5) +
#'   ggplot2::scale_color_identity() +
#'   ggraph::scale_edge_color_gradient(low = "azure2", high = "azure4") +
#'   ggraph::theme_graph(background = "white") +
#'   ggraph::geom_node_text(ggplot2::aes(label = name),
#'                          repel = TRUE,
#'                          max.overlaps = 10,
#'                          show.legend = FALSE)
#' # for more layouts, see vignette("Layouts", package = "ggraph")
#'
#' # hive plot example
#' g <- g |>
#'   tidygraph::activate(nodes) |>
#'   tidygraph::mutate(centrality = tidygraph::centrality_betweenness())
#' ggraph::ggraph(g, layout = "hive", axis = Type, sort.by = centrality) +
#'   ggraph::geom_edge_hive(ggplot2::aes(colour = "gray", width = weight)) +
#'   ggraph::geom_axis_hive(ggplot2::aes(colour = color),
#'                                       size = 5,
#'                                       label = TRUE) +
#'   ggraph::scale_edge_color_identity() +
#'   theme(legend.position = "none")
#'
#' # example with negative edge weights
#' nw <- dna_network(networkType = "onemode",
#'                   qualifierAggregation = "subtract",
#'                   excludeValues = list(concept =
#'                     "There should be legislation to regulate emissions."))
#' g <- dna_tidygraph(nw, at)
#' ggraph::ggraph(g, layout = "linear", circular = TRUE) +
#'   ggraph::geom_edge_arc(aes(color = color, width = abs)) +
#'   ggraph::scale_edge_color_identity() +
#'   ggraph::geom_node_point(ggplot2::aes(color = color), size = 5) +
#'   ggplot2::scale_color_identity() +
#'   ggraph::theme_graph(background = "white") +
#'   theme(legend.position = "none") +
#'   ggraph::geom_node_text(ggplot2::aes(label = name),
#'                          repel = TRUE,
#'                          max.overlaps = 10,
#'                          show.legend = FALSE)
#'
#' # example with a two-mode network
#' nw <- dna_network(networkType = "twomode",
#'                   qualifierAggregation = "combine")
#' at1 <- dna_getAttributes(statementTypeId = 1, variable = "organization")
#' at2 <- dna_getAttributes(statementTypeId = 1, variable = "concept")
#' at1$Notes <- "organization"
#' at2$Notes <- "concept"
#' at <- rbind(at1, at2)
#' g <- dna_tidygraph(nw, at)
#' ggraph::ggraph(g, layout = "graphopt") +
#'   ggraph::geom_edge_link(ggplot2::aes(color = color), width = 1) +
#'   ggraph::scale_edge_color_identity() +
#'   ggraph::geom_node_point(ggplot2::aes(color = color, shape = Notes),
#'                           size = 5) +
#'   ggplot2::scale_color_identity() +
#'   ggraph::geom_node_text(ggplot2::aes(label = name),
#'                          repel = TRUE,
#'                          max.overlaps = 10,
#'                          show.legend = FALSE) +
#'   ggraph::theme_graph(background = "white") +
#'   theme(legend.position = "none")
#'
#' # manipulate and plot using the igraph package
#' library("igraph")
#' class(g) # resulting objects are both tbl_graph and igraph objects
#' igraph::V(g) # get the nodes using igraph functions
#' igraph::E(g) # get the edges using igraph functions
#' igraph::plot(g) # plot network using igraph package
#'
#' # convert to network object (network package, statnet suite of packages)
#' library("intergraph")
#' intergraph::asNetwork(g)
#' }
#'
#' @author Philip Leifeld
#' @family networks
#' @export
dna_tidygraph <- function(network, attributes = NULL, ...) {
  if (length(intersect(c("dna_network_onemode", "dna_network_twomode", "matrix"), class(network))) < 1) {
    stop("The 'network' argument must provide an object created by the 'dna_network' function or a matrix.")
  }
  if (!is.null(attributes) && (length(intersect(c("dna_attributes", "data.frame"), class(attributes))) < 1) || !"value" %in% colnames(attributes)) {
    stop("The 'attributes' argument must be NULL or created by the 'dna_getAttributes' function or a data frame with a 'values' column.")
  }
  if (!requireNamespace("tidygraph", quietly = TRUE) || packageVersion("tidygraph") < "1.3.1") {
    stop("The 'dna_tidygraph' function requires the 'tidygraph' package (>= 1.3.1) to be installed.\n",
         "To do this, enter 'install.packages(\"tidygraph\")'.")
  }

  if ("dna_network_twomode" %in% class(network)) {
    nodes <- data.frame(name = c(rownames(network), colnames(network)), type = c(rep(TRUE, nrow(network)), rep(FALSE, ncol(network))), stringsAsFactors = FALSE)
    edges <- data.frame(from = rep(rownames(network), times = ncol(network)), to = rep(colnames(network), each = nrow(network)), weight = as.vector(network))
    edges <- edges[edges$weight != 0, ]
    edges$from <- match(edges$from, nodes$name)
    edges$to <- match(edges$to, nodes$name)
    g <- tidygraph::tbl_graph(nodes = nodes, edges = edges, directed = FALSE) # create tbl_graph object for ggraph
  } else if ("dna_network_onemode" %in% class(network)) {
    g <- tidygraph::as_tbl_graph(network, directed = FALSE) # create tbl_graph object for ggraph
  } else {
    stop("Argument supplied by argument 'network' not recognized.")
  }

  if (!is.null(attributes)) {
    nodes <- tidygraph::as_tibble(g, active = "nodes")$name # extract nodes from graph for matching
    at <- attributes[attributes$value %in% nodes, ] # retain only those attributes present in the network
    at <- at[match(nodes, at$value), ] # sort attributes in the same order as the nodes in the graph
    g <- tidygraph::mutate(g, at[, colnames(at) != "value"]) # embed node attributes in graph
  }

  edges <- tidygraph::as_tibble(g, active = "edges") # extract edges from graph
  u <- unique(edges$weight) # unique edge weights
  combined <- length(u) < 5 && any(grepl("combine", attributes(network)$call)) # combined qualifier aggregation?
  edges$edgecol <- sapply(edges$weight, function(weight) { # create edge colors
    if (length(u) == 2 & all(sort(u) %in% 0:1) & weight > 0) { # binary: 1 = gray
      "gray"
    } else if (combined) { # "combined" qualifier aggregation
      if (weight == 1) {
        "green"
      } else if (weight == 2) {
        "red"
      } else if (weight == 3) {
        "blue"
      } else {
        "gray"
      }
    } else if (any(u < 0)) { # "subtract" (or something else that generates negative ties)
      if (weight < 0) {
        "red"
      } else {
        "green"
      }
    } else { # any other scale, for example "congruence" qualifier aggregation
      "gray"
    }
  })
  g <- g |> # assign absolute values, edge colors, and sign as edge attributes
    tidygraph::activate(edges) |>
    tidygraph::mutate(abs = abs(edges$weight),
                      color = edges$edgecol,
                      sign = ifelse(edges$weight < 0, "negative", "positive"))

  return(g)
}