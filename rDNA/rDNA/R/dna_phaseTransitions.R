#' Detect phase transitions and states in a discourse network
#'
#' Detect phase transitions and states in a discourse network.
#'
#' This function applies the state dynamics methods of Masuda and Holme to a
#' time window discourse network. It computes temporally overlapping discourse
#' networks, computes the dissimilarity between all networks, and clusters them.
#' For the dissimilarity, the sum of absolute edge weight differences and the
#' Euclidean spectral distance are available. Several clustering techniques can
#' be applied to identify the different stages and phases from the resulting
#' distance matrix.
#'
#' The function offers kernel smoothing, which means the farther away from a
#' time point a statement is, the less important it becomes for the network that
#' is created around the time point. Several kernel smoothing functions are
#' available; see the \code{kernel} argument.
#'
#' @param distanceMethod The distance measure that expresses the dissimilarity
#'   between any two network matrices. The following choices are available:
#'   \itemize{
#'     \item \code{"absdiff"}: The sum of the cell-wise absolute differences
#'       between the two matrices, i.e., the sum of differences in edge weights.
#'       This is equivalent to the graph edit distance because the network
#'       dimensions are kept constant across all networks by including all nodes
#'       at all time points (i.e., by including isolates).
#'     \item \code{"spectral"}: The Euclidean distance between the normalized
#'       eigenvalues of the graph Laplacian matrices, also called the spectral
#'       distance between two network matrices. Any negative values (e.g., from
#'       the subtract method) are replaced by zero before computing the
#'       distance.
#'   }
#' @param clusterMethods The clustering techniques that are applied to the
#'   distance matrix in the end. Hierarchical methods are repeatedly cut off at
#'   different levels, and solutions are compared using network modularity to
#'   pick the best-fitting cluster membership vector. Some of the methods are
#'   slower than others, hence they are not included by default. It is possible
#'   to include any number of methods in the argument. For each included method,
#'   the cluster membership vector (i.e., the states over time) along with the
#'   associated time stamps of the networks are returned, and the modularity of
#'   each included method is computed for comparison. The following methods are
#'   available:
#'   \itemize{
#'     \item \code{"single"}: Hierarchical clustering with single linkage using
#'       the \code{\link[stats]{hclust}} function from the \pkg{stats} package.
#'     \item \code{"average"}: Hierarchical clustering with average linkage
#'       using the \code{\link[stats]{hclust}} function from the \pkg{stats}
#'       package.
#'     \item \code{"complete"}: Hierarchical clustering with complete linkage
#'       using the \code{\link[stats]{hclust}} function from the \pkg{stats}
#'       package.
#'     \item \code{"ward"}: Hierarchical clustering with Ward's method (D2)
#'       using the \code{\link[stats]{hclust}} function from the \pkg{stats}
#'       package.
#'     \item \code{"kmeans"}: k-means clustering using the
#'       \code{\link[stats]{kmeans}} function from the \pkg{stats} package.
#'     \item \code{"pam"}: Partitioning around medoids using the
#'       \code{\link[cluster]{pam}} function from the \pkg{cluster} package.
#'     \item \code{"spectral"}: Spectral clustering. An affinity matrix using a
#'       Gaussian (RBF) kernel is created. The Laplacian matrix of the affinity
#'       matrix is computed and normalized. The first first k eigenvectors of
#'       the normalized Laplacian matrix are clustered using k-means.
#'     \item \code{"concor"}: CONvergence of iterative CORrelations (CONCOR)
#'       with exactly \code{k = 2} clusters. (Not included by default because of
#'       the limit to \code{k = 2}.)
#'     \item \code{"fastgreedy"}: Fast & greedy community detection using the
#'       \code{\link[igraph]{cluster_fast_greedy}} function in the \pkg{igraph}
#'       package.
#'     \item \code{"walktrap"}: Walktrap community detection using the
#'       \code{\link[igraph]{cluster_walktrap}} function in the \pkg{igraph}
#'       package.
#'     \item \code{"leading_eigen"}: Leading eigenvector community detection
#'       using the \code{\link[igraph]{cluster_leading_eigen}} function in the
#'       \pkg{igraph} package. (Can be slow, hence not included by default.)
#'     \item \code{"edge_betweenness"}: Girvan-Newman edge betweenness community
#'       detection using the \code{\link[igraph]{cluster_edge_betweenness}}
#'       function in the \pkg{igraph} package. (Can be slow, hence not included
#'       by default.)
#'   }
#' @param k.min For the hierarchical cluster methods, how many clusters or
#'   states should at least be identified? Only the best solution between
#'   \code{k.min} and \code{k.max} clusters is retained and compared to other
#'   methods.
#' @param k.max For the hierarchical cluster methods, up to how many clusters or
#'   states should be identified? Only the best solution between \code{k.min}
#'   and \code{k.max} clusters is retained and compared to other methods.
#' @param cores The number of computing cores for parallel processing. If
#'   \code{1} (the default), no parallel processing is used. If a larger number,
#'   the \pkg{pbmcapply} package is used to parallelize the clustering. Note
#'   that this method is based on forking and is only available on Unix
#'   operating systems, including MacOS and Linux. Note also that the remaining
#'   computations, including the computation of the distance matrix and the
#'   time window network generation with kernel smoothing, are done in parallel
#'   using threads in Java, irrespective of this setting, using as many parallel
#'   threads as cores are available on the system.
#' @param kernel Use kernel smoothing for computing network time slices? Several
#'   kernel smoothing functions are available, similar to kernel density
#'   estimation. They down-weight statements the farther they are temporally
#'   away from the temporal mid-point of the respective time slice. Valid
#'   settings are:
#'   \itemize{
#'     \item \code{"uniform"}: Weight all statements within a time window
#'       equally with a value of \code{0.5}.
#'     \item \code{"triangular"}: Use a triangular kernel function.
#'     \item \code{"epanechnikov"}: Use an Epanechnikov kernel smoother.
#'     \item \code{"gaussian"}: Use a standard normal distribution as a kernel
#'       smoother.
#'     \item \code{"no"}: Circumvent kernel smoothing and weight all statements
#'       with a value of \code{1.0}. This is a legacy setting and is slow and
#'       may not return the same results as \code{"uniform"} due to the way it
#'       was written up.
#'   }
#' @param normalizeToOne Divide all cells by the sum of all cells before
#'   computing the dissimilarity between two network matrices? This
#'   normalization scales all edge weights to a sum of \code{1.0}. Doing so can
#'   make networks more comparable by boosting the edge weights of networks that
#'   are relatively sparsely populated by concepts, for example at the beginning
#'   or end of the debate. Note that this normalization should not make any
#'   difference with Euclidean spectral distances of the graph Laplacian because
#'   the eigenvalues are normalized to sum to one in this distance method.
#' @param indentTime If \code{TRUE}, the sequence of time slices under the time
#'   window algorithm starts with the first network and ends with the last
#'   network that are entirely covered within the timeline defined by the start
#'   and stop dates and times. For example, if the start date is 1 February, the
#'   stop date is 31 December, and the time window duration is 21 days, the
#'   mid-point of the first time window will be 11 February (to ensure the first
#'   network entirely fits into the timeline), and the last network will be
#'   centered around 20 December (to ensure the last network entirely fits into
#'   the timeline). If \code{FALSE}, the start and stop dates and times are used
#'   as the first and last mid-points. In that case, the first and last few
#'   networks may contain fewer statements than other time slices and may,
#'   therefore, be more similar to each other. This can potentially be
#'   counter-acted by setting the \code{normalizeToOne} argument.
#' @inheritParams dna_network
#'
#' @examples
#' \dontrun{
#' library("ggplot2")
#' dna_init()
#' dna_sample()
#' dna_openDatabase("sample.dna", coderId = 1, coderPassword = "sample")
#'
#' # compute states and phases for sample dataset
#' results <- dna_phaseTransitions(distanceMethod = "spectral",
#'                                 clusterMethods = c("ward",
#'                                                    "pam",
#'                                                    "concor",
#'                                                    "walktrap"),
#'                                 k.min = 2,
#'                                 k.max = 6,
#'                                 networkType = "onemode",
#'                                 variable1 = "organization",
#'                                 variable2 = "concept",
#'                                 timeWindow = "days",
#'                                 windowSize = 15,
#'                                 kernel = "gaussian",
#'                                 indentTime = FALSE,
#'                                 normalizeToOne = FALSE)
#' results
#' autoplot(results)
#'
#' # access individual plots
#' plots <- autoplot(results)
#' plots[[1]] # show heatmap
#' plots[[2]] # show cluster silhouettes
#' plots[[3]] # show temporal embedding
#' plots[[4]] # show state dynamics
#'
#' # save plots to combined PDF
#' library("ggplotify") # needed to convert heatmap to ggplot diagram
#' library("patchwork") # needed to merge plots into 4 x 4 diagram
#' p1 <- ggplotify::as.ggplot(plots[[1]])
#' p <- p1 + plots[[2]] + plots[[3]] + plots[[4]] + plot_layout(ncol = 2)
#' ggsave(filename = "phase_transitions.pdf", p, width = 14, height = 12)
#' }
#'
#' @rdname dna_phaseTransitions
#' @author Philip Leifeld
#' @importFrom stats dist
#' @importFrom utils combn
#' @importFrom rJava .jarray .jcall .jnull J
#' @export
dna_phaseTransitions <- function(distanceMethod = "absdiff",
                                 clusterMethods = c("single",
                                                    "average",
                                                    "complete",
                                                    "ward",
                                                    "kmeans",
                                                    "pam",
                                                    "spectral",
                                                    "fastgreedy",
                                                    "walktrap"),
                                 k.min = 2,
                                 k.max = 6,
                                 cores = 1,
                                 networkType = "twomode",
                                 statementType = "DNA Statement",
                                 variable1 = "organization",
                                 variable1Document = FALSE,
                                 variable2 = "concept",
                                 variable2Document = FALSE,
                                 qualifier = "agreement",
                                 qualifierDocument = FALSE,
                                 qualifierAggregation = "subtract",
                                 normalization = "no",
                                 duplicates = "document",
                                 start.date = "01.01.1900",
                                 stop.date = "31.12.2099",
                                 start.time = "00:00:00",
                                 stop.time = "23:59:59",
                                 timeWindow = "days",
                                 windowSize = 200,
                                 kernel = "uniform",
                                 normalizeToOne = TRUE,
                                 indentTime = FALSE,
                                 excludeValues = list(),
                                 excludeAuthors = character(),
                                 excludeSources = character(),
                                 excludeSections = character(),
                                 excludeTypes = character(),
                                 invertValues = FALSE,
                                 invertAuthors = FALSE,
                                 invertSources = FALSE,
                                 invertSections = FALSE,
                                 invertTypes = FALSE) {

  # check arguments and packages
  if (distanceMethod == "spectral" && networkType == "twomode") {
    distanceMethod <- "absdiff"
    warning("Spectral distances only work with one-mode networks. Using 'distanceMethod = \"absdiff\"' instead.")
  }
  if (cores > 1 && !requireNamespace("pbmcapply", quietly = TRUE)) {
    pbmclapply <- FALSE
    warning("Argument 'cores' requires the 'pbmcapply' package, which is not installed.\nSetting 'cores = 1'. Consider installing the 'pbmcapply' package if you use Linux or MacOS.")
  }
  igraphMethods <- c("louvain", "fastgreedy", "walktrap", "leading_eigen", "edge_betweenness", "infomap", "label_prop", "spinglass")
  if (any(igraphMethods %in% clusterMethods) && !requireNamespace("igraph", quietly = TRUE)) {
    clusterMethods <- clusterMethods[-igraphMethods]
    warning("'igraph' package not installed. Dropping clustering methods from the 'igraph' package. Consider installing 'igraph'.")
  }
  if ("pam" %in% clusterMethods && !requireNamespace("cluster", quietly = TRUE)) {
    clusterMethods <- clusterMethods[which(clusterMethods != "pam")]
    warning("'cluster' package not installed. Dropping clustering methods from the 'cluster' package. Consider installing 'cluster'.")
  }
  if ("concor" %in% clusterMethods && k.min > 2) {
    clusterMethods <- clusterMethods[which(clusterMethods != "concor")]
    warning("Dropping 'concor' from clustering methods because the CONCOR implementation in rDNA can only find exactly two clusters, but the 'k.min' argument was larger than 2.")
  }
  clusterMethods <- rev(clusterMethods) # reverse order to save time during parallel computation by starting the computationally intensive methods first
  mcall <- match.call() # save the arguments for storing them in the results later

  # generate the time window networks
  if (is.null(timeWindow) || is.na(timeWindow) || !is.character(timeWindow) || length(timeWindow) != 1 || !timeWindow %in% c("events", "seconds", "minutes", "hours", "days", "weeks", "months", "years")) {
    timeWindow <- "events"
    warning("The 'timeWindow' argument was invalid. Proceeding with 'timeWindow = \"events\" instead.")
  }

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

  # call rNetwork function to compute results
  .jcall(dna_api(),
         "V",
         "rTimeWindow",
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
         TRUE,
         duplicates,
         start.date,
         stop.date,
         start.time,
         stop.time,
         timeWindow,
         as.integer(windowSize),
         kernel,
         normalizeToOne,
         indentTime,
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
         invertTypes
  )
  exporter <- dna_api()$getExporter() # save Java object reference to exporter class

  # compute distance matrix
  if (distanceMethod == "modularity") {
    stop("Differences in modularity have not been implemented yet. Please use absolute differences or spectral Euclidean distance as a distance method.")
  } else if (!distanceMethod %in% c("absdiff", "spectral")) {
    stop("Distance method not recognized. Try \"absdiff\" or \"spectral\".")
  }
  distance_mat <- .jcall(exporter,
                         "[[D",
                         "computeDistanceMatrix",
                         distanceMethod,
                         simplify = TRUE)
  distance_mat <- distance_mat / max(distance_mat) # rescale between 0 and 1

  # retrieve mid-point dates (gamma)
  m <- .jcall(exporter, "[Ldna/export/Matrix;", "getMatrixResultsArray") # get list of Matrix objects from Exporter object
  dates <- sapply(m, function(x) .jcall(x, "J", "getDateTimeLong")) # long integers, still needs conversion to date

  # define clustering function
  hclustMethods <- c("single", "average", "complete", "ward")
  cl <- function(method, distmat) {
    tryCatch({
      similarity_mat <- 1 - distmat
      g <- igraph::graph_from_adjacency_matrix(similarity_mat, mode = "undirected", weighted = TRUE, diag = FALSE) # graph needs to be based on similarity, not distance
      if (method %in% hclustMethods) {
        if (method == "single") {
          suppressWarnings(cl <- stats::hclust(as.dist(distmat), method = "single"))
        } else if (method == "average") {
          suppressWarnings(cl <- stats::hclust(as.dist(distmat), method = "average"))
        } else if (method == "complete") {
          suppressWarnings(cl <- stats::hclust(as.dist(distmat), method = "complete"))
        } else if (method == "ward") {
          suppressWarnings(cl <- stats::hclust(as.dist(distmat), method = "ward.D2"))
        }
        opt_k <- lapply(k.min:k.max, function(x) {
          mem <- stats::cutree(cl, k = x)
          mod <- igraph::modularity(x = g, weights = igraph::E(g)$weight, membership = mem)
          return(list(mem = mem, mod = mod))
        })
        mod <- sapply(opt_k, function(x) x$mod)
        kk <- which.max(mod)
        mem <- opt_k[[kk]]$mem
      } else if (method == "kmeans") {
        opt_k <- lapply(k.min:k.max, function(x) {
          suppressWarnings(cl <- stats::kmeans(distmat, centers = x))
          mem <- cl$cluster
          mod <- igraph::modularity(x = g, weights = igraph::E(g)$weight, membership = mem)
          return(list(cl = cl, mem = mem, mod = mod))
        })
        mod <- sapply(opt_k, function(x) x$mod)
        kk <- which.max(mod)
        mem <- opt_k[[kk]]$mem
      } else if (method == "pam") {
        opt_k <- lapply(k.min:k.max, function(x) {
          suppressWarnings(cl <- cluster::pam(distmat, k = x))
          mem <- cl$cluster
          mod <- igraph::modularity(x = g, weights = igraph::E(g)$weight, membership = mem)
          return(list(cl = cl, mem = mem, mod = mod))
        })
        mod <- sapply(opt_k, function(x) x$mod)
        kk <- which.max(mod)
        mem <- opt_k[[kk]]$mem
      } else if (method == "spectral") {
        sigma <- 1.0
        affinity_matrix <- exp(-distmat^2 / (2 * sigma^2))
        L <- diag(rowSums(affinity_matrix)) - affinity_matrix
        D.sqrt.inv <- diag(1 / sqrt(rowSums(affinity_matrix)))
        L.norm <- D.sqrt.inv %*% L %*% D.sqrt.inv
        eigenvalues <- eigen(L.norm) # eigenvalue decomposition
        opt_k <- lapply(k.min:k.max, function(x) {
          U <- eigenvalues$vectors[, 1:x]
          mem <- kmeans(U, centers = x)$cluster # cluster the eigenvectors
          mod <- igraph::modularity(x = g, weights = igraph::E(g)$weight, membership = mem)
          return(list(mem = mem, mod = mod))
        })
        mod <- sapply(opt_k, function(x) x$mod)
        kk <- which.max(mod)
        mem <- opt_k[[kk]]$mem
      } else if (method == "concor") {
        suppressWarnings(mi <- stats::cor(similarity_mat))
        iter <- 1
        while (any(abs(mi) <= 0.999) & iter <= 50) {
          mi[is.na(mi)] <- 0
          mi <- stats::cor(mi)
          iter <- iter + 1
        }
        mem <- ((mi[, 1] > 0) * 1) + 1
      } else if (method %in% igraphMethods) {
        if (method == "fastgreedy") {
          suppressWarnings(cl <- igraph::cluster_fast_greedy(g))
        } else if (method == "walktrap") {
          suppressWarnings(cl <- igraph::cluster_walktrap(g))
        } else if (method == "leading_eigen") {
          suppressWarnings(cl <- igraph::cluster_leading_eigen(g))
        } else if (method == "edge_betweenness") {
          suppressWarnings(cl <- igraph::cluster_edge_betweenness(g))
        } else if (method == "spinglass") {
          suppressWarnings(cl <- igraph::cluster_spinglass(g))
        }
        opt_k <- lapply(k.min:k.max, function(x) {
          mem <- igraph::cut_at(communities = cl, no = x)
          mod <- igraph::modularity(x = g, weights = igraph::E(g)$weight, membership = mem)
          return(list(mem = mem, mod = mod))
        })
        mod <- sapply(opt_k, function(x) x$mod)
        kk <- which.max(mod)
        mem <- opt_k[[kk]]$mem
      }
      list(method = method,
           modularity = igraph::modularity(x = g, weights = igraph::E(g)$weight, membership = mem),
           memberships = mem)
    },
    error = function(e) {
      warning("Cluster method '", method, "' could not be computed due to an error: ", e)
    },
    warning = function(w) {
      warning("Cluster method '", method, "' threw a warning: ", w)
    })
  }

  # apply all clustering methods to distance matrix
  if (cores > 1) {
    cat(paste("Clustering distance matrix on", cores, "cores.\n"))
    a <- Sys.time()
    l <- pbmcapply::pbmclapply(clusterMethods, cl, distmat = distance_mat, mc.cores = cores)
    b <- Sys.time()
  } else {
    cat("Clustering distance matrix... ")
    a <- Sys.time()
    l <- lapply(clusterMethods, cl, distmat = distance_mat)
    b <- Sys.time()
    cat(intToUtf8(0x2714), "\n")
  }
  print(b - a)
  for (i in length(l):1) {
    if (length(l[[i]]) == 1) {
      l <- l[-i]
      clusterMethods <- clusterMethods[-i]
    }
  }
  results <- list()
  mod <- sapply(l, function(x) x$modularity)
  best <- which(mod == max(mod))[1]
  results$modularity <- mod[best]
  results$clusterMethod <- clusterMethods[best]

  # temporal embedding via MDS
  if (!requireNamespace("MASS", quietly = TRUE)) {
    mem <- data.frame("date" = as.POSIXct(dates, tz = "UTC", origin = "1970-01-01"),
                      "state" = l[[best]]$memberships)
    results$states <- mem
    warning("Skipping temporal embedding because the 'MASS' package is not installed. Consider installing it.")
  } else {
    cat("Temporal embedding...\n")
    a <- Sys.time()
    distmat <- distance_mat + 1e-12
    mds <- MASS::isoMDS(distmat) # MDS of distance matrix
    points <- mds$points
    mem <- data.frame("date" = as.POSIXct(dates, tz = "UTC", origin = "1970-01-01"),
                      "state" = l[[best]]$memberships,
                      "X1" = points[, 1],
                      "X2" = points[, 2])
    results$states <- mem
    b <- Sys.time()
    print(b - a)
  }

  results$distmat <- distance_mat
  class(results) <- "dna_phaseTransitions"
  attributes(results)$stress <- ifelse(ncol(results$states) == 2, NA, mds$stress)
  attributes(results)$call <- mcall
  return(results)
}

#' Print the summary of a \code{dna_phaseTransitions} object
#'
#' Show details of a \code{dna_phaseTransitions} object.
#'
#' Print a summary of a \code{dna_phaseTransitions} object, which can be created
#' using the \link{dna_phaseTransitions} function.
#'
#' @param x A \code{dna_phaseTransitions} object.
#' @param ... Further options (currently not used).
#'
#' @author Philip Leifeld
#'
#' @rdname dna_phaseTransitions
#' @importFrom utils head
#' @export
print.dna_phaseTransitions <- function(x, ...) {
  cat(paste0("States: ", max(x$states$state), ". Cluster method: ", x$clusterMethod, ". Modularity: ", round(x$modularity, 3), ".\n\n"))
  print(utils::head(x$states, 20))
  cat(paste0("...", nrow(x$states), " further rows\n"))
}

#' @rdname dna_phaseTransitions
#' @param object A \code{"dna_phaseTransitions"} object.
#' @param ... Additional arguments. Currently not in use.
#' @param plots The plots to include in the output list. Can be one or more of
#'   the following: \code{"heatmap"}, \code{"silhouette"}, \code{"mds"},
#'   \code{"states"}.
#'
#' @author Philip Leifeld, Kristijan Garic
#' @importFrom ggplot2 autoplot ggplot aes geom_line geom_point xlab ylab labs ggtitle theme_bw theme arrow unit scale_shape_manual element_text scale_x_datetime scale_colour_manual guides
#' @importFrom rlang .data
#' @export
autoplot.dna_phaseTransitions <- function(object, ..., plots = c("heatmap", "silhouette", "mds", "states")) {
  # settings for all plots
  k <- max(object$states$state)
  shapes <- c(21:25, 0:14)[1:k]
  l <- list()

  # heatmap
  if ("heatmap" %in% plots) {
    try({
      if (!requireNamespace("heatmaply", quietly = TRUE)) {
        warning("Heatmap skipped because the 'heatmaply' package is not installed.")
      } else {
        l[[length(l) + 1]] <- heatmaply::ggheatmap(1 - object$distmat,
                                                   dendrogram = "both",
                                                   showticklabels = FALSE, # remove axis labels
                                                   show_dendrogram = TRUE,
                                                   hide_colorbar = TRUE)
      }
    })
  }

  # silhouette plot
  if ("silhouette" %in% plots) {
    try({
      if (!requireNamespace("cluster", quietly = TRUE)) {
        warning("Silhouette plot skipped because the 'cluster' package is not installed.")
      } else if (!requireNamespace("factoextra", quietly = TRUE)) {
        warning("Silhouette plot skipped because the 'factoextra' package is not installed.")
      } else {
        sil <- cluster::silhouette(object$states$state, dist(object$distmat))
        l[[length(l) + 1]] <- factoextra::fviz_silhouette(sil, print.summary = FALSE) +
          ggplot2::ggtitle(paste0("Cluster silhouettes (mean width: ", round(mean(sil[, 3]), 3), ")")) +
          ggplot2::ylab("Silhouette width") +
          ggplot2::labs(fill = "State", color = "State") +
          ggplot2::theme_classic() +
          ggplot2::theme(axis.text.x = element_blank(), axis.ticks.x = element_blank())
      }
    })
  }

  # temporal embedding
  if ("mds" %in% plots) {
    try({
      if (is.na(attributes(object)$stress)) {
        warning("No temporal embedding found. Skipping this plot.")
      } else if (!requireNamespace("igraph", quietly = TRUE)) {
        warning("Temporal embedding plot skipped because the 'igraph' package is not installed.")
      } else if (!requireNamespace("ggraph", quietly = TRUE)) {
        warning("Temporal embedding plot skipped because the 'ggraph' package is not installed.")
      } else {
        nodes <- object$states
        nodes$date <- as.character(nodes$date)
        nodes$State <- as.factor(nodes$state)

        # Extract state values
        state_values <- nodes$State

        edges <- data.frame(sender = as.character(object$states$date),
                            receiver = c(as.character(object$states$date[2:(nrow(object$states))]), "NA"))
        edges <- edges[-nrow(edges), ]
        g <- igraph::graph_from_data_frame(edges, directed = TRUE, vertices = nodes)
        l[[length(l) + 1]] <- ggraph::ggraph(g, layout = "manual", x = igraph::V(g)$X1, y = igraph::V(g)$X2) +
          ggraph::geom_edge_link(arrow = ggplot2::arrow(type = "closed", length = ggplot2::unit(2, "mm")),
                                 start_cap = ggraph::circle(1, "mm"),
                                 end_cap = ggraph::circle(2, "mm")) +
          ggraph::geom_node_point(ggplot2::aes(shape = state_values, fill = state_values), size = 2) +
          ggplot2::scale_shape_manual(values = shapes) +
          ggplot2::ggtitle("Temporal embedding (MDS)") +
          ggplot2::xlab("Dimension 1") +
          ggplot2::ylab("Dimension 2") +
          ggplot2::theme_bw() +
          ggplot2::guides(size = "none") +
          ggplot2::labs(shape = "State", fill = "State")
      }
    })
  }

  # state dynamics
  if ("states" %in% plots) {
    try({
      d <- data.frame(
        time = object$states$date,
        id = cumsum(c(TRUE, diff(object$states$state) != 0)),
        State = factor(object$states$state, levels = 1:k, labels = paste("State", 1:k)),
        time1 = as.Date(object$states$date)
      )

      # Extracting values
      time_values <- d$time
      state_values <- d$State
      id_values <- d$id

      l[[length(l) + 1]] <- ggplot2::ggplot(d, ggplot2::aes(x = time_values, y = state_values, colour = state_values)) +
        ggplot2::geom_line(aes(group = 1), linewidth = 2, color = "black", lineend = "square") +
        ggplot2::geom_line(aes(group = id_values), linewidth = 2, lineend = "square") +
        ggplot2::scale_x_datetime(date_labels = "%b %Y", breaks = "4 months") + # format x-axis as month year
        ggplot2::xlab("Time") +
        ggplot2::ylab("") +
        ggplot2::ggtitle("State dynamics") +
        ggplot2::theme_bw() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1)) +
        ggplot2::guides(linewidth = "none") +
        ggplot2::labs(color = "State")
    })
  }

  return(l)
}