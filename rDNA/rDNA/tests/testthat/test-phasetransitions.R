context("Testing phase transitions")

# Create a function to set up the database for tests
setup_dna_database <- function() {
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  return(samp)
}

# Create a function to clean up after tests
cleanup_dna_database <- function(samp) {
  dna_closeDatabase()
  unlink(samp)
}

test_that("dna_phaseTransitions produces expected output with help file example", {
  samp <- setup_dna_database()
  results <- dna_phaseTransitions(distanceMethod = "spectral",
                                  clusterMethods = c("ward",
                                                     "pam",
                                                     "concor",
                                                     "walktrap"),
                                  k.min = 2,
                                  k.max = 6,
                                  networkType = "onemode",
                                  variable1 = "organization",
                                  variable2 = "concept",
                                  timeWindow = "days",
                                  windowSize = 15,
                                  kernel = "gaussian",
                                  indentTime = FALSE,
                                  normalizeToOne = FALSE)
  expect_true("dna_phaseTransitions" %in% class(results))
  expect_true(!is.null(results$states))
  expect_true(!is.null(results$modularity))
  expect_true(!is.null(results$clusterMethod))
  expect_true(!is.null(results$distmat))

  plots <- autoplot(results)
  expect_type(plots, "list")
  expect_length(plots, 4)
  expect_true("egg" %in% class(plots[[1]]))
  expect_true("ggplot" %in% class(plots[[2]]))
  expect_true("ggplot" %in% class(plots[[3]]))
  expect_true("ggplot" %in% class(plots[[4]]))

  cleanup_dna_database(samp)
})
