context("Testing polarization")

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

test_that("dna_polarization produces expected output with help file example", {
  samp <- setup_dna_database()

  p <- dna_polarization(timeWindow = "days",
                        windowSize = 8,
                        kernel = "gaussian",
                        normalizeScores = FALSE)
  expect_true("dna_polarization" %in% class(p))
  expect_length(p, 10)
  expect_length(p$finalMaxQs, 22)
  expect_true(is.numeric(p$finalMaxQs))
  expect_true("POSIXct" %in% class(p$startDates))
  expect_true("POSIXct" %in% class(p$middleDates))
  expect_true("POSIXct" %in% class(p$stopDates))
  expect_equal(names(p), c("finalMaxQs", "earlyConvergence", "maxQs", "sdQs", "avgQs", "startDates", "middleDates", "stopDates", "memberships", "labels"))

  plots <- autoplot(p)
  expect_type(plots, "list")
  expect_length(plots, 3)
  expect_true("ggplot" %in% class(plots[[1]]))
  expect_true("ggplot" %in% class(plots[[2]]))
  expect_true("ggplot" %in% class(plots[[3]]))

  p2 <- dna_polarization(timeWindow = "no",
                         normalizeScores = TRUE,
                         algorithm = "genetic",
                         randomSeed = 1234)
  expect_length(p2$finalMaxQs, 1)
  expect_equal(p2$finalMaxQs, 0.5342493, tolerance = 0.1)
  plots2 <- autoplot(p2)
  expect_length(plots2, 2)
  expect_true("ggplot" %in% class(plots2[[1]]))
  expect_true("ggplot" %in% class(plots2[[2]]))

  cleanup_dna_database(samp)
})
