context("Testing IRT scaling")

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

test_that("dna_scale1dbin produces expected output with help file example", {
  testthat::skip_on_ci()
  samp <- setup_dna_database()

  sink(nullfile())
  fit_1d_bin <- dna_scale1dbin(
    variable1 = "organization",
    variable2 = "concept",
    qualifier = "agreement",
    threshold = 0.49,
    theta_constraints = list(
      `National Petrochemical & Refiners Association` = "+",
      `Alliance to Save Energy` = "-"),
    mcmc_iterations = 20000,
    mcmc_burnin = 2000,
    mcmc_thin = 10,
    mcmc_normalize = TRUE,
    theta_prior_mean = 0,
    theta_prior_variance = 1,
    alpha_beta_prior_mean = 0,
    alpha_beta_prior_variance = 0.25,
    store_variables = "both",
    drop_constant_concepts = FALSE,
    drop_min_actors = 1,
    verbose = TRUE,
    seed = 12345
  )
  sink()

  expect_contains(class(fit_1d_bin), "dna_scale")
  expect_contains(class(fit_1d_bin), "dna_scale1dbin")
  expect_equal(names(fit_1d_bin), c("sample", "ability", "discrimination", "difficulty", "call"))
  expect_equal(dim(fit_1d_bin$sample), c(2000, 17))
  expect_equal(dim(fit_1d_bin$ability), c(7, 9))
  expect_equal(dim(fit_1d_bin$discrimination), c(5, 8))
  expect_equal(dim(fit_1d_bin$difficulty), c(5, 8))
  expect_true(length(utils::capture.output(fit_1d_bin)) > 1)
  plot_1d_bin <- autoplot(fit_1d_bin)
  expect_length(plot_1d_bin, 9)
  classes <- unique(sapply(plot_1d_bin, class)[2, ])
  expect_length(classes, 1)
  expect_equal(classes, "ggplot")

  cleanup_dna_database(samp)
})

test_that("dna_scale2dbin produces expected output with help file example", {
  testthat::skip_on_ci()
  samp <- setup_dna_database()

  sink(nullfile())
  fit_2d_bin <- dna_scale2dbin(
    variable1 = "organization",
    variable2 = "concept",
    qualifier = "agreement",
    threshold = 0.4,
    item_constraints = list(
      `Climate change is caused by greenhouse gases (CO2).` = list(2, "-"),
      `Climate change is caused by greenhouse gases (CO2).` = c(3, 0),
      `CO2 legislation will not hurt the economy.` = list(3, "-")),
    mcmc_iterations = 20000,
    mcmc_burnin = 2000,
    mcmc_thin = 10,
    alpha_beta_prior_mean = 0,
    alpha_beta_prior_variance = 1,
    store_variables = "organization",
    drop_constant_concepts = TRUE,
    verbose = TRUE,
    seed = 12345
  )
  sink()

  expect_contains(class(fit_2d_bin), "dna_scale")
  expect_contains(class(fit_2d_bin), "dna_scale2dbin")
  expect_equal(names(fit_2d_bin), c("sample", "ability", "call"))
  expect_equal(dim(fit_2d_bin$sample), c(2000, 14))
  expect_equal(dim(fit_2d_bin$ability), c(7, 12))
  expect_true(length(utils::capture.output(fit_2d_bin)) > 1)
  plot_2d_bin <- autoplot(fit_2d_bin)
  expect_length(plot_2d_bin, 3)
  classes <- unique(sapply(plot_2d_bin, class)[2, ])
  expect_length(classes, 1)
  expect_equal(classes, "ggplot")

  cleanup_dna_database(samp)
})

test_that("dna_scale1dord produces expected output with help file example", {
  testthat::skip_on_ci()
  samp <- setup_dna_database()

  sink(nullfile())
  expect_warning(fit_1d_ord <- dna_scale1dord(
    variable1 = "organization",
    variable2 = "concept",
    qualifier = "agreement",
    zero_as_na = TRUE,
    threshold = 0.4,
    lambda_constraints = list(`CO2 legislation will not hurt the economy.` = list(2, "-")),
    mcmc_iterations = 20000,
    mcmc_burnin = 2000,
    mcmc_thin = 10,
    mcmc_tune = 1.5,
    mcmc_normalize = FALSE,
    lambda_prior_mean = 0,
    lambda_prior_variance = 0.1,
    store_variables = "organization",
    drop_constant_concepts = TRUE,
    verbose = TRUE,
    seed = 12345
  ), "Setting 'zero_as_na' to FALSE because there are otherwise only 1s in the data matrix")
  sink()

  expect_contains(class(fit_1d_ord), "dna_scale")
  expect_contains(class(fit_1d_ord), "dna_scale1dord")
  expect_equal(names(fit_1d_ord), c("sample", "ability", "call"))
  expect_equal(dim(fit_1d_ord$sample), c(2000, 7))
  expect_equal(dim(fit_1d_ord$ability), c(7, 9))
  expect_true(length(utils::capture.output(fit_1d_ord)) > 1)
  plot_1d_ord <- autoplot(fit_1d_ord)
  expect_length(plot_1d_ord, 3)
  classes <- unique(sapply(plot_1d_ord, class)[2, ])
  expect_length(classes, 1)
  expect_equal(classes, "ggplot")

  cleanup_dna_database(samp)
})

test_that("dna_scale2dord produces expected output with help file example", {
  testthat::skip_on_ci()
  samp <- setup_dna_database()

  sink(nullfile())
  expect_warning(fit_2d_ord <- dna_scale2dord(
    variable1 = "organization",
    variable2 = "concept",
    qualifier = "agreement",
    zero_as_na = TRUE,
    threshold = 0.4,
    lambda_constraints = list(
      `Climate change is caused by greenhouse gases (CO2).` = list(2, "-"),
      `Climate change is caused by greenhouse gases (CO2).` = list(3, 0),
      `CO2 legislation will not hurt the economy.` = list(3, "-")),
    mcmc_iterations = 20000,
    mcmc_burnin = 2000,
    mcmc_thin = 10,
    mcmc_tune = 1.5,
    lambda_prior_mean = 0,
    lambda_prior_variance = 0.1,
    store_variables = "both",
    drop_constant_concepts = TRUE,
    verbose = TRUE,
    seed = 12345
  ), "'threshold' is not supported and will be ignored.")
  sink()

  expect_contains(class(fit_2d_ord), "dna_scale")
  expect_contains(class(fit_2d_ord), "dna_scale2dord")
  expect_equal(names(fit_2d_ord), c("sample", "ability", "discrimination", "difficulty", "call"))
  expect_equal(dim(fit_2d_ord$sample), c(2000, 28))
  expect_equal(dim(fit_2d_ord$ability), c(7, 12))
  expect_equal(dim(fit_2d_ord$discrimination), c(5, 11))
  expect_equal(dim(fit_2d_ord$difficulty), c(5, 8))
  expect_true(length(utils::capture.output(fit_2d_ord)) > 1)
  plot_2d_ord <- autoplot(fit_2d_ord)
  expect_length(plot_2d_ord, 9)
  classes <- unique(sapply(plot_2d_ord, class)[2, ])
  expect_length(classes, 1)
  expect_equal(classes, "ggplot")

  cleanup_dna_database(samp)
})
