context("backbone")

test_that("Penalized backbone works", {
  samp <- dna_sample(overwrite = TRUE)
  dna_init()
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "penalty",
                    penalty = 3.5,
                    iterations = 10000,
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  output <- capture.output(b)
  expect_match(output[1], "Backbone method: penalty")
  expect_length(b$backbone, 3)
  expect_true(is.character(b$backbone))
  expect_length(b$redundant, 3)
  expect_true(is.character(b$redundant))
  expect_true(is.numeric(b$unpenalized_backbone_loss))
  expect_length(b$unpenalized_backbone_loss, 1)
  expect_true(b$unpenalized_backbone_loss > 0)
  expect_true(is.numeric(b$unpenalized_redundant_loss))
  expect_length(b$unpenalized_redundant_loss, 1)
  expect_true(b$unpenalized_redundant_loss > 0)
  expect_true("dna_network_onemode" %in% class(b$backbone_network))
  expect_true("dna_network_onemode" %in% class(b$redundant_network))
  expect_true("dna_network_onemode" %in% class(b$full_network))
  expect_equal(b$iterations, 10000)
  expect_equal(attributes(b)$method, "penalty")
  expect_s3_class(b, "dna_backbone")
  expect_equal(dim(b$diagnostics), c(10000, 9))
  expect_false(any(is.na(b$diagnostics)))
  dna_closeDatabase()
  unlink(samp)
})

test_that("Plot method works for backbones with penalty", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "penalty",
                    penalty = 3.5,
                    iterations = 10000,
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  expect_no_error(plot(b, ma = 500))
  expect_no_warning(plot(b, ma = 500))
  expect_no_message(plot(b, ma = 500))
  fn <- tempfile()
  pdf(fn)
  plot(b, ma = 500)
  dev.off()
  expect_true(file.exists(fn))
  file.remove(fn)
  dna_closeDatabase()
  unlink(samp)
})

test_that("Autoplot method works for backbones with penalty", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "penalty",
                    penalty = 3.5,
                    iterations = 10000,
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  skip_if_not_installed("ggplot2")
  library("ggplot2")
  p <- autoplot(b)
  expect_true("list" %in% class(p))
  expect_length(p, 4)
  expect_true(any(unique(as.character(sapply(p, class))) %in% c("gg", "ggplot")))
  dna_closeDatabase()
  unlink(samp)
})

test_that("Fixed backbone works", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "fixed",
                    backboneSize = 4,
                    iterations = 2000,
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  output <- capture.output(b)
  expect_match(output[1], "Backbone method: fixed")
  expect_length(b$backbone, 4)
  expect_true(is.character(b$backbone))
  expect_length(b$redundant, 2)
  expect_true(is.character(b$redundant))
  expect_true(is.numeric(b$unpenalized_backbone_loss))
  expect_length(b$unpenalized_backbone_loss, 1)
  expect_true(b$unpenalized_backbone_loss > 0)
  expect_true(is.numeric(b$unpenalized_redundant_loss))
  expect_length(b$unpenalized_redundant_loss, 1)
  expect_true(b$unpenalized_redundant_loss > 0)
  expect_true("dna_network_onemode" %in% class(b$backbone_network))
  expect_true("dna_network_onemode" %in% class(b$redundant_network))
  expect_true("dna_network_onemode" %in% class(b$full_network))
  expect_equal(b$iterations, 2000)
  expect_equal(attributes(b)$method, "fixed")
  expect_s3_class(b, "dna_backbone")
  expect_equal(dim(b$diagnostics), c(2000, 9))
  expect_false(any(is.na(b$diagnostics)))
  dna_closeDatabase()
  unlink(samp)
})

test_that("Plot method works for fixed backbone size", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "fixed",
                    backboneSize = 4,
                    iterations = 2000,
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  expect_no_error(plot(b, ma = 500))
  expect_no_warning(plot(b, ma = 500))
  expect_no_message(plot(b, ma = 500))
  fn <- tempfile()
  pdf(fn)
  plot(b, ma = 500)
  dev.off()
  expect_true(file.exists(fn))
  file.remove(fn)
  dna_closeDatabase()
  unlink(samp)
})

test_that("Autoplot method works for backbones with fixed size", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "fixed",
                    backboneSize = 4,
                    iterations = 2000,
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  skip_if_not_installed("ggplot2")
  library("ggplot2")
  p <- autoplot(b)
  expect_true("list" %in% class(p))
  expect_length(p, 4)
  expect_true(any(unique(as.character(sapply(p, class))) %in% c("gg", "ggplot")))
  dna_closeDatabase()
  unlink(samp)
})

test_that("Nested backbone works", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "nested",
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  output <- capture.output(b)
  expect_match(output[1], "Backbone method: nested")
  expect_true(is.numeric(b$backboneLoss))
  expect_true(is.numeric(b$redundantLoss))
  expect_true(is.character(b$entity))
  expect_true(is.integer(b$i))
  expect_true(is.numeric(b$statements))
  expect_equal(dim(b), c(6, 5))
  expect_equal(colnames(b), c("i", "entity", "backboneLoss", "redundantLoss", "statements"))
  expect_equal(attributes(b)$numStatementsFull, 23)
  expect_equal(attributes(b)$method, "nested")
  expect_s3_class(b, "dna_backbone")
  expect_false(any(is.na(b)))
  dna_closeDatabase()
  unlink(samp)
})

test_that("Plot method works for nested backbone", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "nested",
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  expect_no_error(plot(b, ma = 500))
  expect_no_warning(plot(b, ma = 500))
  expect_no_message(plot(b, ma = 500))
  fn <- tempfile()
  pdf(fn)
  plot(b, ma = 500)
  dev.off()
  expect_true(file.exists(fn))
  file.remove(fn)
  dna_closeDatabase()
  unlink(samp)
})

test_that("Autoplot method works for nested backbones", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_backbone(method = "nested",
                    variable1 = "organization",
                    variable2 = "concept",
                    qualifier = "agreement",
                    qualifierAggregation = "subtract",
                    normalization = "average")
  skip_if_not_installed("ggplot2")
  library("ggplot2")
  p <- autoplot(b)
  expect_true(any(class(p) %in% c("ggraph", "gg", "ggplot")))
  dna_closeDatabase()
  unlink(samp)
})


test_that("Evaluate backbone solution works", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  b <- dna_evaluateBackboneSolution(
    c("There should be legislation to regulate emissions.",
      "Emissions legislation should regulate CO2.")
  )
  expect_length(b, 2)
  dna_closeDatabase()
  unlink(samp)
})