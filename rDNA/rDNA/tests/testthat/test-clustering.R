context("clustering")

# example 1: compute 12 cluster solutions for one time point
test_that("Example 1 produces expected output", {
  testthat::skip_on_ci()
  skip_if_not_installed("igraph", minimum_version = "0.8.1")
  skip_if_not_installed("sna", minimum_version = "2.4")
  skip_if_not_installed("cluster", minimum_version = "1.12.0")
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  mc1 <- dna_multiclust(variable1 = "organization",
                        variable2 = "concept",
                        qualifier = "agreement",
                        duplicates = "document",
                        k = 0,
                        saveObjects = TRUE)
  expect_s3_class(mc1, "dna_multiclust")
  expect_equal(names(mc1), c("cl", "k", "max_mod", "memberships", "modularity"))
  expect_true(length(mc1$modularity) > 0)
  expect_true(length(mc1$max_mod) > 0)
  expect_true(length(mc1$memberships) > 0)
  expect_true(length(mc1$cl) > 0)
  dna_closeDatabase()
  unlink(samp)
})

# example 2: compute only Girvan-Newman edge betweenness with two clusters
test_that("Example 2 produces expected output", {
  testthat::skip_on_ci()
  skip_if_not_installed("igraph", minimum_version = "0.8.1")
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  set.seed(12345)
  mc2 <- dna_multiclust(k = 2,
                        single = FALSE,
                        average = FALSE,
                        complete = FALSE,
                        ward = FALSE,
                        kmeans = FALSE,
                        pam = FALSE,
                        equivalence = FALSE,
                        concor_one = FALSE,
                        concor_two = FALSE,
                        louvain = FALSE,
                        fastgreedy = FALSE,
                        walktrap = FALSE,
                        leading_eigen = FALSE,
                        edge_betweenness = TRUE,
                        infomap = FALSE,
                        label_prop = FALSE,
                        spinglass = FALSE)
  expect_s3_class(mc2, "dna_multiclust")
  expect_equal(names(mc2), c("k", "max_mod", "memberships", "modularity"))
  expect_true(length(mc2$modularity) > 0)
  expect_true(length(mc2$memberships) > 0)
  dna_closeDatabase()
  unlink(samp)
})

# example 3: smoothed modularity using time window algorithm
test_that("Example 3 produces expected output", {
  testthat::skip_on_ci()
  skip_if_not_installed("igraph", minimum_version = "0.8.1")
  skip_if_not_installed("sna", minimum_version = "2.4")
  skip_if_not_installed("cluster", minimum_version = "1.12.0")
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")
  mc3 <- dna_multiclust(k = 2,
                        timeWindow = "events",
                        windowSize = 28)
  expect_s3_class(mc3, "dna_multiclust")
  expect_equal(names(mc3), c("k", "max_mod", "memberships", "modularity"))
  expect_true(length(mc3$max_mod) > 0)
  dna_closeDatabase()
  unlink(samp)
})