context("data access")

preparation <- function() {
  dna_sample(overwrite = TRUE)
  dna_openDatabase(db_url = "sample.dna", coderId = 1, coderPassword = "sample")
}

cleanup <- function() {
  dna_closeDatabase()
  unlink("sample.dna")
}

test_that("dna_network, as.matrix, print, autoplot, and dna_tidygraph work", {
  preparation()

  nw <- dna_network(networkType = "twomode")
  expect_true(class(nw)[1] == "dna_network_twomode")
  expect_equal(dim(nw), 7:6)
  expect_equal(sum(nw), 40)

  expect_true(class(as.matrix(nw))[1] == "matrix")
  expect_equal(dim(as.matrix(nw)), 7:6)
  expect_equal(sum(as.matrix(nw)), 40)

  expect_output(print(nw), "CO2 \\* Cap")
  expect_output(print(nw), "Ener\\*     2     0")
  expect_output(print(nw), "Start: 2005-01-26\\nStop:  2005-02-16\\nStatements: 40\\nCall")
  expect_output(print(nw), "Row labels:\\n1 Alliance to Save Energy")
  expect_output(print(nw), "Column labels:\\n1 CO2 legislation will not hurt the economy")

  nw <- dna_network(networkType = "onemode")
  expect_true(class(nw)[1] == "dna_network_onemode")
  expect_equal(dim(nw), c(7, 7))
  expect_equal(sum(nw), 276)

  expect_true(class(as.matrix(nw))[1] == "matrix")
  expect_equal(dim(as.matrix(nw)), c(7, 7))
  expect_equal(sum(as.matrix(nw)), 276)

  expect_output(print(nw), "Alli\\* Ener")
  expect_output(print(nw), "Envi\\*    10     2")
  expect_output(print(nw), "Start: 2005-01-26\\nStop:  2005-02-16\\nStatements: 40\\nCall")
  expect_output(print(nw), "Labels:\\n1 Alliance to Save Energy")

  cleanup()
})

test_that("dna_tidygraph works", {
  skip_if_not_installed("tidygraph", minimum_version = "1.3.1")
  preparation()

  nw <- dna_network(networkType = "twomode")
  at <- rbind(dna_getAttributes(nw, variableId = 2), dna_getAttributes(nw, variableId = 3))
  g <- dna_tidygraph(nw, attributes = at)
  expect_true("igraph" %in% class(g))
  expect_true("tbl_graph" %in% class(g))
  expect_equal(tidygraph::activate(g, "nodes") |> tidygraph::as_tibble() |> dim(), c(13, 7))
  expect_equal(tidygraph::activate(g, "nodes") |> tidygraph::as_tibble() |> colnames(), c("name", "type", "ID", "color", "Type", "Alias", "Notes"))
  expect_true(all(grepl("^#[0-9a-fA-F]{6}$", tidygraph::activate(g, "nodes") |> tidygraph::as_tibble() |> tidygraph::pull("color"))))
  expect_equal(tidygraph::activate(g, "edges") |> tidygraph::as_tibble() |> dim(), c(22, 6))
  expect_equal(tidygraph::activate(g, "edges") |> tidygraph::as_tibble() |> colnames(), c("from", "to", "weight", "abs", "color", "sign"))

  nw <- dna_network(networkType = "onemode")
  at <- dna_getAttributes(nw, variableId = 2)
  g <- dna_tidygraph(nw, attributes = at)
  expect_true("igraph" %in% class(g))
  expect_true("tbl_graph" %in% class(g))
  expect_equal(tidygraph::activate(g, "nodes") |> tidygraph::as_tibble() |> dim(), 7:6)
  expect_equal(tidygraph::activate(g, "nodes") |> tidygraph::as_tibble() |> colnames(), c("name", "ID", "color", "Type", "Alias", "Notes"))
  expect_true(all(grepl("^#[0-9a-fA-F]{6}$", tidygraph::activate(g, "nodes") |> tidygraph::as_tibble() |> tidygraph::pull("color"))))
  expect_equal(tidygraph::activate(g, "edges") |> tidygraph::as_tibble() |> dim(), c(21, 6))
  expect_equal(tidygraph::activate(g, "edges") |> tidygraph::as_tibble() |> colnames(), c("from", "to", "weight", "abs", "color", "sign"))

  cleanup()
})
