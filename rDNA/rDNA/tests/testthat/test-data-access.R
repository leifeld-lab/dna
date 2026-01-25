context("data access")

test_that("DNA can use databases and profiles", {
  dna_init()
  s <- dna_sample(overwrite = TRUE)
  expect_equal(dim(dna_queryCoders("sample.dna")), c(4, 3))
  expect_false(dna_openDatabase(db_url = s, coderId = 12, coderPassword = "sample"))
  expect_false(dna_openDatabase(db_url = s, coderId = 2, coderPassword = "test"))
  expect_error(dna_openDatabase(db_url = "test.dna", coderId = 2, coderPassword = "sample"), "Database file not found")
  expect_output(dna_openDatabase(db_url = s, coderId = 2, coderPassword = "sample"), "DNA database: ")
  expect_output(dna_printDetails(), "DNA database: ")
  expect_output(dna_printDetails(), "41 statements in 7 documents")
  expect_true(dna_saveConnectionProfile(file = "profile.dnc", coderPassword = "sample"))
  expect_false(dna_saveConnectionProfile(file = "profile.dnc", coderPassword = "sample"))
  expect_output(dna_closeDatabase(), "Database was closed")
  expect_output(dna_closeDatabase(), "Tried to close database, but none was open")
  expect_false(dna_saveConnectionProfile(file = "profile2.dnc", coderPassword = "sample"))
  expect_error(dna_openConnectionProfile(file = "profile2.dnc", coderPassword = ""), "File does not exist")
  expect_false(dna_openConnectionProfile(file = "profile.dnc", coderPassword = "test"))
  expect_output(dna_openConnectionProfile(file = "profile.dnc", coderPassword = "sample"), "DNA database: ")
  dna_closeDatabase()
  unlink("profile.dnc")
  unlink("sample.dna")
})

test_that("dna_sample works", {
  dna_init()
  expect_equal(dna_sample(overwrite = TRUE), paste0(getwd(), "/sample.dna"))
  expect_true(file.exists(paste0(getwd(), "/sample.dna")))
  expect_gt(file.size(paste0(getwd(), "/sample.dna")), 200000)
  expect_warning(dna_sample(), "Sample file already exists")
  unlink("sample.dna")
})

test_that("statement management works", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")

  # dna_getStatements
  expect_no_error(st <- dna_getStatements())
  expect_s3_class(st, "dna_statements")
  expect_equal(nrow(st), 40)
  expect_equal(ncol(st), 10)
  expect_equal(colnames(st), c("ID", "statement_type_id", "document_id", "start", "stop", "coder_id", "person", "organization", "concept", "agreement"))
  expect_equal(as.character(sapply(st, class)), c("integer", "integer", "integer", "integer", "integer", "integer", "character", "character", "character", "integer"))
  expect_equal(st[2, 7], "Joel Bluestein")
  expect_equal(st[15, 9], "There should be legislation to regulate emissions.")
  expect_equal(st, dna_getStatements(statementType = "DNA Statement", statementIds = numeric()))

  # dna_addStatement
  doc_id <- max(st$document_id)
  last_statement_id <- max(st$ID)
  expect_no_condition(id <- dna_addStatement(documentID = doc_id, startCaret = 10, endCaret = 50, statementType = 1, coder = 2, organization = "Sierra Club", concept = "some new concept", agreement = 0))
  st_new <- dna_getStatements()
  expect_equal(last_statement_id + 1, id)
  expect_equal(max(st_new$ID), id)
  expect_equal(st_new$statement_type_id[st_new$ID == id], 1)
  expect_equal(st_new$document_id[st_new$ID == id], doc_id)
  expect_equal(st_new$start[st_new$ID == id], 10)
  expect_equal(st_new$stop[st_new$ID == id], 50)
  expect_equal(st_new$coder_id[st_new$ID == id], 2)
  expect_equal(st_new$person[st_new$ID == id], "")
  expect_equal(st_new$organization[st_new$ID == id], "Sierra Club")
  expect_equal(st_new$concept[st_new$ID == id], "some new concept")
  expect_equal(st_new$agreement[st_new$ID == id], 0)
  expect_no_condition(id <- dna_addStatement(documentID = doc_id, startCaret = 10, endCaret = 50, statementType = 1, coder = 2, organization = "Sierra Club", concept = "some new concept", agreement = FALSE))
  st_new <- dna_getStatements()
  expect_equal(id, last_statement_id + 2)
  expect_equal(st_new$agreement[st_new$ID == id], 0)
  expect_no_condition(id <- dna_addStatement(documentID = doc_id, startCaret = 10, endCaret = 50, statementType = 1, coder = 2, organization = "Sierra Club", concept = "some new concept", agreement = 3))
  st_new <- dna_getStatements()
  expect_equal(st_new$agreement[st_new$ID == id], 1)
  expect_error(id <- dna_addStatement(documentID = doc_id, startCaret = 10, endCaret = 50, statementType = 1, coder = 2, organization = 25, concept = "some new concept", agreement = FALSE), "java.lang.ClassCastException")

  dna_closeDatabase()
  unlink("sample.dna")
})

test_that("attribute management works", {
  dna_init()
  samp <- dna_sample(overwrite = TRUE)
  dna_openDatabase(samp, coderId = 1, coderPassword = "sample")

  # dna_getAttributes
  expect_error(dna_getAttributes(), "Please supply")
  at <- dna_getAttributes(variableId = 2)
  expect_s3_class(at, "dna_attributes")
  expect_equal(nrow(at), 8)
  expect_equal(ncol(at), 6)
  expect_equal(colnames(at), c("ID", "value", "color", "Type", "Alias", "Notes"))
  expect_equal(at[2, 2], "Alliance to Save Energy")
  expect_equal(at[2, 3], "#00CC00")
  expect_equal(at, dna_getAttributes(statementType = "DNA Statement", variable = "organization"))
  expect_equal(at, dna_getAttributes(statementTypeId = 1, variable = "organization"))

  # dna_setAttributes: error messages
  expect_error(dna_setAttributes(), "argument \"data\" is missing")
  expect_error(dna_setAttributes(15), "must be a data frame")
  expect_error(dna_setAttributes(variableId = 2, at[, -1]), "first column of 'data' must be named 'ID'")

  # dna_setAttributes: output
  at[2, 2] <- "new value"
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Added                            0           -         0           -")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Removed                          0           -         0           0")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Value updates                    -           0         1           4")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Color updates                    -           -         0           -")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num before                       3           -         8          40")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num after                        3           -         8          40")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "All changes were only simulated. The database remains unchanged.")

  # dna_setAttributes: recode value
  st_before <- dna_getStatements()
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = FALSE), "All changes have been written into the database.")
  st_after <- dna_getStatements()
  expect_equal(dim(st_before), dim(st_after))
  expect_equal(as.numeric(table(st_before == st_after)), c(4, 396)) # 4 statements updated as reported?

  # dna_setAttributes: update attributes
  at <- dna_getAttributes(variableId = 2)
  at$color[6] <- "#FF00FF"
  at$Alias[3] <- "Some alias"
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Value updates                    -           1         0           0")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = FALSE), "Color updates                    -           -         1           -")
  at <- dna_getAttributes(variableId = 2)
  expect_equal(at$color[6], "#FF00FF")
  expect_equal(at$Alias[3], "Some alias")

  # dna_setAttributes: add a new entity
  at[9, ] <- at[4, ]
  at$ID[9] <- -1
  at$value[9] <- "another new value"
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Added                            0           -         1           -")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num before                       3           -         8          40")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num after                        3           -         9          40")
  expect_no_error(dna_setAttributes(data = at, variableId = 2, simulate = FALSE)) # write new entity into the database
  at <- dna_getAttributes(variableId = 2)
  expect_equal(at$Type[at$value == "another new value"], "Business") # "Business" was copied from row 4

  # dna_setAttributes: remove an entity including statements
  at <- at[-2, ]
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Removed                          0           -         1           4")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num before                       3           -         9          40")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num after                        3           -         8          36")
  dna_setAttributes(data = at, variableId = 2, simulate = FALSE)
  expect_equal(dim(dna_getAttributes(variableId = 2)), c(8, 6)) # instead of previously c(9, 6)
  expect_equal(dim(dna_getStatements(statementType = 1)), c(36, 10)) # instead of previously c(40, 10)

  # dna_setAttributes: add a new attribute variable
  at$newVar <- letters[1:8] # add new attribute variable
  at <- at[, c(1:5, 7, 6)] # swap position of variables to make the challenge harder
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Added                            1           -         0           -")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Value updates                    -           8         0           0")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num before                       3           -         8          36")
  expect_output(dna_setAttributes(data = at, variableId = 2, simulate = TRUE), "Num after                        4           -         8          36")
  dna_setAttributes(data = at, variableId = 2, simulate = FALSE)
  expect_equal(dim(dna_getAttributes(variableId = 2)), c(8, 7))
  expect_equal(colnames(dna_getAttributes(variableId = 2)), c("ID", "value", "color", "Type", "Alias", "Notes", "newVar"))

  dna_closeDatabase()
  unlink("sample.dna")
})