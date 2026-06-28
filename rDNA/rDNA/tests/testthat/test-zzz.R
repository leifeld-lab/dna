context("cleanup")

teardown({
  unlink("sample.dna")
  unlink("profile.dnc")
  unlink("test.dna")
  expect_false(file.exists("sample.dna"))
})