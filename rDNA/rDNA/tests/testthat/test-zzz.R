context("cleanup")

teardown({
  unlink("sample.dna")
  unlink("profile.dnc")
  unlink("test.dna")
  unlink("java/*.jar")
  expect_false(file.exists("sample.dna"))
})