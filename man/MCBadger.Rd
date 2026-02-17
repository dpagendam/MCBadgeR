\name{MCBadger}
\alias{MCBadger}
\title{Run Monte Carlo badgering workflow}
\usage{MCBadger(numSamples, parameterNames, paramTable = NULL, distributionTypes, inputFiles, distributionList, parParams = FALSE, parFiles = TRUE, badger_doThisToEachFile, badger_doThisAfterFiles, useBurrow = FALSE, burrow_doThisToEachFile = NULL, burrow_doThisAfterFiles = NULL, ...)}
\description{Run multiple Monte Carlo samples by applying user-provided functions across scenario files, optionally post-processing outputs.}
\arguments{
  \item{numSamples}{Number of samples to run.}
  \item{parameterNames}{Parameter names.}
  \item{paramTable}{Optional table of parameter values (rows are samples).}
  \item{distributionTypes}{Distribution type per parameter.}
  \item{inputFiles}{Input file paths.}
  \item{distributionList}{Distribution specifications per parameter.}
  \item{parParams}{Parallelise over parameter samples.}
  \item{parFiles}{Parallelise over files within each sample.}
  \item{badger_doThisToEachFile}{Function applied to each input file.}
  \item{badger_doThisAfterFiles}{Function applied after all files.}
  \item{useBurrow}{If TRUE, run burrow stage on outputFiles returned by badger stage.}
  \item{burrow_doThisToEachFile}{Function applied to each output file.}
  \item{burrow_doThisAfterFiles}{Function applied after all output files.}
  \item{...}{Passed through to user functions.}
}
\value{A list of results, one per sample.}
\keyword{internal}
