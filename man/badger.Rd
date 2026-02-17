\name{badger}
\alias{badger}
\title{Apply one parameter sample across files}
\usage{badger(parameterNames, paramVals = NULL, distributionTypes, inputFiles, distributionList, parFiles = TRUE, badger_doThisToEachFile, badger_doThisAfterFiles, useBurrow = FALSE, burrow_doThisToEachFile = NULL, burrow_doThisAfterFiles = NULL, ...)}
\description{Apply a parameter sample across input files and summarise using user-provided functions.}
\arguments{
  \item{parameterNames}{Parameter names.}
  \item{paramVals}{Optional parameter values; if NULL, values are sampled.}
  \item{distributionTypes}{Distribution type per parameter.}
  \item{inputFiles}{Input file paths.}
  \item{distributionList}{Distribution specifications per parameter.}
  \item{parFiles}{Parallelise over files.}
  \item{badger_doThisToEachFile}{Function applied to each input file.}
  \item{badger_doThisAfterFiles}{Function applied after all files.}
  \item{useBurrow}{If TRUE, run burrow stage on outputFiles.}
  \item{burrow_doThisToEachFile}{Function applied to each output file.}
  \item{burrow_doThisAfterFiles}{Function applied after all output files.}
  \item{...}{Passed through to user functions.}
}
\value{Result of badger_doThisAfterFiles(), or burrow result if useBurrow=TRUE.}
\keyword{internal}
