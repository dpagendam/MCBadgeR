\name{burrow}
\alias{burrow}
\title{Post-process output files}
\usage{burrow(outputFiles, burrow_doThisToEachFile, burrow_doThisAfterFiles, parFiles = TRUE, ...)}
\description{Apply user-provided functions across output files and summarise.}
\arguments{
  \item{outputFiles}{Output file paths.}
  \item{burrow_doThisToEachFile}{Function applied to each output file.}
  \item{burrow_doThisAfterFiles}{Function applied after all output files.}
  \item{parFiles}{Parallelise over files.}
  \item{...}{Passed through to user functions.}
}
\value{Result of burrow_doThisAfterFiles().}
\keyword{internal}
