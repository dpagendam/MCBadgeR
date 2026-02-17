#' Set a sensible future plan for the current OS
#'
#' Convenience wrapper to choose a `future` backend based on the operating system.
#'
#' @param workers Optional integer number of workers (passed to [future::plan()]).
#' @return Invisibly returns `NULL`. Called for its side-effect of setting the future plan.
#' @export
#' @importFrom future plan multicore multisession
willow <- function(workers = NULL) {
  # Worker Initialisation for Local Launch of Operating system specific Workflows
  os <- tolower(Sys.info()[["sysname"]])

  if (os == "windows") {
    message("Windows detected. Using multisession plan.")
    future::plan(future::multisession, workers = workers)
  } else if (os == "darwin") {
    message("macOS detected. Using multicore plan.")
    future::plan(future::multicore, workers = workers)
  } else if (os == "linux") {
    message("Linux detected. Using multicore plan.")
    future::plan(future::multicore, workers = workers)
  } else {
    message("Unknown OS. Falling back to multisession.")
    future::plan(future::multisession, workers = workers)
  }

  invisible(NULL)
}

#' Run Monte Carlo "badgering" over a set of input files
#'
#' Generates or accepts parameter samples, applies `badger_doThisToEachFile()` across
#' `inputFiles`, then aggregates with `badger_doThisAfterFiles()`. Optionally, if
#' `useBurrow = TRUE`, uses `burrow()` to post-process output files returned by the
#' badger stage.
#'
#' @param numSamples Integer number of Monte Carlo samples to run. Ignored if `paramTable` is provided and `parParams = TRUE`
#'   (in that case, each row is one sample).
#' @param parameterNames Character vector of parameter names (passed through to the user functions).
#' @param paramTable Optional matrix/data.frame of parameter values, one sample per row.
#' @param distributionTypes Character vector of distribution types per parameter. Supported: `"uniform"`, `"normal"`,
#'   `"lognormal"`, `"triangular"`, `"samples"`.
#' @param inputFiles Character vector of input file paths.
#' @param distributionList List of distribution specifications per parameter.
#' @param parParams Logical. If `TRUE`, parallelise over parameter samples.
#' @param parFiles Logical. If `TRUE`, parallelise over files within each sample.
#' @param badger_doThisToEachFile Function called for each file. Signature should be
#'   `f(file, parameterNames, thisParamSample, ...)`.
#' @param badger_doThisAfterFiles Function called once after processing all files for a sample.
#'   Receives the list output from `badger_doThisToEachFile` and must return a list-like object.
#'   If `useBurrow = TRUE`, it should contain an element named `"outputFiles"`.
#' @param useBurrow Logical. If `TRUE`, call [burrow()] on the `"outputFiles"` produced by the badger stage.
#' @param burrow_doThisToEachFile Function called for each output file in the burrow stage.
#' @param burrow_doThisAfterFiles Function called once after processing all output files in the burrow stage.
#' @param ... Passed through to user functions.
#'
#' @return A list of results, one per sample. If `useBurrow = TRUE`, each element is the burrow result; otherwise each element is the badger result.
#' @export
#' @importFrom future.apply future_lapply
MCBadger <- function(numSamples,
                     parameterNames,
                     paramTable = NULL,
                     distributionTypes,
                     inputFiles,
                     distributionList,
                     parParams = FALSE,
                     parFiles = TRUE,
                     badger_doThisToEachFile,
                     badger_doThisAfterFiles,
                     useBurrow = FALSE,
                     burrow_doThisToEachFile = NULL,
                     burrow_doThisAfterFiles = NULL,
                     ...) {

  stopifnot(is.numeric(numSamples), length(numSamples) == 1, numSamples >= 1)
  stopifnot(is.character(parameterNames), length(parameterNames) >= 1)
  stopifnot(is.character(inputFiles), length(inputFiles) >= 1)

  badger_outputs <- list()

  # If paramTable provided, prefer running exactly those rows
  if (!is.null(paramTable)) {
    paramTable <- as.matrix(paramTable)
    if (parParams) {
      paramList <- split(paramTable, row(paramTable))
      badger_outputs <- future.apply::future_lapply(
        paramList,
        function(param) {
          badger(
            parameterNames = parameterNames,
            paramVals = as.numeric(param),
            distributionTypes = distributionTypes,
            inputFiles = inputFiles,
            distributionList = distributionList,
            parFiles = parFiles,
            badger_doThisToEachFile = badger_doThisToEachFile,
            badger_doThisAfterFiles = badger_doThisAfterFiles,
            useBurrow = useBurrow,
            burrow_doThisToEachFile = burrow_doThisToEachFile,
            burrow_doThisAfterFiles = burrow_doThisAfterFiles,
            ...
          )
        }
      )
    } else {
      n <- min(nrow(paramTable), numSamples)
      for (i in seq_len(n)) {
        paramVal <- as.numeric(paramTable[i, ])
        badger_outputs[[i]] <- badger(
          parameterNames = parameterNames,
          paramVals = paramVal,
          distributionTypes = distributionTypes,
          inputFiles = inputFiles,
          distributionList = distributionList,
          parFiles = parFiles,
          badger_doThisToEachFile = badger_doThisToEachFile,
          badger_doThisAfterFiles = badger_doThisAfterFiles,
          useBurrow = useBurrow,
          burrow_doThisToEachFile = burrow_doThisToEachFile,
          burrow_doThisAfterFiles = burrow_doThisAfterFiles,
          ...
        )
      }
    }
  } else {
    # No paramTable: generate numSamples independent samples
    if (parParams) {
      iterations <- as.list(seq_len(numSamples))
      badger_outputs <- future.apply::future_lapply(
        iterations,
        function(i) {
          badger(
            parameterNames = parameterNames,
            paramVals = NULL,
            distributionTypes = distributionTypes,
            inputFiles = inputFiles,
            distributionList = distributionList,
            parFiles = parFiles,
            badger_doThisToEachFile = badger_doThisToEachFile,
            badger_doThisAfterFiles = badger_doThisAfterFiles,
            useBurrow = useBurrow,
            burrow_doThisToEachFile = burrow_doThisToEachFile,
            burrow_doThisAfterFiles = burrow_doThisAfterFiles,
            ...
          )
        }
      )
    } else {
      for (i in seq_len(numSamples)) {
        badger_outputs[[i]] <- badger(
          parameterNames = parameterNames,
          paramVals = NULL,
          distributionTypes = distributionTypes,
          inputFiles = inputFiles,
          distributionList = distributionList,
          parFiles = parFiles,
          badger_doThisToEachFile = badger_doThisToEachFile,
          badger_doThisAfterFiles = badger_doThisAfterFiles,
          useBurrow = useBurrow,
          burrow_doThisToEachFile = burrow_doThisToEachFile,
          burrow_doThisAfterFiles = burrow_doThisAfterFiles,
          ...
        )
      }
    }
  }

  badger_outputs
}

#' Apply one parameter sample across all input files and summarise
#'
#' @param parameterNames Character vector of parameter names.
#' @param paramVals Optional numeric vector of parameter values. If `NULL`, values are sampled from `distributionList`.
#' @param distributionTypes Character vector of distribution types per parameter.
#' @param inputFiles Character vector of input file paths.
#' @param distributionList List of distribution specifications per parameter.
#' @param parFiles Logical. If `TRUE`, parallelise over files.
#' @param badger_doThisToEachFile Function applied to each file.
#' @param badger_doThisAfterFiles Function applied after all files.
#' @param useBurrow Logical. If `TRUE`, call [burrow()] using `"outputFiles"` in `badger_result`.
#' @param burrow_doThisToEachFile Function applied to each output file (burrow stage).
#' @param burrow_doThisAfterFiles Function applied after all output files (burrow stage).
#' @param ... Passed through to user functions.
#'
#' @return The output of `badger_doThisAfterFiles()` (or `burrow_doThisAfterFiles()` if `useBurrow = TRUE`).
#' @export
#' @importFrom future.apply future_lapply
#' @importFrom EnvStats rtri
badger <- function(parameterNames,
                   paramVals = NULL,
                   distributionTypes,
                   inputFiles,
                   distributionList,
                   parFiles = TRUE,
                   badger_doThisToEachFile,
                   badger_doThisAfterFiles,
                   useBurrow = FALSE,
                   burrow_doThisToEachFile = NULL,
                   burrow_doThisAfterFiles = NULL,
                   ...) {

  thisParamSample <- rep(NA_real_, length(parameterNames))

  if (!is.null(paramVals)) {
    thisParamSample <- as.numeric(paramVals)
  } else {
    for (j in seq_along(parameterNames)) {
      if (distributionTypes[j] == "uniform") {
        thisParamSample[j] <- stats::runif(1, distributionList[[j]][1], distributionList[[j]][2])
      }
      if (distributionTypes[j] == "normal") {
        thisParamSample[j] <- stats::rnorm(1, distributionList[[j]][1], distributionList[[j]][2])
      }
      if (distributionTypes[j] == "lognormal") {
        thisParamSample[j] <- stats::rlnorm(1, distributionList[[j]][1], distributionList[[j]][2])
      }
      if (distributionTypes[j] == "triangular") {
        thisParamSample[j] <- EnvStats::rtri(1, distributionList[[j]][1], distributionList[[j]][2], distributionList[[j]][3])
      }
      if (distributionTypes[j] == "samples") {
        thisParamSample[j] <- base::sample(distributionList[[j]], 1)
      }
    }
  }

  if (parFiles) {
    stuffReturnedFromBadgeringFiles <- future.apply::future_lapply(
      inputFiles,
      function(f) badger_doThisToEachFile(f, parameterNames, thisParamSample, ...)
    )
  } else {
    stuffReturnedFromBadgeringFiles <- vector("list", length(inputFiles))
    for (k in seq_along(inputFiles)) {
      stuffReturnedFromBadgeringFiles[[k]] <- badger_doThisToEachFile(inputFiles[k], parameterNames, thisParamSample, ...)
    }
  }

  badger_result <- badger_doThisAfterFiles(stuffReturnedFromBadgeringFiles)

  if (isTRUE(useBurrow)) {
    outputFiles <- badger_result[["outputFiles"]]
    if (is.null(outputFiles)) {
      stop("useBurrow=TRUE but badger_result does not contain an element named 'outputFiles'.")
    }
    burrow_result <- burrow(outputFiles, burrow_doThisToEachFile, burrow_doThisAfterFiles, parFiles, ...)
    return(burrow_result)
  }

  badger_result
}

#' Post-process output files ("burrow")
#'
#' @param outputFiles Character vector of output file paths.
#' @param burrow_doThisToEachFile Function applied to each output file.
#' @param burrow_doThisAfterFiles Function applied after all output files have been processed.
#' @param parFiles Logical. If `TRUE`, parallelise over output files.
#' @param ... Passed through to user functions.
#' @return Result of `burrow_doThisAfterFiles()`.
#' @export
#' @importFrom future.apply future_lapply
burrow <- function(outputFiles, burrow_doThisToEachFile, burrow_doThisAfterFiles, parFiles = TRUE, ...) {

  if (parFiles) {
    stuffFromFiles <- future.apply::future_lapply(outputFiles, function(f) burrow_doThisToEachFile(f, ...))
  } else {
    stuffFromFiles <- vector("list", length(outputFiles))
    for (k in seq_along(outputFiles)) {
      stuffFromFiles[[k]] <- burrow_doThisToEachFile(outputFiles[k], ...)
    }
  }

  burrow_doThisAfterFiles(stuffFromFiles)
}
