#' Set a sensible future plan for the current OS
#'
#' Convenience wrapper to choose a `future` backend based on the operating system.
#'
#' @param workers Optional integer number of workers (passed to [future::plan()]).
#' @return Invisibly returns `NULL`. Called for its side-effect of setting the future plan.
#' @export
#' @importFrom future plan multicore multisession
willow <- function(workers = NULL) {
	if (is.null(workers)) {

    # If running under SLURM, respect allocation
    slurm_cores <- Sys.getenv("SLURM_CPUS_PER_TASK")

    if (nzchar(slurm_cores)) {
      workers <- as.integer(slurm_cores)
    } else {
      workers <- max(1, parallel::detectCores() - 1)
    }
  }
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

  stopifnot(is.function(badger_doThisToEachFile), is.function(badger_doThisAfterFiles))

  burrow <- function(outputFiles, burrow_doThisToEachFile, burrow_doThisAfterFiles, parFiles = TRUE, ...) {
    stopifnot(is.function(burrow_doThisToEachFile), is.function(burrow_doThisAfterFiles))

    if (parFiles) {
      stuffFromFiles <- future.apply::future_lapply(outputFiles, function(f) burrow_doThisToEachFile(f, ...))
    } else {
      stuffFromFiles <- vector("list", length(outputFiles))
      for (k in seq_along(outputFiles)) {
        stuffFromFiles[[k]] <- burrow_doThisToEachFile(outputFiles[k], ...)
      }
    }
    burrow_doThisAfterFiles(stuffFromFiles, ...)
  }

  if (!is.null(paramVals)) {
    stopifnot(length(paramVals) == length(parameterNames))
    thisParamSample <- as.numeric(paramVals)
  } else {
    stopifnot(length(distributionTypes) == length(parameterNames))
    stopifnot(length(distributionList) == length(parameterNames))
    thisParamSample <- rep(NA_real_, length(parameterNames))
    for (j in seq_along(parameterNames)) {
      type <- distributionTypes[j]
      if (type == "uniform") {
        thisParamSample[j] <- stats::runif(1, distributionList[[j]][1], distributionList[[j]][2])
      } else if (type == "normal") {
        thisParamSample[j] <- stats::rnorm(1, distributionList[[j]][1], distributionList[[j]][2])
      } else if (type == "lognormal") {
        thisParamSample[j] <- stats::rlnorm(1, distributionList[[j]][1], distributionList[[j]][2])
      } else if (type == "triangular") {
        thisParamSample[j] <- EnvStats::rtri(1, distributionList[[j]][1], distributionList[[j]][2], distributionList[[j]][3])
      } else if (type == "samples") {
        thisParamSample[j] <- base::sample(distributionList[[j]], 1)
      } else {
        stop("Unknown distribution type: ", type)
      }
    }
  }

  if (anyNA(thisParamSample)) {
    stop("One or more sampled parameters are NA. Check distributionTypes/distributionList.")
  }

  if (parFiles) {
    stuffReturnedFromBadgeringFiles <- future.apply::future_lapply(
      inputFiles,
      function(f){f <- unlist(f); badger_doThisToEachFile(f, parameterNames, thisParamSample, ...)}
    )
  } else {
    stuffReturnedFromBadgeringFiles <- vector("list", length(inputFiles))
    for (k in seq_along(inputFiles)) {
      stuffReturnedFromBadgeringFiles[[k]] <- badger_doThisToEachFile(inputFiles[k], parameterNames, thisParamSample, ...)
    }
  }

  badger_result <- badger_doThisAfterFiles(stuffReturnedFromBadgeringFiles, ...)

  if (isTRUE(useBurrow)) {
    if (!is.function(burrow_doThisToEachFile) || !is.function(burrow_doThisAfterFiles)) {
      stop("useBurrow=TRUE requires burrow_doThisToEachFile and burrow_doThisAfterFiles to be functions.")
    }
    outputFiles <- badger_result[["outputFiles"]]
    if (is.null(outputFiles)) {
      stop("useBurrow=TRUE but badger_result does not contain an element named 'outputFiles'.")
    }
    return(burrow(outputFiles, burrow_doThisToEachFile, burrow_doThisAfterFiles, parFiles, ...))
  }

  badger_result
}


