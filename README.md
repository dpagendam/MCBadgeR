![MCBadgeR hex sticker](man/figures/mcbadger_hex.png)

# MCBadgeR

Lightweight helpers for running Monte Carlo sampling workflows ("badgering" an underlying model with different parameters) and where a model can be run over multiple scenario input files.  There is also optional functionality for "burrowing" through outputs, with parallelism powered by the `future` ecosystem.


## Workflow of the Package
The core function of the package is called `MCBadger` which is intended to perform a Monte Carlo (MC) sampling of parameters of some underlying model.  For each parameter set that a model is run with, there are two user-provided functions that MCBadger will always execute: `badger_doThisToEachFile` (executed once for each file named in `inputFiles`) and `badger_doThisAfterFiles` which will be run once after `badger_doThisToEachFile` has finished iterating over all input files.
Each of the outputs that is returned from `badger_doThisToEachFile` will be stored as an item in a `list` and this `list` is the sole argument that will be passed to `badger_doThisAfterFiles`.  It is for the individual user to decide what these functions should be and what makes sense for each function to output.  However, there are some examples provided below to help illustrate the utility and flexibility of MCBadger.
`MCBadger` can be used with or without "burrowing".  Burrowing is turned on by setting the argument `useBurrow = TRUE` in `MCBadger`.  When turned on, there is an additional workflow that is performed following `badger_doThisAfterFiles`.  Burrowing executes a similar workflow to the one already described, utilising two user-provided functions called `burrow_doThisToEachFile` and `burrow_doThisAfterFiles`.  
`burrow_doThisToEachFile` is run once for each file name provided in `outputFiles`.  It is intended to find the useful results that you want to extract from the outputs of a model.  Whatever is output from each run of `burrow_doThisToEachFile` is appended to a `list`.  Once all output files have been iterated over, the `list` from burrowing is then provided as the argument to `burrow_doThisAfterFiles`.
Usually, `burrow_doThisAfterFiles` would be used to summarise the results from all of the outputs and return the output that the user wants to keep for the given parameter set.

MCBadger allows the user to parallelise the computation of MCBadger via two input arguments: `parParams` and `parFiles`.  Setting  `parParams = TRUE` forces the Monte Carlo runs of the model to be parallelised over compute cores, whereas `parFiles = TRUE` will allow the runs of `badger_doThisToEachFile` and `burrow_doThisToEachFile` to be parallelised over compute cores.  Note that `parParams = TRUE` requires the model, input files and output files to be copied to individual folders for each compute core to operate on.  The user would need to include these operations as part of their `badger_doThisToEachFile` function. Also, ensure that you have adequate disk space for this to occur.
It is not advisable to set `parParams` and `parFiles` both to `TRUE`.  Please choose one mode of parallelisation at the risk of overloading your system and potentially getting nonsense results from your model.

If you wish to use the `MCBadger` function with parallelisation provided by either `parParams` or `parFiles` then you will need to run the `willow` function before running `MCBadger`.  `willow` will initialise a number of worker cores in a manner that is consistent with your operating system and has only a single argument called `workers`.  For best performance, set `workers` to be less than the number of cores that you have available on your machine.  By default it is set as `workers = NULL` and in this case, the system will set `workers` to be the same as `SLURM_CPUS_PER_TASK` on HPC systems running Slurm or to `workers = parallel::detectCores() - 1` on local machines.

## Install (GitHub)

```r
# install.packages("remotes")
remotes::install_github("dpagendam/MCBadgeR")
```
