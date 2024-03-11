#' A faster way of applying 'unique' to a large dataset. Specify a variable to split data by, each subset of the full data will be examined for uniqueness.
#' @rdname batch_unique
#' @export batch_unique
batch_unique <- function(indata, splitby){
  
  indata <- as.data.frame(indata) # discard geometry
  
  indata[is.na(indata[,splitby]),splitby] <- "NAPlaceholder"
  
  v_split <- unique(indata[,splitby])
  l_indata <- lapply(v_split, function(s){
    indata[indata[,splitby] == s,]
  })
  l_outdata <- lapply(l_indata, unique)
  outdata <- do.call(rbind, l_outdata)
  
  outdata[outdata[,splitby] == "NAPlaceholder", splitby] <- NA
  
  return(outdata)
}
