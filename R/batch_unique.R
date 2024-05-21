#' A faster way of applying 'unique' to a large dataset.
#' @rdname better_unique
#' @export better_unique
better_unique <- function(indata){
  
  indata <- as.data.frame(indata) # discard geometry
  
  # For runspeed, drop columns that are all identical
  vec_varied_cols <- vapply(indata, function(x) length(unique(x)) > 1, logical(1L))
  
  if("PrimaryKey" %in% colnames(indata)){
    vec_varied_cols["PrimaryKey"] <- TRUE # Needed if only one primary key is in the input data
  }
  
  data_varied_cols_only <- indata[,vec_varied_cols]
  
  vec_duplicated <- duplicated(data_varied_cols_only)
  outdata <- indata[!vec_duplicated,]
  
  return(outdata)
}

# A faster way of applying 'unique' to a large dataset. Specify a variable to split data by, each subset of the full data will be examined for uniqueness.
# @rdname batch_unique
# @export batch_unique
# batch_unique <- function(indata, splitby){
#   
#   indata <- as.data.frame(indata) # discard geometry
#   
#   indata[is.na(indata[,splitby]),splitby] <- "NAPlaceholder"
#   
#   v_split <- unique(indata[,splitby])
#   l_indata <- lapply(v_split, function(s){
#     indata[indata[,splitby] == s,]
#   })
#   l_outdata <- lapply(l_indata, unique)
#   outdata <- do.call(rbind, l_outdata)
#   
#   outdata[outdata[,splitby] == "NAPlaceholder", splitby] <- NA
#   
#   return(outdata)
# }