#' Check for duplicate rows
#' @rdname ingest_qc
#' @export check_duplicates

check_duplicates <- function(tbl){
  tbl2 <- tbl %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  
  duplicaterows <- tbl2[duplicated(tbl2) | duplicated(tbl2, fromLast = T),] # update to use utils batch_unique function
  duplicatepkeys <- unique(duplicaterows$PrimaryKey) 
  if(nrow(duplicaterows) > 0) {
    warning("Duplicated rows!")
    out <- subset(tbl, PrimaryKey %in% duplicatepkeys) %>% dplyr::arrange(PrimaryKey)
    print(out)
  } else {
    print("No duplicate rows found")
    out <- NULL
  }
  
  return(out)
}

#' Check for missing primary keys
#' @rdname ingest_qc
#' @export find_missing_pkeys
find_missing_pkeys <- function(tbl, pkeys){
  if(nrow(tbl) == 0){
    print("No rows in table")
  } else{
    badpkeys <- pkeys[!(pkeys %in% tbl$PrimaryKey)]  
    if(length(badpkeys) > 0){
      warning("A PrimaryKey is not in this table")
      print(dbpkeytable %>% dplyr::filter(PrimaryKey %in% badpkeys))  
    } else {
      print("All PrimaryKeys are represented")
    }
  }
}

#' Check for missing coordinates
#' @rdname ingest_qc
#' @export check_coordinates
check_coordinates <- function(tblPlots){
  print(paste0("Max Longitude: ", max(tblPlots$Longitude)))
  print(paste0("Min Longitude: ", min(tblPlots$Longitude)))
  print(paste0("Max Latitude: ", max(tblPlots$Latitude)))
  print(paste0("Min Latitude: ", min(tblPlots$Latitude)))
  print(paste0("NA Longitude: ", sum(is.na(tblPlots$Longitude))))
  print(paste0("NA Latitude: ", sum(is.na(tblPlots$Latitude))))
}

#' Compare dataset to server data Inner function: compares a specific table to the LDC either cached or downloaded
#' @rdname compare_data
#' @export compare_table_to_public
compare_table_to_public <- function(projectkey, path_foringest, tablename, path_cache = NULL, path_out = NULL, testmode = FALSE){
  
  message("\n Comparing ", tablename)
  
  # Output will be concatenated to this, adding new lines
  prose <- paste0("LDC Ingest QC \n", "ProjectKey: ", projectkey, "\n QC Date: ", Sys.Date(), "\n table name: ", tablename, "\n Cached data: ")
  if(is.null(path_cache)) {
    prose <- paste0(prose, "FALSE \n\n")
  } else {
    prose <- paste0(prose, "TRUE \n\n")
  }
  
  # Get the new data path
  path_intable <- list.files(path_foringest, pattern = tablename, full.names = T)
  
  # Check path 
  if(length(path_intable) > 1){ # if this is the case, something is either misfiled or accidentally duplicated. Resolve before you continue.
    msg <- paste0("More than one table found in input folder: \n", paste(path_intable, collapse = "\n")) 
    stop(msg)
  } else if (length(path_intable) == 0) { # if this is the case, a method is missing and that is maybe fine and expected
    msg <- paste0("Could not find ", tablename, ". Files in provided directory:\n",
                  paste(list.files(path = path_foringest, full.names = T), collapse ="\n"))
    message(msg)
    
    prose <- paste0(prose, msg, "\n\n")
    
    return("Data not found")
  }
  
  # get new and old data ####
  message(paste("Reading", path_intable))
  if(grepl("csv$", path_intable)){
    newdata <- read.csv(path_intable)
  } else if (grepl("rdata$", path_intable)){
    newdata <- readRDS(path_intable)
  }
  
  if(is.null(path_cache)){
    message(paste("Downloading", tablename, "from LDC"))
    olddata <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = tablename, verbose = T)
    
    accessdate <- Sys.Date() # This is used to keep track of database version
    
  } else {
    
    l_olddatapaths <- sort(list.files(path = path_cache, pattern = tablename, full.names = TRUE))
    if(length(l_olddatapaths) > 1){
      msg <- paste("Multiple data files found:", l_olddatapaths, sep = "\n")
      message(msg)
      prose <- paste0(prose, msg, "\n\n")
    } else if(length(l_olddatapaths) == 0){
      msg <- paste(tablename, "not found in", path_cache)
      message(msg)
      prose <- paste0(prose, msg,)
      return(paste(tablename, "not found"))
    }
    
    if(grepl("csv$", l_olddatapaths[length(l_olddatapaths)])){
      olddata <- read.csv(l_olddatapaths[length(l_olddatapaths)])
    } else if (grepl("rdata$", l_olddatapaths[length(l_olddatapaths)])){
      olddata <- readRDS(l_olddatapaths[length(l_olddatapaths)])
    }
    
    accessdate <- unlist(basename(l_olddatapaths[length(l_olddatapaths)]) |>
                           tools::file_path_sans_ext() |>
                           strsplit(split = "_"))
    accessdate <- accessdate[length(accessdate)]
    
    olddata <- subset(olddata, ProjectKey == projectkey)
  }
  
  # Save this for record keeping
  timepass <- paste0(accessdate, "_changeto_", Sys.Date())
  
  ### Check 1: have columns changed ####
  newcols  <- colnames(newdata)[!(colnames(newdata) %in% colnames(olddata))]
  lostcols <- colnames(olddata)[!(colnames(olddata) %in% colnames(newdata))]
  
  # Some columns aren't supposed to be in the new data at this stage, they get added later
  cols_supposed_to_be_missing <- c("rid", "DateModified", 
                                   # next 3 lines all in geoindicators
                                   "horizontal_flux_total_MD", "vertical_flux_MD", "PM2_5_MD",  "PM10_MD", 
                                   "mlra_name", "mlrarsym", "modis_landcover", 
                                   "na_l1name", "na_l2name", "State", "us_l3name", "us_l4name")
  lostcols <- lostcols[!(lostcols %in% cols_supposed_to_be_missing)]
  
  # Output messages
  if(length(newcols) > 0){
    msg <- paste0(length(newcols), " columns in new data but not old \n", paste(newcols, collapse = "\n"))
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
  }
  if(length(lostcols) > 0){
    msg <- paste0(length(lostcols), " columns in old data but not new \n", paste(lostcols, collapse = "\n"))
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
    
  }
  if(length(lostcols) == 0  & length(newcols) == 0) {
    msg <- "Columns as expected"
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
  }
  ## Output 1&2: new and lost columns
  out_newcols <- newcols
  out_lostcols <- lostcols
  
  # Trim data down to shared columns for comparison
  sharedcols <- intersect(colnames(newdata), colnames(olddata))
  
  newdata_sharedcols <- newdata[, sharedcols]
  olddata_sharedcols <- olddata[, sharedcols]
  
  # Convert everything to character because field types get annoying
  newdata_sharedcols <- as.data.frame(lapply(newdata_sharedcols, as.character))
  olddata_sharedcols <- as.data.frame(lapply(olddata_sharedcols, as.character))
  
  # Find keys (assume it is every column with "key" in the name)
  keyvars <- sharedcols[grepl("Key$", sharedcols)]
  
  # But exclude DBKey in case of data found in >1 DIMA
  keyvars <- keyvars[keyvars != "DBKey"]
  
  # And add these ones in for special cases
  if("SeqNo" %in% sharedcols){
    keyvars <- c(keyvars, "SeqNo")
  }
  if("PointNbr" %in% sharedcols){
    keyvars <- c(keyvars, "PointNbr")
  }
  if("PointLoc" %in% sharedcols){
    keyvars <- c(keyvars, "PointLoc")
  }
  
  
  ## Check 2:  Find entirely new plots and plots in old data ####
  pk_both <- unique(intersect(newdata_sharedcols$PrimaryKey, olddata_sharedcols$PrimaryKey))
  pk_lost <- unique(olddata_sharedcols$PrimaryKey[!(olddata_sharedcols$PrimaryKey %in% pk_both)])
  pk_new <- unique(newdata_sharedcols$PrimaryKey[!(newdata_sharedcols$PrimaryKey %in% pk_both)])
  
  out_newkeys <- pk_new
  out_lostkeys <- pk_lost
  out_sharedkeys <- pk_both
  out_allkeys <- unique(c(olddata_sharedcols$PrimaryKey, newdata_sharedcols$PrimaryKey))
  
  if(length(pk_new) == 0 & length(pk_lost) == 0){
    msg <- (paste("No new PrimaryKeys,", length(pk_both), "matching unique keys found in both data sets"))
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
  } else {
    
    msg <- paste0(length(unique(newdata_sharedcols$PrimaryKey)), " unique PrimaryKeys in new data \n",
                  length(unique(olddata_sharedcols$PrimaryKey)), " unique PrimaryKeys in old data \n",
                  length(pk_both), " unique PrimaryKeys in both datasets \n",
                  length(pk_lost), " unique PrimaryKeys on server but not in new data \n",
                  length(pk_new), " unique PrimaryKeys in new data but not on server")
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
  }
  
  ## Check 3: Find changed data
  olddata_sharedkeys <- dplyr::filter(olddata_sharedcols |> dplyr::arrange_at(keyvars), PrimaryKey %in% pk_both) |> dplyr::select(-DateLoadedInDb)
  newdata_sharedkeys <- dplyr::filter(newdata_sharedcols |> dplyr::arrange_at(keyvars), PrimaryKey %in% pk_both)
  newdata_sharedkeys <- newdata_sharedkeys[,colnames(olddata_sharedkeys)]
  
  if(testmode){
    print("Test mode enabled")
    looplength <- 200
  } else {
    looplength <- nrow(newdata_sharedkeys)
  }
  
  # some data cleaning before the comparison
  newdata_tocompare <- newdata_sharedkeys
  olddata_tocompare <- olddata_sharedkeys
  
  # Logicals don't like the data transfer very much
  newdata_tocompare[newdata_tocompare == "TRUE"] <- 1
  newdata_tocompare[newdata_tocompare == "FALSE"] <- 0  
  olddata_tocompare[olddata_tocompare == "TRUE"] <- 1
  olddata_tocompare[olddata_tocompare == "FALSE"] <- 0
  
  # Send the dates back to being dates for comparison (defined as columns with Date in the name)
  datecols <- colnames(newdata_tocompare)[grepl("date", colnames(newdata_tocompare), ignore.case = TRUE)]
  newdata_tocompare <- newdata_tocompare |> dplyr::mutate_at(datecols, as.Date)
  olddata_tocompare <- olddata_tocompare |> dplyr::mutate_at(datecols, as.Date)
  
  all_mismatch <- lapply(1:looplength, function(r) {
    if(testmode) print(r)
    newrow <- newdata_tocompare[r,colnames(newdata_tocompare)[!(colnames(newdata_tocompare) %in% cols_supposed_to_be_missing)]]
    oldrow <- olddata_tocompare[r,colnames(olddata_tocompare)[!(colnames(olddata_tocompare) %in% cols_supposed_to_be_missing)]]
    
    v_exactmatch <- newrow == oldrow
    v_exactmatch[(is.na(newrow) & is.na(oldrow))] <- TRUE
    v_exactmatch[is.na(v_exactmatch)] <- FALSE
    
    mismatchcols <- colnames(oldrow)[!v_exactmatch]
    
    # Output the mismatched rows if found
    if(length(mismatchcols) > 1){
      oldrow$refdata <- TRUE
      newrow$refdata <- FALSE
      
      oldmismatch <- oldrow[,unique(c(keyvars, mismatchcols, "refdata"))]
      newmismatch <- newrow[,unique(c(keyvars, mismatchcols, "refdata"))]
      
      mismatch <- rbind(newmismatch, oldmismatch) |>
        dplyr::arrange_at(keyvars)
      
      mismatch$changecols <- paste(mismatchcols, collapse = ", ")
      
      return(mismatch)
    } else {
      # or else return an empty data frame so the wrapper works
      return(data.frame())
    }
    
  })
  
  out_mismatch <- dplyr::bind_rows(all_mismatch)
  
  if(nrow(out_mismatch) == 0){
    msg <- "Existing data has not changed"
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
    
  } else {
    msg <- paste(nrow(out_mismatch) / 2, "of", nrow(olddata), "existing rows have changed")
    message(msg)
    prose <- paste0(prose, msg, "\n\n")
    
    if(!is.null(path_out)) {
      outname <- file.path(path_out, paste("ChangedData", projectkey, tablename, timepass, sep = "_"))
      write.csv(out_mismatch, paste0(outname, ".csv"))}
  }
  
  # Get number of hits of each key, for use in the later overall summary
  out_keysummary <- as.data.frame(t(sapply(out_allkeys, function(k) {
    newhits <- sum(newdata$PrimaryKey == k)
    oldhits <- sum(olddata$PrimaryKey == k)
    return(data.frame("PrimaryKey" = k,
                      "table" = tablename,
                      "n_newdatarows" = newhits,
                      "n_olddatarows" = oldhits,
                      "delta_rows" = newhits - oldhits
    ))
  })))
  
  # Output
  out <- list(prose,
              out_allkeys,
              out_sharedkeys,
              out_lostkeys,
              out_newkeys,
              out_lostcols,
              out_newcols,
              out_mismatch,
              out_keysummary)
  names(out) <-
    c("OverallSummary", "AllKeys", "SharedKeys", "LostKeys", "NewKeys", "LostCols", "NewCols", "MismatchedData", "KeySummary")
  
  return(out)
}

#' Compare dataset to server data Wrapper function: run compare_table_to_public over all tables in a project key
#' @rdname compare_data
#' @export compare_dataset_to_public
compare_dataset_to_public <- function(projectkey, path_foringest, path_cache = NULL, path_out = NULL, testmode = FALSE){
  
  v_tablenames <- c("dataGap", 
                    "dataHeader", 
                    "dataHeight", 
                    "dataHorizontalFlux", 
                    "dataLPI", 
                    "geoIndicators", 
                    "geoSpecies", 
                    "dataSoilStability", 
                    "dataSpeciesInventory")
  
  l_changesummary <- sapply(v_tablenames, 
                            compare_table_to_public, 
                            projectkey = projectkey, 
                            path_foringest = path_foringest, 
                            path_cache = path_cache, 
                            path_out = path_out,
                            testmode = testmode)
  
  # Get an overall summary of which keys are found where
  l_keysummary <- sapply(l_changesummary, function(m) m["KeySummary"])
  l_keysummary <- l_keysummary[!is.na(l_keysummary)]
  keysummary <- do.call(rbind, l_keysummary)
  row.names(keysummary) <- NULL
  
  # Pivot wider (for once, make tall data wide. As a treat.)
  overall_keysummary <- tidyr::pivot_wider(keysummary, 
                                           id_cols = PrimaryKey, 
                                           names_from = table,
                                           values_from = c(delta_rows, n_newdatarows, n_olddatarows),
                                           names_sort = FALSE) |> as.data.frame() |> list()
  
  output_robj <- c(overall_keysummary, l_changesummary)
  names(output_robj)[1] <- "KeyCountChangeSummary"
  
  # get all the prose together
  l_prose <- sapply(l_changesummary, function(m) m["OverallSummary"])
  outprose <- as.character(paste(l_prose[!is.na(l_prose)], collapse = "\n\n"))
  
  output_robj <- c(outprose, output_robj)
  names(output_robj)[1] <- "OverallSummary"
  
  # Save output
  if(is.null(path_out)){
    message("Report will not be saved")
    return(output_robj)
  } else {
    outname <- file.path(path_out, paste0("DataComparisonSummary_", projectkey, "_", Sys.Date(), ".rdata"))
    saveRDS(output_robj, outname)
    
    outname2 <- file.path(path_out, paste0("DataComparisonSummary_PROSE_", projectkey, "_", Sys.Date(), ".txt"))
    write.table(outprose, outname2, row.names = FALSE, col.names = FALSE)
    return(output_robj)
  }
}

