#' Fetch DIMA data from the Landscape Data Commons API 
#'
#' @description Given a table name and optional field query, download DIMA data 
#' from the online Landscape Data Commons API (api.landscapedatacommons.org). 
#' Fetch_api is not intended for public use, but can access both 
#' dima.landscapedatacommons.org and api.landscapedatacommons.org. The function 
#' 'fetch_ldc' can only access api.landscapedatacommons.org, and is 
#' suitable for sharing outside of the LDC group.

#' @param endpoint Character string or list. Name of the requested table e.g. "tblPlots". 
#' Provide a list of character strings to download more than one table. 
#' @param values Optional. JSON query as character string. If a list of endpoints is provided, the filter column must be present in all requested tables.
#' @param verbose If true, print the URL of the requested record.
#' @param api URL prefix specifying the API to access. Use "dima" or "api".

#' @return A data frame containing DIMA data of the requested table, or a list
#' of data frames containing the requested tables. 

#' @examples 
#' indicators_all <- fetch_ldc(endpoint = "geoIndicators")
#' 
#' indicators_NorCal15 <- fetch_ldc(endpoint = "geoIndicators", values = "ProjectName=California NorCal 2015")
#' 
#' data_gap <- fetch_ldc(endpoint = list("dataHeader", "dataGap"))
#' data_gap_NorCal15 <- fetch_ldc(endpoint = list("dataHeader", "dataGap"), values = "ProjectName=California NorCal 2015")

## Fetch data for a single table
#' @rdname fetch_api
#' @export fetch_api
fetch_api <- function(api, endpoint, values = NULL, verbose = T){
  values <- gsub(" ", "%20", values)
  
  api <- tolower(api)
  if(!(api %in% c("dima", "api"))) stop("api parameter not recognized. Use 'api' or 'dima'")

  if(is.null(values)){
    url <- paste0("https://", api, ".landscapedatacommons.org/api/",endpoint)
  } else {
    url <- paste0("https://", api, ".landscapedatacommons.org/api/",endpoint,"?",values)
  }
  if(verbose) print(paste("Accessing", url))
  get_url <- httr::GET(url) 
  flat_get <- httr::content(get_url, "text", encoding = "UTF-8")
  jsonize <- jsonlite::fromJSON(flat_get, flatten = TRUE)
  df <- as.data.frame(jsonize)
  return(df)
}

# ## Fetch data from API for a single table 
# #' @rdname fetch_api
# #' @export fetch_ldc
# fetch_ldc <- function(endpoint, values = NULL, verbose = T){
#   values <- gsub(" ", "%20", values)
#  
#   if(is.null(values)){
#     url <- paste0("https://api.landscapedatacommons.org/api/",endpoint)
#   } else {
#     url <- paste0("https://api.landscapedatacommons.org/api/",endpoint,"?",values)
#   }
#   if(verbose) print(paste("Accessing", url))
#   get_url <- httr::GET(url) 
#   flat_get <- httr::content(get_url, "text", encoding = "UTF-8")
#   jsonize <- jsonlite::fromJSON(flat_get, flatten = TRUE)
#   df <- as.data.frame(jsonize)
#   return(df)
# }

#' Query the Landscape Data Commons
#' @description Fetch data from the Landscape Data commons, querying by ecological site ID, PrimaryKey, or ProjectKey.
#' @param keys Character vector. The character strings containing the values to match in the query/queries.
#' @param key_type Character string. The type of key values being used. This determines which variable in the Landscape Data Commons to filter using the values \code{keys}. Must be \code{"ecosite"} for \code{EcologicalSiteId}, \code{"primarykey"} for \code{PrimaryKey}, or \code{"projectkey"} for \code{ProjectKey}. Defaults to \code{"ecosite"}.
#' @param data_type Character string. Determines which table to download data from. Valid values are \code{"lpi"}, \code{"height"}, \code{"gap"}, \code{"soil"}, \code{"species"}, \code{"speciesinventory"}, and \code{"header"}. Defaults to \code{"lpi"}.
#' @param verbose Logical. If \code{TRUE} then the function will report with diagnostic messages as it runs. Defaults to \code{FALSE}.
#' @export
fetch_ldc <- function(keys,
                      key_type = "ecosite",
                      data_type = "lpi",
                      verbose = FALSE){
  
  if (!(data_type %in% c("lpi", "height", "gap", "soil", "species", "speciesinventory", "header"))) {
    stop("data_type must be 'lpi', 'height', 'gap', 'soil', 'species', 'speciesinventory', or 'header'.")
  }
  current_data_source <- switch(data_type,
                                "lpi" = {"datalpi"},
                                "height" = {"dataheight"},
                                "gap" = {"datagap"},
                                "soil" = {"datasoilstability"},
                                "species" = "geospecies",
                                "speciesinventory" = "dataspeciesinventory",
                                "header" = "dataheader")
  
  if (!(key_type %in% c("ecosite", "primarykey", "projectkey"))) {
    stop("key_type must be 'ecosite', 'primarykey', 'projectkey'.")
  }
  current_key_type <- switch(key_type,
                             "ecosite" = {"EcologicalSiteId"},
                             "primarykey" = {"PrimaryKey"},
                             "projectkey" = {"ProjectKey"})
  
  query_results_list <- lapply(X = keys,
                               current_data_source = current_data_source,
                               current_key_type = current_key_type,
                               FUN = function(X, current_data_source, current_key_type){
                                 # Build the query
                                 query <- paste0("https://api.landscapedatacommons.org/api/",
                                                 current_data_source, "?",
                                                 current_key_type, "=",
                                                 X)
                                 
                                 # Getting the data via curl
                                 # connection <- curl::curl(query)
                                 # results_raw <- readLines(connection)
                                 # results <- jsonlite::fromJSON(results_raw)
                                 if (verbose) {
                                   message("Attempting to query EDIT with:")
                                   message(query)
                                 }
                                 
                                 # Full query results
                                 full_results <- httr::GET(query,
                                                           config = httr::timeout(60))
                                 # Grab only the data portion
                                 results_raw <- full_results[["content"]]
                                 # Convert from raw to character
                                 results_character <- rawToChar(results_raw)
                                 # Convert from character to data frame
                                 results <- jsonlite::fromJSON(results_character)
                                 if (verbose) {
                                   message("Results converted from json to character")
                                 }
                                 
                                 results
                               })
  
  results <- do.call(rbind,
                     query_results_list)
  
  # So we can tell the user later which actually got queried
  if (is.null(results)) {
    warning("No results returned. There are no records in the LDC for the supplied keys.")
    return(results)
  } else {
    if (verbose) {
      message("Determining if keys are missing.")
    }
    queried_keys <- unique(results[[current_key_type]])
    missing_keys <- keys[!(keys %in% queried_keys)]
  }
  
  if (length(missing_keys) > 0) {
    missing_key_warning <- paste0("The following keys did not return data from the LDC: ",
                   paste(missing_keys,
                         collapse = ", "))
    warning(missing_key_warning)
  }
  
  
  # Only keep going if there are results!!!!
  if (length(results) > 0) {
    # Convert from character to numeric variables where possible
    data_corrected <- lapply(X = names(results),
                             data = results,
                             FUN = function(X, data){
                               # Get the current variable values as a vector
                               vector <- data[[X]]
                               # Try to coerce into numeric
                               numeric_vector <- suppressWarnings(as.numeric(vector))
                               # If that works without introducing NAs, return the numeric vector
                               # Otherwise, return the original character vector
                               if (all(!is.na(numeric_vector))) {
                                 return(numeric_vector)
                               } else {
                                 return(vector)
                               }
                             })
    
    # From some reason co.call(cbind, data_corrected) was returning a list not a data frame
    # so I'm resorting to using dplyr
    data <- suppressMessages(dplyr::bind_cols(data_corrected))
    # Correct the names of the variables
    names(data) <- names(results)
    
    # Put it in the workspace list
    return(data)
  } else {
    return(results)
  }
}
