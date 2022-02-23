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
#' @export fetch_api_single
fetch_api_single <- function(api, endpoint, values = NULL, verbose = T){
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

## wrapper, automatically detecting if multiple tables are requested
#' @export fetch_api
#' @rdname fetch_api
fetch_api <- function(endpoint, values=NULL, api = "dima", verbose = T) {
  api <- tolower(api)
  if(class(endpoint) == "list"){
    print(paste("Fetching", length(endpoint), "tables"))
    out <- lapply(endpoint, fetch_api_single, values = values, api = api, verbose = verbose)
    names(out) <- endpoint
  } else {
    out <- fetch_api_single(endpoint = endpoint, values = values, api = api, verbose = verbose)
  }
  
  return(out)
}

## Fetch data from API for a single table 
#' @rdname fetch_api
#' @export fetch_ldc_single
fetch_ldc_single <- function(endpoint, values = NULL, verbose = T){
  values <- gsub(" ", "%20", values)
  
  if(is.null(values)){
    url <- paste0("https://api.landscapedatacommons.org/api/",endpoint)
  } else {
    url <- paste0("https://api.landscapedatacommons.org/api/",endpoint,"?",values)
  }
  if(verbose) print(paste("Accessing", url))
  get_url <- httr::GET(url) 
  flat_get <- httr::content(get_url, "text", encoding = "UTF-8")
  jsonize <- jsonlite::fromJSON(flat_get, flatten = TRUE)
  df <- as.data.frame(jsonize)
  return(df)
}

## wrapper, automatically detecting if multiple tables are requested
#' @export fetch_ldc
#' @rdname fetch_api
fetch_ldc <- function(endpoint, values=NULL, verbose = T) {
  if(class(endpoint) == "list"){
    print(paste("Fetching", length(endpoint), "tables"))
    out <- lapply(endpoint, fetch_ldc_single, values = values, verbose = verbose)
    names(out) <- endpoint
  } else {
    out <- fetch_ldc_single(endpoint = endpoint, values = values, verbose = verbose)
  }
  return(out)
}
