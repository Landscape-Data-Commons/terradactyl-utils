#' Extract a table from a DIMA
#'
#' @description Given path, a filename, and a query, extract a table from a DIMA

#' @rdname extract_table
#' @export extract_table
extract_table <- function(dima.path, query){
  
  if (is.null(names(query)) | length(names(query)[!grepl(x = names(query), pattern = "^$")]) != length(names(query))) {
    warning("The query vector must have a name for each value, even if there is only one. Using the query text as names, name the input parameter for more concise naming")
    names(query) <- query
  }
  if (!grepl(x = dima.path, pattern = "\\.(MDB)|(mdb)|(accdb)|(ACCDB)$")) {
    stop("Valid file extension required for the argument dima.")
  }
  
  ## Using odbc
  # dima.connection <- odbc::dbConnect(drv = "Microsoft Access Driver", dsn = paste(data.path, dima, sep = "/"))
  # data.current <- lapply(query, FUN = DBI::dbGetQuery, conn = dima.connection)
  # odbc::dbDisconnect(dima.connection)
  
  ## Use the appropriate function from RODBC:: based on 32- versus 64-bit installs of R
  arch <- R.Version()$arch
  if(arch == "x86_64") {
    dima.channel <- RODBC::odbcConnectAccess2007(dima.path)
  }
  if(arch == "i386") {
    stop("32-bit R is no longer supported.")
  }
  
  ## Apply the SQL queries to the DIMA
  data.current <- lapply(query, FUN = RODBC::sqlQuery, channel = dima.channel, stringsAsFactors = FALSE)
  RODBC::odbcClose(channel = dima.channel)
  return(data.current)
}
