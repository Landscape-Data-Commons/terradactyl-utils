#' Extract a table from a DIMA
#'
#' @description Given path, a filename, and a query, extract a table from a DIMA

#' @rdname extract_table
#' @export extract_table
extract_table <- function(data.path, dima, query){
  if (is.null(names(query)) | length(names(query)[!grepl(x = names(query), pattern = "^$")]) != length(names(query))) {
    stop("The query vector must have a name for each value, even if there is only one.")
  }
  if (!grepl(x = dima, pattern = "\\.(MDB)|(mdb)|(accdb)|(ACCDB)$")) {
    stop("Valid file extension required for the argument dima.")
  }
  if (!(dima %in% list.files(path = data.path, pattern = "\\.(MDB)|(mdb)|(accdb)|(ACCDB)$"))) {
    stop("Unable to find the specified DIMA in the provided data path")
  }
  
  ## Using odbc
  # dima.connection <- odbc::dbConnect(drv = "Microsoft Access Driver", dsn = paste(data.path, dima, sep = "/"))
  # data.current <- lapply(query, FUN = DBI::dbGetQuery, conn = dima.connection)
  # odbc::dbDisconnect(dima.connection)
  
  ## Use the appropriate function from RODBC:: based on 32- versus 64-bit installs of R
  switch(R.Version()$arch,
         "x86_64" = {
           dima.channel <- RODBC::odbcConnectAccess2007(paste(data.path, dima, sep = "/"))
         },
         "i386" = {
           dima.channel <- RODBC::odbcConnectAccess(paste(data.path, dima, sep = "/"))
         })
  ## Apply the SQL queries to the DIMA
  data.current <- lapply(query, FUN = RODBC::sqlQuery, channel = dima.channel, stringsAsFactors = FALSE)
  RODBC::odbcClose(channel = dima.channel)
  return(data.current)
}
