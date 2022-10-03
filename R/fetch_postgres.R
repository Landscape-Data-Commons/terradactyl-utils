#' Fetch data for a single table from the postgres database
#' @rdname fetch_postgres
#' @export fetch_postgres
fetch_postgres <- 
  function(tbl,
           projectkey,
           schema = "dimadev",
           host="jornada-ldc2.jrn.nmsu.edu",
           port=5432,
           dbname="postgres",
           user="dima_get",
           password="dima@1912!"){
    
    con <- DBI::dbConnect(RPostgres::Postgres(), 
                          dbname = dbname, 
                          host=host, 
                          port=port, 
                          user=user, 
                          password=password)
    
    # this can be done with some more complex SQL joining, but this shortcut works and is simpler code (to me, anyway)
    query1 <- paste0('SELECT * FROM "', schema, '"."tblPlots" WHERE "ProjectKey" = ', "'", projectkey, "'")
    tblPlots <- DBI::dbGetQuery(con, query1)
    dbkeys = unique(tblPlots$DBKey)
    
    query2 <- paste0('SELECT * FROM "', tbl, '" WHERE "DBKey" IN ', paste0("('", paste0(dbkeys,  collapse = "', '"), "')"))
    out <- DBI::dbGetQuery(con, query2)
    
    
    return(out)
  }
