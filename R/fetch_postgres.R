#' Fetch data for a single table from the postgres database
#' @rdname fetch_postgres
#' @export fetch_postgres
fetch_postgres <- 
  function(tbl,
           projectkey,
           user,
           password,
           schema = "dimadev",
           host="jornada-ldc2.jrn.nmsu.edu",
           port=5432,
           dbname="postgres"
  ){
    
    con <- DBI::dbConnect(RPostgres::Postgres(), 
                          dbname = dbname, 
                          host=host, 
                          port=port, 
                          user=user, 
                          password=password)
    
    query1 <- paste0('SELECT * FROM "', schema, '"."tblPlots" WHERE "ProjectKey" = ', "'", projectkey, "'")
    tblPlots <- DBI::dbGetQuery(con, query1)
    dbkeys = unique(tblPlots$DBKey)
    
    query2 <- paste0('SELECT * FROM "',schema, '"."', tbl, '" WHERE "DBKey" IN ', paste0("('", paste0(dbkeys,  collapse = "', '"), "')"))
    out <- DBI::dbGetQuery(con, query2)
    
    return(out)
  }

#' @rdname fetch_postgres
#' @export fetch_projectkeys
fetch_projectkeys <-
  function(schema,
           host="jornada-ldc2.jrn.nmsu.edu",
           port=5432,
           dbname="postgres",
           user,
           password){
    
    con <- DBI::dbConnect(RPostgres::Postgres(), 
                          dbname = dbname, 
                          host=host, 
                          port=port, 
                          user=user, 
                          password=password)
    
    query1 <- paste0('SELECT "project_key" FROM "', schema, '"."Projects"')
    ProjectKeys <- DBI::dbGetQuery(con, query1) %>% unique() %>% dplyr::pull(project_key)
    
    return(ProjectKeys)
  }

#' @rdname fetch_postgres
#' @export fetch_DBKeys
fetch_DBKeys <-
  function(schema,
           host="jornada-ldc2.jrn.nmsu.edu",
           port=5432,
           dbname="postgres",
           user,
           password){
    
    con <- DBI::dbConnect(RPostgres::Postgres(), 
                          dbname = dbname, 
                          host=host, 
                          port=port, 
                          user=user, 
                          password=password)
    
    query2 <- paste0('SELECT "DBKey" FROM "', schema, '"."tblLPIDetail"')
    query3 <- paste0('SELECT "DBKey" FROM "', schema, '"."tblLPIHeader"')
    query4 <- paste0('SELECT "DBKey" FROM "', schema, '"."tblGapDetail"')
    query5 <- paste0('SELECT "DBKey" FROM "', schema, '"."tblGapHeader"')
    query6 <- paste0('SELECT "DBKey" FROM "', schema, '"."tblLines"')
    
    DBKeys <- 
      unique(c(
        unlist(DBI::dbGetQuery(con, query2)),
        unlist(DBI::dbGetQuery(con, query3)),
        unlist(DBI::dbGetQuery(con, query4)),
        unlist(DBI::dbGetQuery(con, query5)),
        unlist(DBI::dbGetQuery(con, query6))
        ))
        
        return(DBKeys)
  }
