#' Fetch data for a single table from the postgres database
#' @rdname fetch_postgres
#' @export fetch_postgres
fetch_postgres <- 
  function(tbl,
           schema = "dimadev",
           host="jornada-ldc2.jrn.nmsu.edu",
           port=5432,
           dbname="postgres",
           user="dima_get",
           password="dima@1912!"){
    
    require(DBI)
    require(dbplyr)
    require(dplyr)
    require(RPostgres)
    require(tidyverse)
    
    con <- dbConnect(RPostgres::Postgres(), 
                     dbname = dbname, 
                     host=host, 
                     port=port, 
                     user=user, 
                     password=password)
    
    out <- tbl(con, in_schema(schema, tbl)) %>% as_tibble()
    return(out)
  }
