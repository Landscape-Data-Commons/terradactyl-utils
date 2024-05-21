#' Fetch data for a single table from the postgres database
#' @rdname fetch_postgres
#' @export fetch_postgres
fetch_postgres <- 
  function(tbl,
           projectkey,
           user,
           password,
           schema = "dimadev",
           host="128.123.177.184", # changed 5/2024
           port=5435, # changed 5/2024
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
           host="128.123.177.184",
           port=5435,
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
           host="128.123.177.184",
           port=5435,
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

#' Delete data from the postgres server
#' @rdname delete_postgres
#' @export delete_postgres
delete_postgres <- 
  function(key,
           user,
           password,
           by = "projectkey",
           schema = "dimadev",
           host="128.123.177.184", # changed 5/2024
           port=5435, # changed 5/2024
           dbname = "postgres"
  ){
    
    user_input <- readline("Are you sure you want to run this? (y/n)  ")
    if(user_input != 'y') stop('Better safe than sorry')
    
    con <- DBI::dbConnect(RPostgres::Postgres(), 
                          dbname = dbname, 
                          host=host, 
                          port=port, 
                          user=user, 
                          password=password)
    
    if(by == "projectkey") {
      projectkey <- key
      query1 <- paste0('SELECT * FROM "', schema, '"."tblPlots" WHERE "ProjectKey" = ', "'", projectkey, "'")
      tblPlots <- DBI::dbGetQuery(con, query1)
      dbkeys = unique(tblPlots$DBKey)
    } else if (by == "dbkey"){
      dbkeys <- key
    } else {
      stop("'by' must be either projectkey or dbkey")
    }
    
    l.tables <- list(#"tblDKDetail", "tblDKHeader", 
      # "tblESDRockFragments", 
      "tblGapHeader", "tblGapDetail",
      "tblLPIDetail", "tblLPIHeader", "tblLines", #"tblPlantDenDetail", "tblPlantDenHeader",
      # "tblPlantDenQuads", "tblPlantDenSpecies", 
      #"tblPlotHistory", 
      # "tblPlotNotes",
      "tblPlots", "tblSites", 
      # "tblSpecRichDetail", "tblSpecRichHeader", 
      "tblSpecies", "tblSpeciesGeneric",
      "tblSoilStabHeader", "tblSoilStabDetail")
    
    ### to do: check to make sure these tables exist first, else it will potentially error out the function
    ### there are different tables present in public vs dimadev
    
    msg <- sapply(l.tables, function(tbl){
      delquery <- paste0('DELETE FROM "', schema, '"."', tbl, '" WHERE "DBKey" IN ', paste0("('", paste0(dbkeys,  collapse = "', '"), "')"))
      ndel <- DBI::dbExecute(con, delquery)
      print(paste0("Deleted ", ndel, " rows from ", schema, ".", tbl))  
    })
    
    if(by == "projectkey"){
      query3 <- paste0('DELETE FROM "', schema, '"."Projects" WHERE "project_key" = ', "'", projectkey, "'")
      ndel3 <- DBI::dbExecute(con, query3)
      msg3 <- print(paste0("Deleted ", ndel3, " rows from ", schema, ".Projects"))
      
      msg <- c(msg, msg3)
    }
    
    return(msg)
  }

### Using DBKey in delete_postgres is not the most elegant. This is how I do it:
# p <- fetch_projectkeys("dimadev", user = "", password = "")
# for(k in p){
#   delete_postgres(key = k,
#                   by = "projectkey",
#                   user = "",
#                   password = "",
#                   schema = "dimadev")
# }
## To do: find a way to override the user check that doesn't undermine the whole point of the safety check

