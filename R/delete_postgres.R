#' Delete data from the postgres database based on a projectkey
#' 
#' @rdname fetch_postgres
#' @export fetch_postgres

delete_postgres <- 
  function(projectkey,
           schema = "dimadev",
           host = "jornada-ldc2.jrn.nmsu.edu",
           port = 5432,
           dbname = "postgres",
           user = "dima_get",
           password = "dima@1912!"){
    
    con <- DBI::dbConnect(RPostgres::Postgres(), 
                          dbname = dbname, 
                          host=host, 
                          port=port, 
                          user=user, 
                          password=password)
    
    query1 <- paste0('SELECT * FROM "', schema, '"."tblPlots" WHERE "ProjectKey" = ', "'", projectkey, "'")
    tblPlots <- DBI::dbGetQuery(con, query1)
    dbkeys = unique(tblPlots$DBKey)
    
    l.tables <- list(#"tblDKDetail", "tblDKHeader", 
                     # "tblESDRockFragments", 
                     "tblGapHeader", "tblGapDetail",
                     "tblLPIDetail", "tblLPIHeader", "tblLines", "tblPlantDenDetail", "tblPlantDenHeader",
                     "tblPlantDenQuads", "tblPlantDenSpecies", "tblPlotHistory", "tblPlotNotes",
                     "tblPlots", "tblSites", "tblSpecRichDetail", "tblSpecRichHeader", "tblSpecies", "tblSpeciesGeneric")
    
    ### to do: check to make sure these tables exist first, else it will potentially error out the function
    ### there are different tables present in public vs dimadev
    
    msg <- sapply(l.tables, function(tbl){
      delquery <- paste0('DELETE FROM "', schema, '"."', tbl, '" WHERE "DBKey" IN ', paste0("('", paste0(dbkeys,  collapse = "', '"), "')"))
      ndel <- DBI::dbExecute(con, delquery)
      print(paste0("Deleted ", ndel, " rows from ", schema, ".", tbl))  
    })
    
    query3 <- paste0('DELETE FROM "', schema, '"."Projects" WHERE "project_key" = ', "'", projectkey, "'")
    ndel3 <- DBI::dbExecute(con, query3)
    msg3 <- print(paste0("Deleted ", ndel3, " rows from ", schema, ".Projects"))
    
    msg <- c(msg, msg3)
    
    return(msg)
  }
