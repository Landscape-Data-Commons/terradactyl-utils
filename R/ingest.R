#' Take data from public postgres server and prepare them for ingestion
#' @rdname ingest
#' @export ingest_DIMA
ingest_DIMA <- function(projectkey, 
                        path_parent,
                        path_specieslist, 
                        path_templatetd,
                        path_schema,
                        pgschema = "public",
                        doLPI = T,
                        doGap = T,
                        doSR = T,
                        doSS = T,
                        doHt = T,
                        doRH = T,
                        doHF = T,
                        doGSP = T,
                        user = NULL,
                        password = NULL){
  
  # Set paths, based on the suffix. Creates file folders if needed
  if(!dir.exists(path_parent)) stop(paste0("Parent ", path_parent, " does not exist"))
  # path_parent <- paste0(path_parent, projectkey)
  
  path_dimatables <- file.path(path_parent, "DIMATables")
  if(!dir.exists(path_dimatables)) dir.create(path_dimatables)
  path_tall <- file.path(path_parent, "Tall")
  if(!dir.exists(path_tall)) dir.create(path_tall)
  path_foringest <- file.path(path_parent, "For Ingest")
  if(!dir.exists(path_foringest)) dir.create(path_foringest)
  
  schema <- read.csv(path_schema)
  
  # next sections are split out by method in order to let you pick and choose which to run. Each follows this pattern:
  ### get data from postgres server
  ### save dima tables to path_dimatables
  ### gather 
  ### drop any duplicate rows (rows where dbkey and dateloadedindb are the only differences)
  ### drop rows without coordinates
  ### write tall
  
  tblPlots <- fetch_postgres("tblPlots", schema = pgschema, projectkey = projectkey, user = user, password = password)
  
  if(nrow(tblPlots) == 0){
    stop("tblPlots has no rows in postgres server")
  }
  
  tblPlots$SpeciesState <- projectkey
  
  pkeys <- dplyr::filter(tblPlots, !is.na(Latitude) & !is.na(Longitude)) %>% dplyr::pull(PrimaryKey)
  
  write.csv(tblPlots, file.path(path_dimatables, "tblPlots.csv"), row.names = F)
  
  if(doGap){
    message("Gathering gap data")
    tblGapHeader <- fetch_postgres("tblGapHeader", schema = pgschema, projectkey = projectkey, user = user, password = password)
    tblGapDetail <- fetch_postgres("tblGapDetail", schema = pgschema, projectkey = projectkey, user = user, password = password)
    write.csv(tblGapHeader, file.path(path_dimatables, "tblGapHeader.csv"), row.names = F)
    write.csv(tblGapDetail, file.path(path_dimatables, "tblGapDetail.csv"), row.names = F)
    
    tall_gap <- gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail) %>% dplyr::filter(PrimaryKey %in% pkeys)
    
    dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
    write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
    
  } else {
    tblGapHeader <- NULL # need to have an object named in order to run gather_header below
  }
  
  if(doLPI){
    message("Gathering LPI data")
    
    tblLPIHeader <- fetch_postgres("tblLPIHeader", schema = pgschema, projectkey = projectkey, user = user, password = password)
    tblLPIDetail <- fetch_postgres("tblLPIDetail", schema = pgschema, projectkey = projectkey, user = user, password = password)
    write.csv(tblLPIHeader, file.path(path_dimatables, "tblLPIHeader.csv"), row.names = F)
    write.csv(tblLPIDetail, file.path(path_dimatables, "tblLPIDetail.csv"), row.names = F)
    
    tall_lpi <- gather_lpi(source = "DIMA", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    
    dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
    write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)
    
  } else {
    tblLPIHeader <- NULL # need to have an object named in order to run gather_header below
  }
  
  if(doHt){
    message("Gathering height data")
    
    if(!doLPI) {
      tblLPIHeader <- fetch_postgres("tblLPIHeader", schema = pgschema, projectkey = projectkey, user = user, password = password)
      tblLPIDetail <- fetch_postgres("tblLPIDetail", schema = pgschema, projectkey = projectkey, user = user, password = password)
      write.csv(tblLPIHeader, file.path(path_dimatables, "tblLPIHeader.csv"), row.names = F)
      write.csv(tblLPIDetail, file.path(path_dimatables, "tblLPIDetail.csv"), row.names = F)
    }
    tall_height <- gather_height(source = "DIMA", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tall_height <- tall_height[which(!duplicated(dropcols_height)),] %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
    write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)
    
  }
  
  if(doSR){
    message("Gathering species inventory data")
    
    tblSpecRichHeader <- fetch_postgres("tblSpecRichHeader", schema = pgschema, projectkey = projectkey, user = user, password = password)
    tblSpecRichDetail <- fetch_postgres("tblSpecRichDetail", schema = pgschema, projectkey = projectkey, user = user, password = password)
    write.csv(tblSpecRichHeader, file.path(path_dimatables, "tblSpecRichHeader.csv"), row.names = F)
    write.csv(tblSpecRichDetail, file.path(path_dimatables, "tblSpecRichDetail.csv"), row.names = F)
    
    tall_speciesinventory <- gather_species_inventory(source = "DIMA", tblSpecRichDetail = tblSpecRichDetail, tblSpecRichHeader = tblSpecRichHeader)
    
    dropcols_speciesinventory <- tall_speciesinventory  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tall_speciesinventory <- tall_speciesinventory[which(!duplicated(dropcols_speciesinventory)),] %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    saveRDS(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.rdata"))
    write.csv(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)
    
  } else {
    tblSpecRichHeader <- NULL # need to have an object named in order to run gather_header below
  }
  
  if(doSS){
    message("Gathering soil stability data")
    
    tblSoilStabHeader <- fetch_postgres("tblSoilStabHeader", schema = pgschema, projectkey = projectkey, user = user, password = password)
    tblSoilStabDetail <- fetch_postgres("tblSoilStabDetail", schema = pgschema, projectkey = projectkey, user = user, password = password)
    write.csv(tblSoilStabHeader, file.path(path_dimatables, "tblSoilStabHeader.csv"), row.names = F)
    write.csv(tblSoilStabDetail, file.path(path_dimatables, "tblSoilStabDetail.csv"), row.names = F)
    
    tall_soilstability <- gather_soil_stability(source = "DIMA", tblSoilStabHeader = tblSoilStabHeader, tblSoilStabDetail = tblSoilStabDetail)
    
    dropcols_soilstability <- tall_soilstability  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tall_soilstability <- tall_soilstability[which(!duplicated(dropcols_soilstability)),] %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    saveRDS(tall_soilstability, file.path(path_tall, "soil_stability_tall.rdata"))
    write.csv(tall_soilstability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)
  }
  
  if(doHF){
    message("Gathering horizontal flux data")
    
    tblHorizontalFlux <- fetch_postgres("tblHorizontalFlux", schema = pgschema, projectkey = projectkey, user = user, password = password)
    # no gather needed here
    
    dropcols_hf <- tblHorizontalFlux  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tblHorizontalFlux <- tblHorizontalFlux[which(!duplicated(dropcols_hf)),]
    
    tblHorizontalFlux <- tblHorizontalFlux %>% 
      dplyr::rename(DateLoadedInDb = DateLoadedInDB) %>%
      dplyr::mutate(ProjectKey = projectkey,
                    DateEstablished = NA) %>%
      dplyr::select(-PlotKey, -Collector, -labTech, -rid) %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    # saveRDS(tblHorizontalFlux, file.path(path_tall, "dataHorizontalFlux.rdata"))
    # write.csv(tblHorizontalFlux, file.path(path_tall, "dataHorizontalFlux.csv"), row.names = F)

  } else {
    tblHorizontalFlux = NULL
  }

  if(doRH){
    tblQualHeader <- fetch_postgres("tblQualHeader", schema = "public", projectkey = projectkey, user = user, password = password)
    tblQualDetail <- fetch_postgres("tblQualDetail", schema = "public", projectkey = projectkey, user = user, password = password)
  
    tall_rangelandhealth <- gather_rangeland_health(source = "DIMA", tblQualHeader = tblQualHeader, tblQualDetail = tblQualDetail)
   
    dropcols_rangelandhealth <- tall_rangelandhealth  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
    tall_rangelandhealth <- tall_rangelandhealth[which(!duplicated(dropcols_rangelandhealth)),] %>%
      dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
    
    saveRDS(tall_rangelandhealth, file.path(path_tall, "rangeland_health_tall.rdata"))
    write.csv(tall_rangelandhealth, file.path(path_tall, "rangeland_health_tall.csv"), row.names = F)
  }
  
  message("Gathering header")
  header <- gather_header(dsn = NULL, source = "DIMA", tblPlots = tblPlots, date_tables = list(tblLPIHeader, tblGapHeader, 
                                                                                              tblSpecRichHeader, tblHorizontalFlux))
  
  dropcols_header <- header %>% dplyr::select(-"DBKey", -"DateLoadedInDb")
  header <- header[which(!duplicated(dropcols_header)),]
  
  write.csv(header, file.path(path_tall, "header.csv"), row.names = F)
  saveRDS(header, file.path(path_tall, "header.rdata"))
  
  # attach date to horizontalflux
  if(doHF){
    tblHorizontalFlux <- tblHorizontalFlux %>%
      dplyr::left_join(header %>% dplyr::select(PrimaryKey, DateVisited)) %>%
      dplyr::mutate(BoxID = as.character(BoxID),
                    StackID = as.character(StackID),
                    # PlotKey = as.character(PlotKey),
                    RecKey = as.character(RecKey))
    write.csv(tblHorizontalFlux, file.path(path_foringest, "dataHorizontalFlux.csv"), row.names = F)
  }
    
  # translate tall
  translate_coremethods(path_tall = path_tall,
                     path_out = path_foringest,
                     path_schema = path_schema,
                     projectkey = projectkey,
                     verbose = T)
  
  if(doLPI) {
    l <- lpi_calc(
      lpi_tall = file.path(path_tall, "lpi_tall.rdata"),
      header = header,
      source = "DIMA",
      species_file = path_specieslist,
      dsn = path_templatetd
    )
  } else {
    l <- NULL
  }
  
  if(doGap){
    g <- gap_calc(
      gap_tall = file.path(path_tall, "gap_tall.rdata"),
      header = header
    )
  } else {
    g <- NULL
  }
  
  if(doHt){
    h <- height_calc(
      height_tall = file.path(path_tall, "height_tall.rdata"),
      header = header,
      source = "DIMA",
      species_file = path_specieslist
    )
  } else {
    h <- NULL
  }
  
  if(doSR){
    sr <- spp_inventory_calc(
      header = header,
      spp_inventory_tall = file.path(path_tall, "species_inventory_tall.rdata"),
      species_file = path_specieslist, 
      source = "DIMA"
    )
  } else {
    sr <- NULL
  }
  
  if(doSS){
    ss <- soil_stability_calc(header = header, 
                              soil_stability_tall = file.path(path_tall, "soil_stability_tall.rdata"))
  } else {
    ss <- NULL
  }
  
  if(doRH){
    rh <- tall_rangelandhealth # There is no indicator calculation for RH, just a gather; I structured it like this to preserve the symmetry.
  } else {
    rh <- NULL
  }
  
  all_indicators <- header
  if(doLPI) all_indicators <- all_indicators %>% dplyr::left_join(., l)
  if(doGap) all_indicators <- all_indicators %>% dplyr::left_join(., g) 
  if(doHt) all_indicators <- all_indicators %>% dplyr::left_join(., h)  
  if(doSR) all_indicators <- all_indicators %>% dplyr::left_join(., sr)
  if(doSS) all_indicators <- all_indicators %>% dplyr::left_join(., ss)
  if(doRH) all_indicators <- all_indicators %>% dplyr::left_join(., rh)
  
  all_indicators_dropcols <- all_indicators %>% 
    dplyr::select_if(!names(.) %in% c("DBKey", "DateLoadedInDb", "rid", "SpeciesList"))
  all_indicators_unique <- all_indicators[which(!duplicated(all_indicators_dropcols)),]
  
  i <- add_indicator_columns(template = path_templatetd,
                                               source = "DIMA",
                                               all_indicators = all_indicators_unique,
                                               prefixes_to_zero = c("AH", "FH", "NumSpp"))
  geoInd <- i %>% 
    translate_schema(schema = schema,
                     projectkey = projectkey,
                     datatype = "geoIndicators",
                     dropcols = TRUE,
                     verbose = TRUE)
  
  write.csv(geoInd, file = file.path(path_foringest, "geoIndicators.csv"), row.names = F)
  
  if(doGSP){
    a <- accumulated_species(
      lpi_tall = 
        if(file.exists(file.path(path_tall, "lpi_tall.rdata"))){
          file.path(path_tall, "lpi_tall.rdata")
        } else {
          NULL
        },
      height_tall = 
        if(file.exists(file.path(path_tall, "height_tall.rdata"))){
          file.path(path_tall, "height_tall.rdata")
        } else {
          NULL
        },
      spp_inventory_tall = 
        if(file.exists(file.path(path_tall, "species_inventory_tall.rdata"))){
          file.path(path_tall, "species_inventory_tall.rdata")
        } else {
          NULL
        },
      header = file.path(path_tall, "header.rdata"),
      species_file = path_specieslist,
      dead = F,
      source = "DIMA") %>% 
      dplyr::left_join(header %>% dplyr::select(PrimaryKey, DateVisited)) %>% 
      dplyr::filter(!(is.na(AH_SpeciesCover) & is.na(AH_SpeciesCover_n) & 
                        is.na(Hgt_Species_Avg) & is.na(Hgt_Species_Avg_n))) %>%
      translate_schema(schema = schema,
                       projectkey = projectkey,
                       datatype = "geoSpecies",
                       dropcols = TRUE,
                       verbose = TRUE)
    
    write.csv(a, file.path(path_foringest, "geoSpecies.csv"), row.names = F)
    
  }

  return(geoInd)
}




#' @rdname ingest
#' @export new_data_only
new_data_only <- function(projectkey, path_parent, path_cache) {

  if(!dir.exists(path_parent)){
    stop(paste0(path_parent, " does not exist"))
  }
    
  path_foringest <- file.path(path_parent, "For Ingest")
  if(!dir.exists(path_foringest)) {
    message("Creating ", path_foringest)
    dir.create(path_foringest)
  }
  
  paths_indata <- list.files(
    path = path_foringest,
    pattern = ".csv",
    recursive = F,
  )
  
  paths_cache <-  list.files(
    path = path_cache,
    pattern = ".rdata",
    recursive = F
  )
  
  cachedate <- 
    sapply(paths_cache, function(p){
      nchunks <- length(strsplit(p, "_")[[1]])
      cachedate <- strsplit(p, "_")[[1]][nchunks] %>% gsub(pattern = "\\.rdata", replace = "")
    }) %>%
    as.Date() %>%
    max()
  
  message(paste0("Loading cache dated ", cachedate))
  
  paths_cache <- paths_cache[grepl(cachedate, paths_cache)]
  
  indata <- sapply(paths_indata, function(p){
    read.csv(file.path(path_foringest, p))
  })
  
  cache <- sapply(paths_cache, function(p){
    readRDS(file.path(path_cache, p))
  }) 
  
  cache <- cache[sapply(cache, length) > 0]
  
  names(cache) <- 
    names(cache) %>% 
    gsub(pattern = paste0(projectkey, "_"), replace = "") %>%
    gsub(pattern = paste0("_", cachedate, ".rdata"), replace = "")
    
  names(indata) <-
    names(indata) %>% 
    gsub(pattern = ".csv", replace = "")
  
  outdata <- 
    sapply(names(indata), function(nm){
      c <- cache[[nm]]
      n <- indata[[nm]]
      
      goodpkeys <- unique(c$PrimaryKey)
      
      out <- n %>% dplyr::filter(PrimaryKey %in% goodpkeys)
      
      return(out)
    })
  
  if(!dir.exists(file.path(path_foringest, "/New Data Only"))) dir.create(file.path(path_foringest, "/New Data Only"))
  
  sapply(1:length(outdata), function(i){
    write.csv(outdata[[i]], paste0(file.path(path_foringest, "/New Data Only", names(indata)[i]), ".csv"), row.names = F)
  })
  
  return(outdata)
}

#' @rdname ingest
#' @export fetch_splist_from_tdat
fetch_splist_from_tdat <- function(path_td, speciesstate){
  splist <- suppressWarnings(
    sf::st_read(path_td, "tblStateSpecies",
                query = paste0("SELECT * FROM tblStateSpecies WHERE SpeciesState = '", speciesstate, "'")))
  return(splist)
}
