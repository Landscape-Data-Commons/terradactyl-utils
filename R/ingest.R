#' Take data from public postgres server and prepare them for ingestion
#' @rdname ingest
#' @export ingest_DIMA
ingest_DIMA <- function(projectkey, 
                        path_specieslist, 
                        path_templatetd,
                        doLPI = T,
                        doGap = T,
                        doSR = T,
                        doSS = T,
                        doHt = T,
                        doRH = T, # not yet implemented
                        doHF = T,
                        doGSP = T){
  
  # Set paths, based on the suffix. Creates file folders if needed
  path_parent <- paste0("C:/Users/jrbrehm/Documents/Data/", projectkey)
  if(!dir.exists(path_parent)) dir.create(path_parent)
  path_dimatables <- file.path(path_parent, "DIMATables")
  if(!dir.exists(path_dimatables)) dir.create(path_dimatables)
  path_tall <- file.path(path_parent, "Tall")
  if(!dir.exists(path_tall)) dir.create(path_tall)
  path_foringest <- file.path(path_parent, "For Ingest")
  if(!dir.exists(path_foringest)) dir.create(path_foringest)
  
  # next sections are split out by method in order to let you pick and choose which to run. Each follows this pattern:
  ### get data from postgres server
  ### save dima tables to path_dimatables
  ### gather 
  ### drop any duplicate rows (rows where dbkey and dateloadedindb are the only differences)
  ### write tall
  
  tblPlots <- fetch_postgres("tblPlots", schema = "public", projectkey = projectkey)
  tblPlots$SpeciesState <- projectkey
  
  write.csv(tblPlots, file.path(path_dimatables, "tblPlots.csv"), row.names = F)
  
  if(doGap){
    tblGapHeader <- fetch_postgres("tblGapHeader", schema = "public", projectkey = projectkey)
    tblGapDetail <- fetch_postgres("tblGapDetail", schema = "public", projectkey = projectkey)
    write.csv(tblGapHeader, file.path(path_dimatables, "tblGapHeader.csv"), row.names = F)
    write.csv(tblGapDetail, file.path(path_dimatables, "tblGapDetail.csv"), row.names = F)
    
    tall_gap <- gather_gap(source = "AIM", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail)
    
    dropcols_gap <- tall_gap %>% dplyr::select(-"DBKey")
    tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),]
    
    saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
    write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
    
  } else {
    tblGapHeader <- NULL # need to have an object named in order to run gather_header below
  }
  
  if(doLPI){
    tblLPIHeader <- fetch_postgres("tblLPIHeader", schema = "public", projectkey = projectkey)
    tblLPIDetail <- fetch_postgres("tblLPIDetail", schema = "public", projectkey = projectkey)
    write.csv(tblLPIHeader, file.path(path_dimatables, "tblLPIHeader.csv"), row.names = F)
    write.csv(tblLPIDetail, file.path(path_dimatables, "tblLPIDetail.csv"), row.names = F)
    
    tall_lpi <- gather_lpi(source = "AIM", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    
    dropcols_lpi <- tall_lpi %>% dplyr::select(-"DBKey")
    tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),]
    
    saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
    write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)
    
  } else {
    tblLPIHeader <- NULL # need to have an object named in order to run gather_header below
  }
  
  if(doHt){
    if(!doLPI) {
      tblLPIHeader <- fetch_postgres("tblLPIHeader", schema = "public", projectkey = projectkey)
      tblLPIDetail <- fetch_postgres("tblLPIDetail", schema = "public", projectkey = projectkey)
      write.csv(tblLPIHeader, file.path(path_dimatables, "tblLPIHeader.csv"), row.names = F)
      write.csv(tblLPIDetail, file.path(path_dimatables, "tblLPIDetail.csv"), row.names = F)
    }
    tall_height <- gather_height(source = "AIM", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    dropcols_height <- tall_height %>% dplyr::select(-"DBKey")
    tall_height <- tall_height[which(!duplicated(dropcols_height)),]
    saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
    write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)
    
  }
  
  if(doSR){
    tblSpecRichHeader <- fetch_postgres("tblSpecRichHeader", schema = "public", projectkey = projectkey)
    tblSpecRichDetail <- fetch_postgres("tblSpecRichDetail", schema = "public", projectkey = projectkey)
    write.csv(tblSpecRichHeader, file.path(path_dimatables, "tblSpecRichHeader.csv"), row.names = F)
    write.csv(tblSpecRichDetail, file.path(path_dimatables, "tblSpecRichDetail.csv"), row.names = F)
    
    tall_speciesinventory <- gather_species_inventory(source = "AIM", tblSpecRichDetail = tblSpecRichDetail, tblSpecRichHeader = tblSpecRichHeader)
    
    dropcols_speciesinventory <- tall_speciesinventory %>% dplyr::select(-"DBKey")
    tall_speciesinventory <- tall_speciesinventory[which(!duplicated(dropcols_speciesinventory)),]
    
    saveRDS(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.rdata"))
    write.csv(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)
    
  } else {
    tblSpecRichHeader <- NULL # need to have an object named in order to run gather_header below
  }
  
  if(doSS){
    # this section entirely untested
    tblSoilStabHeader <- fetch_postgres("tblSoilStabHeader", schema = "public", projectkey = projectkey)
    tblSoilStabDetail <- fetch_postgres("tblSoilStabDetail", schema = "public", projectkey = projectkey)
    write.csv(tblSoilStabHeader, file.path(path_dimatables, "tblSoilStabHeader.csv"), row.names = F)
    write.csv(tblSoilStabDetail, file.path(path_dimatables, "tblSoilStabDetail.csv"), row.names = F)
    
    tall_soilstability <- gather_soil_stability(source = "AIM", tblSoilStabHeader = tblSoilStabHeader, tblSoilStabDetail = tblSoilStabDetail)
    
    dropcols_soilstability <- tall_soilstability %>% dplyr::select(-"DBKey")
    tall_soilstability <- tall_soilstability[which(!duplicated(dropcols_soilstability)),]
    
    saveRDS(tall_soilstability, file.path(path_tall, "soil_stability_tall.rdata"))
    write.csv(tall_soilstability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)
  } else {
    tblSoilStabHeader = NULL
  }
  
  
  if(doHF){
    tblHorizontalFlux <- fetch_postgres("tblHorizontalFlux", schema = "public", projectkey = projectkey)
    # no gather needed here
    
    dropcols_hf <- tblHorizontalFlux %>% dplyr::select(-"DBKey")
    tblHorizontalFlux <- tblHorizontalFlux[which(!duplicated(dropcols_hf)),]
    
    tblHorizontalFlux <- tblHorizontalFlux %>% 
      dplyr::rename(DateLoadedInDb = DateLoadedInDB) %>%
      dplyr::mutate(ProjectKey = projectkey,
                    DateEstablished = NA,
                    BoxID = as.character(BoxID),
                    StackID = as.character(StackID)) %>%
      dplyr::select(-PlotKey, -Collector, -labTech, -rid)
    
    # saveRDS(tblHorizontalFlux, file.path(path_tall, "dataHorizontalFlux.rdata"))
    # write.csv(tblHorizontalFlux, file.path(path_tall, "dataHorizontalFlux.csv"), row.names = F)

  } else {
    tblHorizontalFlux = NULL
  }

  
  header <- gather_header(dsn = NULL, source = "AIM", tblPlots = tblPlots, date_tables = list(tblLPIHeader, tblGapHeader, 
                                                                                              tblSpecRichHeader, tblHorizontalFlux))
  
  dropcols_header <- header %>% dplyr::select(-"DBKey", -"DateLoadedInDb")
  header <- header[which(!duplicated(dropcols_header)),]
  
  write.csv(header, file.path(path_tall, "header.csv"), row.names = F)
  saveRDS(header, file.path(path_tall, "header.rdata"))
  
  # attach date to horizontalflux
  if(nrow(tblHorizontalFlux > 0)){
    tblHorizontalFlux <- tblHorizontalFlux %>%
      dplyr::right_join(header %>% dplyr::select(PrimaryKey, DateVisited))
    write.csv(tblHorizontalFlux, file.path(path_foringest, "dataHorizontalFlux.csv"), row.names = F)
  }
    
  # translate tall
  translate_coremethods(path_tall = path_tall,
                     path_out = path_foringest,
                     path_schema = "C:/Users/jrbrehm/Documents/GitHub/Workspace/Schema Translation/Translation.xlsx",
                     projectkey = projectkey,
                     verbose = T)
  
  if(doLPI) {
    l <- lpi_calc(
      lpi_tall = file.path(path_tall, "lpi_tall.rdata"),
      header = header,
      source = "AIM",
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
      source = "AIM",
      species_file = path_templatetd
    )
  } else {
    h <- NULL
  }
  
  if(doSR){
    sr <- spp_inventory_calc(
      header = header,
      spp_inventory_tall = file.path(path_tall, "species_inventory_tall.rdata"),
      species_file = path_specieslist, 
      source = "AIM"
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
    print("rangehealth summary not yet implemented")
  } else {
    rh <- NULL
  }
  
  all_indicators <- header
  if(doLPI) all_indicators <- all_indicators %>% dplyr::left_join(l)
  if(doGap) all_indicators <- all_indicators %>% dplyr::left_join(g) 
  if(doHt) all_indicators <- all_indicators %>% dplyr::left_join(h)  
  if(doSR) all_indicators <- all_indicators %>% dplyr::left_join(sr)
  if(doSS) all_indicators <- all_indicators %>% dplyr::left_join(ss)
  if(doRH) all_indicators <- all_indicators %>% dplyr::left_join(rh)
  
  all_indicators_dropcols <- all_indicators %>% 
    dplyr::select(-"DBKey", -"DateLoadedInDb")
  all_indicators_unique <- all_indicators[which(!duplicated(all_indicators_dropcols)),]
  
  i <- terradactylUtils::add_indicator_columns(template = path_templatetd,
                                               source = "AIM",
                                               all_indicators = all_indicators_unique,
                                               prefixes_to_zero = c("AH", "FH", "NumSpp"))
  geoInd <- i %>% 
    translate_schema(matrix = subset(readxl::read_xlsx("C:/Users/jrbrehm/Documents/GitHub/Workspace/Schema Translation/Translation.xlsx"), 
                                     Table2 == "geoIndicators"), tocol = "Column2", 
                     fromcol = "Column1",
                     projectkey = projectkey,
                     dropcols = T)
  
  colnames(geoInd)
  
  # drop MWAC-only plots
  # geoInd <- 
  #   geoInd %>% dplyr::filter(!(is.na(AH_AnnGrassCover) & is.na(GapCover_25_plus) 
  #                              & is.na(Hgt_Grass_Avg) & is.na(RH_InvasivePlants) 
  #                              & is.na(SoilStability_All)))
  
  write.csv(geoInd, file = file.path(path_foringest, "geoIndicators.csv"), row.names = F)
  
  if(doGSP){
    a <- accumulated_species(
      lpi_tall = file.path(path_tall, "lpi_tall.rdata"),
      height_tall = file.path(path_tall, "height_tall.rdata"),
      spp_inventory_tall = file.path(path_tall, "species_inventory_tall.rdata"),
      header = file.path(path_tall, "header.rdata"),
      species_file = path_specieslist,
      dead = F,
      source = "AIM") %>% 
      dplyr::right_join(header %>% dplyr::select(PrimaryKey, DateVisited, DBKey)) %>% 
      translate_schema(matrix = subset(readxl::read_xlsx("C:/Users/jrbrehm/Documents/GitHub/workspace/Schema Translation/Translation.xlsx"), 
                                       Table2 == "geoSpecies"), tocol = "Column2", fromcol = "Column1", projectkey = "NDOW")
    
    write.csv(a, file.path(path_foringest, "geoSpecies.csv"), row.names = F)
    
  }
  
  return(geoInd)
}


#' @rdname ingest
#' @export translate_coremethods
translate_coremethods <- function(path_tall, path_out, path_schema, projectkey, verbose = F){

  fullmatrix <- readxl::read_xlsx(path_schema)

  if(file.exists(file.path(path_tall, "header.Rdata"))){
    print("Translating header data")
    header   <- readRDS(file.path(path_tall, "header.Rdata"))
    dataHeader <- header %>%
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataHeader"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataHeader, file.path(path_out, "dataHeader.csv"), row.names = F)
  } else {
    stop("Header data not found. Unable to translate data")
  }

  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))){
    print("Translating LPI data")
    tall_lpi <- readRDS(file.path(path_tall, "lpi_tall.Rdata")) %>%
      dplyr::right_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited, DBKey))
     dataLPI <- tall_lpi %>%
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataLPI"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataLPI, file.path(path_out, "dataLPI.csv"), row.names = F)
  } else {
    print("LPI data not found")
  }

  if(file.exists(file.path(path_tall, "height_tall.Rdata"))){
    print("Translating height data")
    tall_ht  <- readRDS(file.path(path_tall, "height_tall.Rdata")) %>%
      dplyr::right_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited, DBKey))
    dataHeight <- tall_ht %>%
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataHeight"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataHeight, file.path(path_out, "dataHeight.csv"), row.names = F)
  } else {
    print("Height data not found")
  }

  if(file.exists(file.path(path_tall, "species_inventory_tall.Rdata"))){
    print("Translating species inventory data")
    tall_sr  <- readRDS(file.path(path_tall, "species_inventory_tall.Rdata")) %>%
      dplyr::right_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited, DBKey))
    dataSpeciesInventory <- tall_sr %>%
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataSpeciesInventory"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataSpeciesInventory, file.path(path_out, "dataSpeciesInventory.csv"), row.names = F)
  } else {
    print("Species inventory data not found")
  }


  if(file.exists(file.path(path_tall, "soil_stability_tall.Rdata"))){
    print("Translating soil stability data")
    tall_ss  <- readRDS(file.path(path_tall, "soil_stability_tall.Rdata")) %>%
      dplyr::right_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited, DBKey))
    dataSoilStability <- tall_ss %>%
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataSoilStability"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataSoilStability, file.path(path_out, "dataSoilStability.csv"), row.names = F)
  } else {
    print("Soil stability data not found")
  }

  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))){
    print("Translating canopy gap data")
    tall_gap <- readRDS(file.path(path_tall, "gap_tall.Rdata")) %>%
      dplyr::right_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited, DBKey))
    dataGap <- tall_gap %>%
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataGap"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataGap, file.path(path_out, "dataGap.csv"), row.names = F)
  } else {
    print("Gap data not found")
  }
}

#' @rdname ingest
#' @export ingest_indicators_gdb
ingest_indicators_gdb  <- function(path_terradat, path_out, path_schema, verbose = F){

  td <- sf::st_read(path_terradat, "TerrADat")
  tdsp <- sf::st_read(path_terradat, "TerrADatSpeciesIndicators")
  lmf <- sf::st_read(path_terradat, "LMF")
  lmfsp <- sf::st_read(path_terradat, "LMFSpeciesIndicators")

  fullmatrix <- readxl::read_xlsx(path_schema)

  tall_geoIndicators <- dplyr::bind_rows(td, lmf)
  geoIndicators <- tall_geoIndicators %>%
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "geoIndicators"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)

  tall_geoSpecies <- dplyr::bind_rows(tdsp, lmfsp)
  geoSpecies <- tall_geoSpecies %>%
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "geoSpecies"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)

  write.csv(geoIndicators, file.path(path_out, "geoIndicators.csv"), row.names = F)
  write.csv(geoSpecies, file.path(path_out, "geoSpecies.csv"), row.names = F)

  return(list(geoIndicators, geoSpecies))

}
