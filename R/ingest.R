#' Prepare tall data for ingestion into the data commons
#' @description Given paths to tall tables and a schema, writes 

## ingest tools
#' @rdname ingest
#' @export ingest_coremethods
ingest_coremethods <- function(path_tall, path_out, path_schema, projectkey, verbose = F){
  
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
    print("Header data not found")
  }
  
  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))){
    print("Translating LPI data")
    tall_lpi <- readRDS(file.path(path_tall, "lpi_tall.Rdata"))
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
    tall_ht  <- readRDS(file.path(path_tall, "height_tall.Rdata"))
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
    tall_sr  <- readRDS(file.path(path_tall, "species_inventory_tall.Rdata"))
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
    tall_ss  <- readRDS(file.path(path_tall, "soil_stability_tall.Rdata"))
    dataSoilStability <- tall_ss %>% 
      translate_schema(
        matrix = subset(fullmatrix, Table2 == "dataSoilStability"),
        tocol = "Column2", fromcol = "Column1", verbose = verbose, projectkey = projectkey)
    write.csv(dataSoilStability, file.path(path_out, "dataSoilStability.csv"), row.names = F)
  } else {
    print("Soil stability data not found")
  }
  
  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))){
    tall_gap <- readRDS(file.path(path_tall, "gap_tall.Rdata"))
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
