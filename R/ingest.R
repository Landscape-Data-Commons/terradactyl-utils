#' Prepare tall data for ingestion into the data commons
#' @description Given paths to tall tables and a schema, writes 

## ingest tools
#' @export ingest_coremethods
#' @rdname ingest
ingest_coremethods <- function(path_tall, path_out, path_schema, verbose = F){
  header   <- readRDS(file.path(path_tall, "header.Rdata"))
  tall_lpi <- readRDS(file.path(path_tall, "lpi_tall.Rdata"))
  tall_ht  <- readRDS(file.path(path_tall, "height_tall.Rdata"))
  tall_sr  <- readRDS(file.path(path_tall, "species_inventory_tall.Rdata"))
  tall_ss  <- readRDS(file.path(path_tall, "soil_stability_tall.Rdata"))
  tall_gap <- readRDS(file.path(path_tall, "gap_tall.Rdata"))
  
  fullmatrix <- readxl::read_xlsx(path_schema)
  
  dataGap <- tall_gap %>% 
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "dataGap"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)
  dataHeader <- header %>% 
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "dataHeader"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)
  dataHeight <- tall_ht %>% 
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "dataHeight"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)
  dataLPI <- tall_lpi %>% 
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "dataLPI"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)
  dataSpeciesInventory <- tall_sr %>% 
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "dataSpeciesInventory"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)
  dataSoilStability <- tall_ss %>% 
    translate_schema(
      matrix = subset(fullmatrix, Table2 == "dataSoilStability"),
      tocol = "Column2", fromcol = "Column1", verbose = verbose)
  
  write.csv(dataGap, file.path(path_out, "dataGap.csv"), row.names = F)
  write.csv(dataHeader, file.path(path_out, "dataHeader.csv"), row.names = F)
  write.csv(dataHeight, file.path(path_out, "dataHeight.csv"), row.names = F)
  write.csv(dataLPI, file.path(path_out, "dataLPI.csv"), row.names = F)
  write.csv(dataSoilStability, file.path(path_out, "dataSoilStability.csv"), row.names = F)
  write.csv(dataSpeciesInventory, file.path(path_out, "dataSpeciesInventory.csv"), row.names = F)
  
}

#' @export ingest_indicators_gdb
#' @rdname ingest
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
