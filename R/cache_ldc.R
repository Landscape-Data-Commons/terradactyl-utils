#' Fetch data from the LDC server to act as a local cache
#' @rdname cache_ldc
#' @export cache_ldc
cache_ldc <- function(projectkey, verbose, path_cache){
  
  if(!dir.exists(path_cache)){
    stop("Invalid path_cache provided")
  }

  dataHeader <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "header", take = 8000, verbose = T)
  saveRDS(dataHeader, file.path(path_cache, paste0(projectkey, "_dataHeader_", Sys.Date(), ".rdata")))

  ### dataHorizontalFlux
  dataHorizontalFlux <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "horizontalflux", take = 8000, verbose = T)
  saveRDS(dataHorizontalFlux, file.path(path_cache, paste0(projectkey, "_dataHorizontalFlux_", Sys.Date(), ".rdata")))
  
  ### LPI
  dataLPI <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "lpi", take = 8000, verbose = T)
  saveRDS(dataLPI, file.path(path_cache, paste0(projectkey, "_dataLPI_", Sys.Date(), ".rdata")))
  
  ### Height
  dataHeight <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "height", take = 8000, verbose = T)
  saveRDS(dataHeight, file.path(path_cache, paste0(projectkey, "_dataHeight_", Sys.Date(), ".rdata")))
  
  ### Gap
  dataGap <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "gap", take = 8000, verbose = T)
  saveRDS(dataGap, file.path(path_cache, paste0(projectkey, "_dataGap_", Sys.Date(), ".rdata")))
  
  ### SR
  dataSpeciesInventory <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "speciesinventory", take = 8000, verbose = T)
  saveRDS(dataSpeciesInventory, file.path(path_cache, paste0(projectkey, "_dataSpeciesInventory_", Sys.Date(), ".rdata")))
  
  ### SS
  dataSoilStability <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "soilstability", take = 8000, verbose = T)
  saveRDS(dataSoilStability, file.path(path_cache, paste0(projectkey, "_dataSoilStability_", Sys.Date(), ".rdata")))
  
  ### geoInd
  geoIndicators <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "geoIndicators", take = 8000, verbose = T)
  saveRDS(geoIndicators, file.path(path_cache, paste0(projectkey, "_geoIndicators_", Sys.Date(), ".rdata")))
  
  ### geoSpecies
  geoSpecies <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "geoSpecies", take = 8000, verbose = T)
  saveRDS(geoSpecies, file.path(path_cache, paste0(projectkey, "_geoSpecies_", Sys.Date(), ".rdata")))
  
  return(paste0("Data saved to ", path_cache))
}



