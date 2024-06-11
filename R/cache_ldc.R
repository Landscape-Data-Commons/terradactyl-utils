#' Fetch data from the LDC server to act as a local cache
#' @rdname cache_ldc
#' @export cache_ldc
cache_ldc <- function(projectkey, verbose, path_cache){
  
  if(!dir.exists(path_cache)){
    stop("Invalid path_cache provided")
  }

  outprefix <- paste(path_cache, projectkey, sep = "_")
  
  dataHeader <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "header", take = 8000, verbose = T)
  saveRDS(dataHeader, paste0(outprefix, "_dataHeader_", Sys.Date(), ".rdata"))
  
  ### aero_summary
  aero_summary <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "aerosummary", take = 8000, verbose = T)
  saveRDS(aero_summary, paste0(outprefix, "_aero_summary_", Sys.Date(), ".rdata"))
  
  ### aero_runs ### Not available through API? Might need a terradactyl update
  # aero_runs <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "aero_runs", take = 8000, verbose = T)
  # saveRDS(aero_summary, paste0(outprefix, "_aero_runs_", Sys.Date(), ".rdata"))
  
  ### dataHorizontalFlux
  dataHorizontalFlux <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "horizontalflux", take = 8000, verbose = T)
  saveRDS(dataHorizontalFlux, paste0(outprefix, "_dataHorizontalFlux_", Sys.Date(), ".rdata"))
  
  ### LPI
  dataLPI <- fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "lpi", take = 8000, verbose = T)
  saveRDS(dataLPI, paste0(outprefix, "_dataLPI_", Sys.Date(), ".rdata"))
  
  ### Height
  dataHeight <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "height", take = 8000, verbose = T)
  saveRDS(dataHeight, paste0(outprefix, "_dataHeight_", Sys.Date(), ".rdata"))
  
  ### Gap
  dataGap <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "gap", take = 8000, verbose = T)
  saveRDS(dataGap, paste0(outprefix, "_dataGap_", Sys.Date(), ".rdata"))
  
  ### SR
  dataSpeciesInventory <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "speciesinventory", take = 8000, verbose = T)
  saveRDS(dataSpeciesInventory, paste0(outprefix, "_dataSpeciesInventory_", Sys.Date(), ".rdata"))
  
  ### SS
  dataSoilStability <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "soilstability", take = 8000, verbose = T)
  saveRDS(dataSoilStability, paste0(outprefix, "_dataSoilStability_", Sys.Date(), ".rdata"))
  
  ### geoInd
  geoIndicators <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "geoIndicators", take = 8000, verbose = T)
  saveRDS(geoIndicators, paste0(outprefix, "_geoIndicators_", Sys.Date(), ".rdata"))
  
  ### geoSpecies
  geoSpecies <- trex::fetch_ldc(keys = projectkey, key_type = "ProjectKey", data_type = "geoSpecies", take = 8000, verbose = T)
  saveRDS(geoSpecies, paste0(outprefix, "_geoSpecies_", Sys.Date(), ".rdata"))
  
  return(paste0("Data saved to ", path_cache))
}



