#' Generate header from tblPlots object
#' @description Generate a header file from a tblPlots object in the environment. TEMPORARY! Need to fold this in to terradactyl.
#' @return A dataframe equivalent to what is produced by gather_header()

## Create the header from tblPlots, already loaded in environment.
#' @rdname gather_header_fromenv_terradat
#' @export gather_header_fromenv_terradat
gather_header_fromenv_terradat <- function(tblPlots) {
  # Set up filter expression (e.g., filter on DBKey, SpeciesState, etc)
  filter_exprs <- rlang::quos(...)
  
  # tblPlots provides the link between species tables
  # (LPI, Height, Species Richness) and tblStateSpecies
  header <- tblPlots %>%
    
    # Filter using the filtering expression specified by user
    dplyr::filter(!!!filter_exprs) %>%
    
    # Select the field names we need in the final feature class
    dplyr::select(PrimaryKey, SpeciesState, PlotID, PlotKey, DBKey,
                  EcologicalSiteId = EcolSite, Latitude_NAD83 = Latitude, Longitude_NAD83 = Longitude, State,
                  County, DateEstablished = EstablishDate, DateLoadedInDb,
                  ProjectName
    ) %>%
    
    # If there are any Sites with no PrimaryKeys, delete them
    subset(!is.na(PrimaryKey))
  
  # add null datevisited column to these. TO DO: get this data from LPI header
  header$DateVisited <- NA
  
  # Return the header file
  return(header)
}
