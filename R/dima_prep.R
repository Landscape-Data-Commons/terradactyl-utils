#' Attach PrimaryKeys to DIMA Tables
#'
#' @description Given path to a DIMA and a designation for SpeciesState, create an R object (list) containing DIMA tables with PrimaryKeys appended

#' @rdname dima_prep
#' @export dima_prep
dima_prep <- function(path_dima, speciesstate, path_out = NULL, remove_invalid_keys = T){
  
  # Get DBKey (name of the DIMA) from the file path
  dbkey <- strsplit(path_dima, "/")[[1]][length(strsplit(path_dima, "/")[[1]])] %>% tools::file_path_sans_ext()
  
  # Create and name queries to send to extract_table
  l_query <- list("SELECT * FROM tblPlots",
                  "SELECT * FROM tblLPIHeader",
                  "SELECT * FROM tblLPIDetail",
                  "SELECT * FROM tblGapHeader",
                  "SELECT * FROM tblGapDetail",
                  "SELECT * FROM tblSpecRichHeader",
                  "SELECT * FROM tblSpecRichDetail",
                  "SELECT * FROM tblSoilStabHeader",
                  "SELECT * FROM tblSoilStabDetail",
                  "SELECT * FROM tblQualHeader",
                  "SELECT * FROM tblQualDetail",
                  "SELECT * FROM tblSpecies",
                  "SELECT * FROM tblLines")
  
  names(l_query) <- c("tblPlots",
                      "tblLPIHeader",
                      "tblLPIDetail",
                      "tblGapHeader",
                      "tblGapDetail",
                      "tblSpecRichHeader",
                      "tblSpecRichDetail",
                      "tblSoilStabHeader",
                      "tblSoilStabDetail",
                      "tblQualHeader",
                      "tblQualDetail",
                      "tblSpecies",
                      "tblLines")
  
  # Get data out of the specified DIMA
  data_dima <- terradactylUtils::extract_table(path_dima, l_query)
  
  # create join tables, connecting all relevant keys
  join_lpi <- 
    dplyr::left_join(data_dima$tblLPIHeader %>% dplyr::select("RecKey", "LineKey", "plotVisitKey") %>% 
                       dplyr::mutate_all(as.character), 
                     data_dima$tblLines %>% dplyr::select("LineKey", "PlotKey") %>% 
                       dplyr::mutate_all(as.character), 
                     by = "LineKey") %>%
    unique() %>%
    dplyr::filter(PlotKey != "999999999" &
                    !is.na(PlotKey))
  
  join_gap <-
    dplyr::left_join(data_dima$tblGapHeader %>% dplyr::select("RecKey", "LineKey", "plotVisitKey") %>% 
                       dplyr::mutate_all(as.character), 
                     data_dima$tblLines %>% dplyr::select("LineKey", "PlotKey") %>% 
                       dplyr::mutate_all(as.character), 
                     by = "LineKey") %>%
    unique() %>% 
    dplyr::mutate_all(as.character) %>%
    dplyr::filter(PlotKey != "999999999" &
                    !is.na(PlotKey))
  
  join_sr <-
    dplyr::left_join(data_dima$tblSpecRichHeader %>% dplyr::select("RecKey", "LineKey", "plotVisitKey") %>% 
                       dplyr::mutate_all(as.character), 
                     data_dima$tblLines %>% dplyr::select("LineKey", "PlotKey") %>% 
                       dplyr::mutate_all(as.character), 
                     by = "LineKey") %>%
    unique() %>% 
    dplyr::mutate_all(as.character) %>%
    dplyr::filter(PlotKey != "999999999" &
                    !is.na(PlotKey))
  
  join_ss <- data_dima$tblSoilStabHeader %>% dplyr::select("PlotKey", "plotVisitKey", "RecKey") %>%  
    unique() %>% 
    dplyr::mutate_all(as.character) %>%
    dplyr::filter(PlotKey != "999999999" &
                    !is.na(PlotKey))
  
  
  join_plots <- do.call(rbind, list(join_lpi %>% dplyr::select("PlotKey", "plotVisitKey"), 
                                    join_gap %>% dplyr::select("PlotKey", "plotVisitKey"), 
                                    join_sr %>% dplyr::select("PlotKey", "plotVisitKey"), 
                                    join_ss %>% dplyr::select("PlotKey", "plotVisitKey"))) %>% 
    unique()
  
  if(all(is.na(join_plots$plotVisitKey))){
    stop("No plotVisitKey values found. Open DIMA and select Administrative Functions > Data Management > Manage Plot Visit Dates")
  }
  
  # rh doesn't have plotVisitKey in header, have to get it from join_plots
  join_rh <- data_dima$tblQualHeader %>% dplyr::select("PlotKey", "RecKey") %>% 
    unique() %>% 
    dplyr::mutate_all(as.character) %>%
    dplyr::left_join(join_plots, by = "PlotKey") %>% 
    dplyr::filter(PlotKey != "999999999" &
                    !is.na(PlotKey))
  
  
  ### create output list
  data_out <- list()
  
  ### Attach primarykey to each table
  # Plots
  data_out$tblPlots <- join_plots %>% 
    dplyr::left_join(
      data_dima$tblPlots %>% 
        dplyr::mutate(SiteKey = as.character(SiteKey),
                      PlotKey = as.character(PlotKey)),
      by = "PlotKey") %>%
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey,
                  DateLoadedInDb = Sys.Date()) %>%
    unique()
  
  # LPI
  data_out$tblLPIHeader <- data_dima$tblLPIHeader %>%
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::mutate(DBKey = dbkey,
                  RecKey = as.character(RecKey),
                  LineKey = as.character(LineKey)
    )
  data_out$tblLPIDetail <- join_lpi %>% 
    dplyr::left_join(data_dima$tblLPIDetail %>%
                       dplyr::mutate(RecKey = as.character(RecKey),
                       ),
                     by = "RecKey") %>% 
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey) %>%
    unique()
  
  # Gap
  data_out$tblGapHeader <- data_dima$tblGapHeader %>%
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::mutate(DBKey = dbkey,
                  RecKey = as.character(RecKey),
                  LineKey = as.character(LineKey))
  
  data_out$tblGapDetail <- join_gap %>% 
    dplyr::left_join(data_dima$tblGapDetail %>%
                       dplyr::mutate(RecKey = as.character(RecKey)), 
                     by = "RecKey") %>% 
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey) %>%
    dplyr::select(-LineKey) %>%
    unique()
  
  # Species Richness
  data_out$tblSpecRichHeader <- data_dima$tblSpecRichHeader %>%
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::mutate(DBKey = dbkey,
                  RecKey = as.character(RecKey),
                  LineKey = as.character(LineKey))
  
  data_out$tblSpecRichDetail <- join_sr %>% 
    dplyr::left_join(data_dima$tblSpecRichDetail %>%
                       dplyr::mutate(RecKey = as.character(RecKey)), 
                     by = "RecKey") %>% 
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey) %>%
    unique()
  
  # Soil stability
  data_out$tblSoilStabHeader <- data_dima$tblSoilStabHeader %>%
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::mutate(DBKey = dbkey,
                  PlotKey = as.character(PlotKey),
                  RecKey = as.character(RecKey))
  
  data_out$tblSoilStabDetail <- join_ss %>% 
    dplyr::left_join(data_dima$tblSoilStabDetail %>%
                       dplyr::mutate(RecKey = as.character(RecKey)), 
                     by = "RecKey") %>% 
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey) %>%
    dplyr::select(-"PlotKey") %>%
    unique()
  
  # Range health
  data_out$tblQualHeader <- join_rh %>% 
    dplyr::left_join(data_dima$tblQualHeader %>%
                       dplyr::mutate(PlotKey = as.character(PlotKey),
                                     RecKey = as.character(RecKey)), 
                     by = c("PlotKey", "RecKey")) %>% 
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey,
                  DateLoadedInDb = NA) %>%
    unique()
  data_out$tblQualDetail <- join_rh %>% 
    dplyr::left_join(data_dima$tblQualDetail %>%
                       dplyr::mutate(RecKey = as.character(RecKey)), 
                     by = "RecKey") %>% 
    dplyr::rename(PrimaryKey = plotVisitKey) %>%
    dplyr::arrange(PlotKey) %>% 
    dplyr::mutate(DBKey = dbkey) %>%
    unique()
  
  ### remove invalid keys
  if(remove_invalid_keys){
    data_out <- sapply(1:length(data_out), function(n){
      
      invalid_rows <- 
        grepl(".*nvalid Key.*", data_out[[n]]$PrimaryKey) |
        grepl("^999999.*", data_out[[n]]$PrimaryKey)
      
      if(any(invalid_rows)){
        print(paste(sum(invalid_rows), "records could not be connected to a valid PlotKey in table", names(data_out)[n], "and were removed"))
      }
      
      data_out[[n]][!invalid_rows,]
      
    })
    
  }
  
  # attach tblSpecies
  data_out$tblSpecies <- data_dima$tblSpecies
  
  ### name the output
  # The last object in the list is tblLines, which is not needed
  names(data_out) <- names(l_query)[1:(length(l_query) - 1)]
  
  ### attach species state to tblPlots and tblSpecies, to match terradat formatting
  data_out$tblPlots$SpeciesState <- speciesstate
  data_out$tblSpecies$SpeciesState <- speciesstate
  
  ### attach missing data to tblSpecies
  if(!("Noxious" %in% colnames(data_out$tblSpecies))) {
    print("Adding NA Noxious classifications. Edit the output species list to add this data")
    data_out$tblSpecies$Noxious <- ""
  }
  if(!("SG_Group" %in% colnames(data_out$tblSpecies))) {
    print("Adding NA SG_Group classifications. Edit the output species list to add this data")
    data_out$tblSpecies$SG_Group <- ""
  }
  
  ### change GrowthHabitCode to GrowthHabit and GrowthHabitSub
  data_out$tblSpecies$GrowthHabitSub <- dplyr::case_when(
    data_out$tblSpecies$GrowthHabitCode == 1 ~ "Tree",
    data_out$tblSpecies$GrowthHabitCode == 2 ~ "Shrub",
    data_out$tblSpecies$GrowthHabitCode == 3 ~ "SubShrub",
    data_out$tblSpecies$GrowthHabitCode == 4 ~ "Succulent",
    data_out$tblSpecies$GrowthHabitCode == 5 ~ "Forb",
    data_out$tblSpecies$GrowthHabitCode == 6 ~ "Graminoid",
    data_out$tblSpecies$GrowthHabitCode == 7 ~ "Sedge"
  )  
  data_out$tblSpecies$GrowthHabit <- dplyr::case_when(
    data_out$tblSpecies$GrowthHabitCode %in% 1:4 ~ "Woody",
    data_out$tblSpecies$GrowthHabitCode %in% 5:7 ~ "NonWoody"
  )
  
  data_out$tblSpecies <- data_out$tblSpecies %>% dplyr::select_if(!names(.) %in% c("SortSeq"))
  
  ### write output
  if(!is.null(path_out)){
    saveRDS(data_out, file.path(path_out, paste0(dbkey,  "_WithPrimaryKeys.rdata")))
    write.csv(data_out$tblSpecies, file.path(path_out, paste0(dbkey, "_tblSpecies.csv")), row.names = F)
  }
  
  return(data_out)
}
