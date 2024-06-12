#' Translate between schema given a table of associated columns
#'
#' @description Given data and a table associating two schema, translate the data into the new schema

#' @rdname translate_schema
#' @export translate_schema
translate_schema <- function(
  data,
  datatype,
  schema,
  # fromcol,
  # tocol,
  projectkey,
  dropcols = T,
  verbose = T){
  
  ### standardize names
  # colnames(matrix)[colnames(matrix) == fromcol] <- "terradactylAlias"
  # colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"
  
  ### process the incoming matrix by assigning actions to take at each row
  matrix_processed1 <- 
    schema %>% 
    dplyr::filter(Table == datatype) %>%
    dplyr::mutate(
      Field <- stringr::str_trim(Field, side = "both"),
    ) %>%
    dplyr::filter(Field != "" | terradactylAlias != "") %>%
    dplyr::select(terradactylAlias, Field)
  
  matrix_processed <- matrix_processed1 %>% 
    dplyr::mutate(
      DropColumn = matrix_processed1$terradactylAlias != "" & matrix_processed1$Field == "",
      AddColumn = matrix_processed1$Field != "" & matrix_processed1$terradactylAlias == "",
      ChangeColumn = matrix_processed1$Field != "" & matrix_processed1$terradactylAlias != "" & matrix_processed1$Field != matrix_processed1$terradactylAlias,
      NoAction = matrix_processed1$Field == matrix_processed1$terradactylAlias & !AddColumn & !DropColumn,
      Checksum = AddColumn + DropColumn + ChangeColumn + NoAction,
    )
  
    ## check for errors (if errors are here the function is not working)  
  errors <-
    matrix_processed %>%
    dplyr::filter(Checksum != 1)

  if(nrow(errors) > 0) {print("Errors found in translation matrix. Debug function.")
                              return(errors)}

  ChangeColumn <- 
    matrix_processed %>% 
    dplyr::filter(ChangeColumn)
  
  AddColumn <- 
    matrix_processed %>%
    dplyr::filter(AddColumn)
  
  DropColumn <-
    matrix_processed %>%
    dplyr::filter(DropColumn)
 
  ## run translation and add data
  outdata <- data %>%
    dplyr::rename_at(
      ChangeColumn$terradactylAlias, ~ ChangeColumn$Field) %>%
    `is.na<-`(AddColumn$Field)
  
  # # drop columns from prior schema if enabled
  # if(dropcols){
  #   outdata <- outdata %>%
  #     dplyr::select_if(!colnames(.) %in% DropColumn$terradactylAlias)
  # }
  
  # select only the tables in the out schema
  goodnames <- matrix_processed %>% dplyr::filter(Field != "") %>% dplyr::pull(Field)
  
  if(verbose) {
    print(paste("Returning", length(goodnames), "columns"))
    print(dplyr::all_of(goodnames))
  }
  
  outdata <- outdata %>%
    dplyr::select(dplyr::all_of(goodnames))
  
  
  outdata$ProjectKey <- projectkey
  # 
  # # return messages if verbose
  # if(verbose) {
  #   print(paste0(nrow(ChangeColumn), " columns renamed"))
  #   print(ChangeColumn[,c("terradactylAlias", "Field")])}
  # 
  # if(verbose) {
  #   print(paste0(nrow(AddColumn), " columns added"))
  #   print(AddColumn$Field)}
  # 
  # if(verbose & dropcols) {
  #   print(paste0(nrow(DropColumn), " columns removed"))
  #   print(DropColumn$terradactylAlias)
  # }
  
  return(outdata)
}

#' @rdname translate_schema
#' @export translate_coremethods
translate_coremethods <- function(path_tall, path_out, path_schema, projectkey, verbose = F){

  schema <- read.csv(path_schema)

  if(file.exists(file.path(path_tall, "header.Rdata"))){
    print("Translating header data")
    header   <- readRDS(file.path(path_tall, "header.Rdata"))
    dataHeader <- header %>%
      translate_schema(schema = schema,
                       projectkey = projectkey,
                       datatype = "dataHeader",
                       dropcols = TRUE,
                       verbose = TRUE)
    write.csv(dataHeader, file.path(path_out, "dataHeader.csv"), row.names = F)
  } else {
    stop("Header data not found. Unable to translate data")
  }

  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))){
    print("Translating LPI data")
    tall_lpi <- readRDS(file.path(path_tall, "lpi_tall.Rdata")) %>%
      dplyr::left_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited))
     dataLPI <- tall_lpi %>%
       translate_schema(schema = schema,
                        projectkey = projectkey,
                        datatype = "dataLPI",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataLPI, file.path(path_out, "dataLPI.csv"), row.names = F)
  } else {
    print("LPI data not found")
  }

  if(file.exists(file.path(path_tall, "height_tall.Rdata"))){
    print("Translating height data")
    tall_ht  <- readRDS(file.path(path_tall, "height_tall.Rdata")) %>%
      dplyr::left_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited))
    dataHeight <- tall_ht %>%
      translate_schema(schema = schema,
                       projectkey = projectkey,
                       datatype = "dataHeight",
                       dropcols = TRUE,
                       verbose = TRUE)
    write.csv(dataHeight, file.path(path_out, "dataHeight.csv"), row.names = F)
  } else {
    print("Height data not found")
  }

  if(file.exists(file.path(path_tall, "species_inventory_tall.Rdata"))){
    print("Translating species inventory data")
    tall_sr  <- readRDS(file.path(path_tall, "species_inventory_tall.Rdata")) %>%
      dplyr::left_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited))
    dataSpeciesInventory <- tall_sr %>%
      translate_schema(schema = schema,
                       projectkey = projectkey,
                       datatype = "dataSpeciesInventory",
                       dropcols = TRUE,
                       verbose = TRUE)
    write.csv(dataSpeciesInventory, file.path(path_out, "dataSpeciesInventory.csv"), row.names = F)
  } else {
    print("Species inventory data not found")
  }


  if(file.exists(file.path(path_tall, "soil_stability_tall.Rdata"))){
    print("Translating soil stability data")
    tall_ss  <- readRDS(file.path(path_tall, "soil_stability_tall.Rdata")) %>%
      dplyr::left_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited))
    dataSoilStability <- tall_ss %>%
      translate_schema(schema = schema,
                       projectkey = projectkey,
                       datatype = "dataSoilStability",
                       dropcols = TRUE,
                       verbose = TRUE)
    write.csv(dataSoilStability, file.path(path_out, "dataSoilStability.csv"), row.names = F)
  } else {
    print("Soil stability data not found")
  }

  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))){
    print("Translating canopy gap data")
    tall_gap <- readRDS(file.path(path_tall, "gap_tall.Rdata")) %>%
      dplyr::left_join(dataHeader %>% dplyr::select(PrimaryKey, DateVisited))
    dataGap <- tall_gap %>%
      translate_schema(schema = schema,
                       projectkey = projectkey,
                       datatype = "dataGap",
                       dropcols = TRUE,
                       verbose = TRUE)
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

  fullmatrix <- read.csv(path_schema)

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
