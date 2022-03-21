#' Translate between schema given a table of associated columns
#'
#' @description Given data and a table associating two schema, translate the data into the new schema

#' @rdname translate_schema
#' @export translate_schema
translate_schema <- function(
  data,
  matrix,
  fromcol,
  tocol,
  dropcols = T,
  verbose = T){
  # matrix <- submatrix; data <- ldcdata; fromcol <- "Column2"; tocol <- "Column1"; verbose = T; dropcols = T
  
  ### standardize names
  ### could not figure out the dplyr::rename version of this
  colnames(matrix)[colnames(matrix) == fromcol] <- "FromColumn"
  colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"
  
  ### process the incoming matrix by assigning actions to take at each row
  matrix_processed <- 
    matrix %>% 
    dplyr::filter(!is.na(ToColumn) | !is.na(FromColumn)) %>%
    dplyr::select(FromColumn, ToColumn) %>% 
    dplyr::mutate(
      DropColumn = !is.na(FromColumn) & is.na(ToColumn),
      AddColumn = !is.na(ToColumn) & is.na(FromColumn),
      ChangeColumn = !is.na(ToColumn) & !is.na(FromColumn) & ToColumn != FromColumn,
      NoAction = ToColumn == FromColumn & !AddColumn & !DropColumn,
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
      ChangeColumn$FromColumn, ~ ChangeColumn$ToColumn) %>%
    `is.na<-`(AddColumn$ToColumn)
  
  # drop columns from prior schema if enabled
  if(dropcols){
    outdata <- outdata %>%
      dplyr::select_if(!colnames(.) %in% DropColumn$FromColumn)
  }
  
  # return messages if verbose
  if(verbose) {
    print(paste0(nrow(ChangeColumn), " columns renamed"))
    print(ChangeColumn[,c("FromColumn", "ToColumn")])}
  
  if(verbose) {
    print(paste0(nrow(AddColumn), " columns added"))
    print(AddColumn$ToColumn)}
  
  if(verbose & dropcols) {
    print(paste0(nrow(DropColumn), " columns removed"))
    print(DropColumn$FromColumn)
  }
  return(outdata)
}

