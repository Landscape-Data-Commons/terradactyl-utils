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
  projectkey,
  dropcols = T,
  verbose = T){
  
  ### standardize names
  colnames(matrix)[colnames(matrix) == fromcol] <- "FromColumn"
  colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"
  
  ### process the incoming matrix by assigning actions to take at each row
  matrix_processed1 <- 
    matrix %>% 
    dplyr::mutate(
      ToColumn <- stringr::str_trim(ToColumn, side = "both"),
    ) %>%
    dplyr::filter(ToColumn != "" | FromColumn != "") %>%
    dplyr::select(FromColumn, ToColumn)
  
  matrix_processed <- matrix_processed1 %>% 
    dplyr::mutate(
      DropColumn = (matrix_processed1$FromColumn != "" & matrix_processed1$ToColumn == ""),
      AddColumn = matrix_processed1$ToColumn != "" & matrix_processed1$FromColumn == "",
      ChangeColumn = matrix_processed1$ToColumn != "" & matrix_processed1$FromColum != "" & matrix_processed1$ToColumn != matrix_processed1$FromColumn,
      NoAction = matrix_processed1$ToColumn == matrix_processed1$FromColumn & !AddColumn & !DropColumn,
      Checksum = AddColumn + DropColumn + ChangeColumn + NoAction,
    )
  
  ## old code from when translation matrix was a .xlsx (which records blanks as NA rather than "")
    # dplyr::filter(!is.na(ToColumn) | !is.na(FromColumn)) %>%
    # dplyr::select(FromColumn, ToColumn) %>% 
    # dplyr::mutate(
    #   DropColumn = !is.na(FromColumn) & is.na(ToColumn),
    #   AddColumn = !is.na(ToColumn) & is.na(FromColumn),
    #   ChangeColumn = !is.na(ToColumn) & !is.na(FromColumn) & ToColumn != FromColumn,
    #   NoAction = ToColumn == FromColumn & !AddColumn & !DropColumn,
    #   Checksum = AddColumn + DropColumn + ChangeColumn + NoAction,
    # )
  
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
  
  # # drop columns from prior schema if enabled
  # if(dropcols){
  #   outdata <- outdata %>%
  #     dplyr::select_if(!colnames(.) %in% DropColumn$FromColumn)
  # }
  
  # select only the tables in the out schema
  goodnames <- matrix_processed %>% dplyr::filter(ToColumn != "") %>% dplyr::pull(ToColumn)
  
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
  #   print(ChangeColumn[,c("FromColumn", "ToColumn")])}
  # 
  # if(verbose) {
  #   print(paste0(nrow(AddColumn), " columns added"))
  #   print(AddColumn$ToColumn)}
  # 
  # if(verbose & dropcols) {
  #   print(paste0(nrow(DropColumn), " columns removed"))
  #   print(DropColumn$FromColumn)
  # }
  
  return(outdata)
}

