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
  verbose){
  # matrix <- submatrix
  # data <- ldcdata
  # from <- "Column2"
  # to <- "Column1"
  # verbose = T
  
  ### could not figure out the dplyr::rename version of this
  colnames(matrix)[colnames(matrix) == fromcol] <- "FromColumn"
  colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"
  
  matrix_processed <- 
    matrix %>% 
    dplyr::select(FromColumn, ToColumn) %>%
    dplyr::mutate(MissingCol = is.na(ToColumn) & !is.na(FromColumn)) %>%
    dplyr::mutate(ToColumn = dplyr::coalesce(ToColumn, FromColumn)) %>%
    dplyr::mutate(ToChange = ToColumn != FromColumn)
  
  tochange <- 
    matrix_processed %>% 
    dplyr::filter(ToChange)
  
  toadd <- 
    matrix_processed %>%
    dplyr::filter(MissingCol)
  
  if(verbose) {
    print(paste0(nrow(tochange), " columns to rename"))
    print(tochange[,c("FromColumn", "ToColumn")])}
  
  if(verbose) {
    print(paste0(nrow(toadd), " columns to add to output"))
    print(toadd$ToColumn)}
  
  ## run translation and add data
  outdata <- data %>%
    dplyr::rename_at(
      tochange$FromColumn, ~ tochange$ToColumn) %>%
    `is.na<-`(toadd$ToColumn)
  
  return(outdata)
}