#' Remove observer and recorder from a table
#'
#' @description Given a table, return it without observer, recorder, data enterer, or data checker
#' @param data The table to edit
#' @return A table without observer or recorder

#' @rdname forget_who
#' @export forget_who
forget_who <- function(data){
  out <- data %>% 
    dplyr::select_if(!(names(.) %in% c("Observer", "Recorder", "DataEntry", "DataErrorChecking")))
  return(out)
}
