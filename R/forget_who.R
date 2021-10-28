#' Remove observer, recorder, and similar from a table
#'
#' @description Given a table, return it without names of data collectors
#' @param data The table to edit
#' @return A table without names

#' @rdname forget_who
#' @export forget_who
forget_who <- function(data){
  out <- data %>% 
    dplyr::select_if(!(names(.) %in% c("Observer", "Recorder", "DataEntry", "DataErrorChecking",
                                       "ESD_Investigators")))
  return(out)
}
