#' add in all indicators columns from the "TerrADat" layer in a template GDB or a provided list
#' @description Add indicator columns to terradat from a template.
#' @param template String or data frame. A path to a geodatabase containing your template table, or the template table itself. 
#' @param source String. Name of the layer to be read from the geodatabase, if provided. If "AIM" or "TerrADat", layer TerrADat will be read.
#' @param all_indicators Data frame. Indicator data that is missing columns.
#' @param prefixes_to_zero Vector of column prefixes to return 0 for rather than NA. Defaults to "AH", "FH", "NumSpp", and "Spp".
#' @return A data frame containing the data from all_indicators, with added NA or 0 columns.

## add indicator names
#' @rdname add_indicator_columns
#' @export add_indicator_columns
add_indicator_columns <- function(template,
                             source,
                             all_indicators,
                             prefixes_to_zero = c("AH", "FH", "NumSpp", "Spp")){
  
  # template can either be a list of column names to add if not present, or a path to a geodatabase
  if(length(template) == 1) {
    feature_class_field_names <- 
      sf::st_read(template, 
                layer = dplyr::if_else(source %in% c("AIM", "TerrADat", "DIMA"), "TerrADat", source))
    feature_class_field_names <- 
      feature_class_field_names[,!colnames(feature_class_field_names) %in% 
                                  c("created_user", "created_date", 
                                    "last_edited_user", "last_edited_date")] %>%
      names()
  } else {
    feature_class_field_names <- template
  }
   
  # turn the column names of template into a dataframe
  # this will go into a join, marking which field names were provided in template
  indicator_field_names <- 
    data.frame(name = names(all_indicators), 
               calculated = "yes")
  missing_names <- 
    data.frame(name = feature_class_field_names,
               feature.class = "yes") %>% 
    # Join feature class field names to indicator field names
    dplyr::full_join(indicator_field_names) %>% 
    # get the field names where there is not corrollary in calculated
    subset(is.na(calculated), select = "name") %>% 
    dplyr::mutate(value = NA) %>% 
    # make into a data frame
    tidyr::spread(key = name, value = value) %>% 
    dplyr::select_if(!(names(.) %in% c("Shape", "GlobalID")))
  
  # Add a row for each PrimaryKey inall_indicators
  missing_names[nrow(all_indicators), ] <- NA
  
  # For some indicators, the null value is 0 (to indicate the method was completed,
  # but no data in that group were collected)
  regexprefix <- paste0("^", paste(prefixes_to_zero, collapse = "_|^"), "_")
  missing_names[, grepl(names(missing_names), pattern = regexprefix)] <- 0
  
  # Merge back to indicator data to create a feature class for export
  final_feature_class <- dplyr::bind_cols(all_indicators, 
                                          missing_names)
  
  return(final_feature_class)
}
