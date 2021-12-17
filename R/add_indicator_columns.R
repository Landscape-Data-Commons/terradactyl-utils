#' add in all indicators columns from the "TerrADat" layer in a template GDB
#' @description Add indicator columns to terradat from a template

## add indicator names
#' @rdname add_indicator_columns
#' @export add_indicator_columns
add_indicator_columns <- function(dsn_template,
                             source,
                             all_indicators){
  feature_class_field_names <- 
    sf::st_read(dsn_template, 
                layer = dplyr::if_else(source %in% c("AIM", "TerrADat"), "TerrADat", source))
  feature_class_field_names <- 
    feature_class_field_names[,!colnames(feature_class_field_names) %in% 
                                c("created_user", "created_date", 
                                  "last_edited_user", "last_edited_date")]
  indicator_field_names <- 
    data.frame(name = names(all_indicators), 
               calculated = "yes")
  missing_names <- 
    data.frame(name = names(feature_class_field_names), 
               feature.class = "yes") %>% 
    dplyr::full_join(indicator_field_names) %>% 
    subset(is.na(calculated), select = "name") %>% 
    dplyr::mutate(value = NA) %>% 
    tidyr::spread(key = name, value = value) %>% 
    dplyr::select(-Shape, 
                  -GlobalID)
  missing_names[nrow(all_indicators), ] <- NA
  missing_names[, grepl(names(missing_names), pattern = "^FH|^AH|^Num")] <- 0
  final_feature_class <- dplyr::bind_cols(all_indicators, 
                                          missing_names)
  
  return(final_feature_class)
}
