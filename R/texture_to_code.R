#' Convert NRCS texture classes/modifiers to codes
#'
#' @description Replace NRCS textures or texture modifiers with codes.
#' @param tex Vector of character strings
#' @param keep_unrecognized True/False, default to False. If True, unrecognized 
#' strings will be retained. If False, unrecognized entries will be replaced 
#' with NA_character.
#' @return A vector of the original length with identified entries changed to 
#' codes.
#' @examples 
#' texture_strings <- c("sandy loam", "CLAY", "lerm")
#' texmod_strings <- c("gravelly", "STONY", "wet")
#'
#' texture_to_code(texture_strings)
#' texture_to_code(texture_strings, keep_unrecognized = T)
#' 
#' texmod_to_code(texmod_strings)
#' texmod_to_code(texmod_strings, keep_unrecognized = T)

## 
#' @export texmod_to_code
#' @rdname texture_to_code
texmod_to_code <- function(tex, keep_unrecognized = F){
  texlower <- tolower(tex)
  out <- dplyr::case_when(
    texlower == "gravelly" ~ "GR",
    texlower == "fine gravelly" ~ "FGR",
    texlower == "medium gravelly" ~ "MGR",
    texlower == "coarse gravelly" ~ "CGR",
    texlower == "very gravelly" ~ "VGR",
    texlower == "extremely gravelly" ~ "XGR",
    texlower == "cobbly" ~ "CB",
    texlower == "very cobbly" ~ "VCB",
    texlower == "extremely cobbly" ~ "XCB",
    texlower == "stony" ~ "ST",
    texlower == "very stony" ~ "VST",
    texlower == "extremely stony" ~ "XST",
    texlower == "bouldery" ~ "BY",
    texlower == "extremely bouldery" ~ "XBY",
    texlower == "channery" ~ "CN",
    texlower == "very channery" ~ "VCN",
    texlower == "extremely channery" ~ "XCN",
    texlower == "flaggy" ~ "FL",
    texlower == "very flaggy" ~ "VFL",
    texlower == "extremely flaggy" ~ "XFL",
    keep_unrecognized ~ tex)
  return(out)
}

#' @export texture_to_code
#' @rdname texture_to_code
texture_to_code <- function(tex, keep_unrecognized = F){
  texlower <- tolower(tex)
  out <- dplyr::case_when(
    texlower == "coarse sand" ~ "COS",
    texlower == "sand" ~ "S",
    texlower == "fine sand" ~ "FS",
    texlower == "very fine sand" ~ "VFS",
    texlower == "loamy coarse sand" ~ "VCOS",
    texlower == "loamy sand" ~ "LS",
    texlower == "loamy fine sand" ~ "LFS",
    texlower == "sandy clay loam" ~ "SCL",
    texlower == "fine sandy loam" ~ "FSL",
    texlower == "silty clay" ~ "SIC",
    texlower == "loamy very fine sand" ~ "LVFS",
    texlower == "clay loam" ~ "CL",
    texlower == "very fine sandy loam" ~ "VFSL",
    texlower == "silt loam" ~ "SIL",
    texlower == "coarse sandy loam" ~ "COSL",
    texlower == "silty clay loam" ~ "SICL",
    texlower == "loam" ~ "L",
    texlower == "silt" ~ "SI",
    texlower == "sandy loam" ~ "SL",
    texlower == "sandy clay" ~ "SC",
    texlower == "clay" ~ "C",
    keep_unrecognized ~ tex)
  return(out)
}
