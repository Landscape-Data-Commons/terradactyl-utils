require(foreach)
require(doParallel)
require(parallel)

readchunks <- function(d, path_indata){
  lapply(d,
         function(d) {
           suppressWarnings(
             sf::st_read(path_indata, "tblLPIDetail",
                         query = paste0("SELECT * FROM tblLPIDetail WHERE DBKey = '", d, "'")))
         }) %>%
    dplyr::bind_rows()
}


llLPI <- function(d, n, path_indata){
  cl <- makeCluster(spec = n, # number of cores to use
                    type = "PSOCK",
                    methods = FALSE)
  registerDoParallel(cl)

  out <- 
    foreach(x = d, 
            path_indata = path_indata,
            .packages = c("sf"),
            .combine = "rbind" 
    ) %dopar% {
      suppressWarnings(
        sf::st_read(path_indata, "tblLPIDetail",
                    query = paste0("SELECT * FROM tblLPIDetail WHERE DBKey = '", x, "'")))
    }
  
  registerDoSEQ()
  stopCluster(cl)
  
  return(out)
}
