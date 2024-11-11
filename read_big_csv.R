pacman::p_load(tidyverse, fastverse, bench, tictoc, vroom)


path1 <- "data/nr_2023_1.csv"
path2 <- "data/nr_2023_3.csv"
# tic()
# raw <- fread(path2)
# toc()

raw = vroom(path2) |> 
  filter(between(latitude, 51, 52))
rm(raw)
gc()
