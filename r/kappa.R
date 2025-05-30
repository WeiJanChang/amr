# install.packages('openxlsx')
# install.packages('irr')
library(openxlsx)
library(dplyr)
library(irr) # package for Various Coefficients of Interrater Reliability and Agreement
path <-'~/code/amr/test_file/coders_messages.xlsx'
df <- read.xlsx(path)
# Pre-processing: filter NA

#select specific cols
coder1_2 <- df %>% select('coder1','coder2')  %>% filter(if_any(everything(), ~ !is.na(.))) # coder1 and 2 do not overlapping
coder3_4 <- df %>% select('coder3','coder4') %>% filter(if_any(everything(), ~ !is.na(.)))
coder1_3 <- df %>% select('coder1','coder3') %>% filter(if_any(everything(), ~ !is.na(.)))
coder1_4 <- df %>% select('coder1','coder4') %>% filter(if_any(everything(), ~ !is.na(.)))
coder2_3 <- df %>% select('coder2','coder3') %>% filter(if_any(everything(), ~ !is.na(.)))
coder2_4 <- df %>% select('coder2','coder4') %>% filter(if_any(everything(), ~ !is.na(.)))

# Cohen's Kappa 
kappa2(coder3_4)
kappa2(coder1_3)
kappa2(coder1_4)
kappa2(coder2_3)
kappa2(coder2_4)