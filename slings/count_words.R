library(tau)
library(readr)

count_words <- function(path) { 
text=read_file(path)
counts=textcnt(text,n=1,method='string',tolower=T)
return(counts)
}