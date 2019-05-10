
test <- function(path) {

return(path)

}


count_words <- function(path) {
	library(tau)
	library(readr)

	text=read_file(path)
	counts=textcnt(text,n=1,method='string',tolower=T)

	return(counts)

}
