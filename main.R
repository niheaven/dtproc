#
#   dtproc: Data Processing Based on CAIHUI Database
#
#   Copyright (C) 2016-2017  Hsiao-nan Cheung zxn@hffunds.cn
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#   main: Main Source File

if(!require(DBI))
	install.packages("DBI")
if(!require(quantmod))
	install.packages("quantmod")
if(!require(lubridate))
	install.packages("lubridate")
if(!requireNamespace("moments"))
	install.packages("moments")


# Table Name for main.*
TABLE.PERIOD <- "FACTORS_PERIOD"
TABLE.STOCK <- "FACTORS_STOCK"

# Next Period for main.period
START <- ymd("19970430")
# Last C/W Stock for main.stock
S.END <- "002545"

# source("getPrice.R", encoding = 'UTF-8')
source("src/.SupFun.R", chdir = TRUE, encoding = 'UTF-8')
source("src/calcFactors.R", chdir = TRUE, encoding = 'UTF-8')
source("src/writeFactors.R", chdir = TRUE, encoding = 'UTF-8')
# source("test/test.R", encoding = "UTF-8")

# For DB Connections
if (!tryCatch(dbIsValid(ch_data), error = function(e) FALSE))
	ch_data <- dbConnect(RSQLServer::SQLServer(), "DBSERVER", file = "dbi/sql.yaml", database = "CAIHUI")
if (!tryCatch(dbIsValid(ch_data_w), error = function(e) FALSE))
	ch_data_w <- dbConnect(RSQLServer::SQLServer(), "DBSERVER", file = "dbi/sql.yaml", database = "WDF")
if (!tryCatch(dbIsValid(ch_factors), error = function(e) FALSE))
	ch_factors <- dbConnect(RMySQL::MySQL(), dbname = "factors", default.file = "dbi/.my.cnf")

# Main Function for One Stock per Routine
main.stock <- function (con_data, con_factors, table.name = TABLE.STOCK, end = "20161231") {
	end <- ymd(end)
	code.a <- dbGetQuery(con_data, paste0("SELECT SYMBOL FROM TQ_OA_STCODE WHERE BEGINDATE <= '", 
		format(end, "%Y%m%d"), "' AND SETYPE = '101' ORDER BY SYMBOL"))
	code.a <- code.a[!duplicated(code.a), 1]
	code.a.f <- fixCode(code.a)
	i.start <- sum(as.numeric(code.a) <= as.numeric(S.END)) + 1
	for (i in i.start:length(code.a)) {
  	# Throw away broken symbols
	  if ((NROW(dbGetQuery(con_data, paste0("SELECT TRADEDATE FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", code.a[i], "' 
		  AND TRADEDATE <= '", format(end, "%Y%m%d"), "'")))) == 0) {
	    cat("Damn!", code.a.f[i], "(", i, "/", length(code.a), ")", "HAS NOT BEEN LISTED! So Skip the Symbol!\n")
	    next
	  }
		cwres <- tryCatch(cwFactors(code.a[i], con_data, con_factors, table.name, end), error = function(e) {cat(format(Sys.time()), code.a[i], "Factors C/W Error!", "\n\t", e$message, "\n", file = "log/factorErrors.log", append = TRUE)})
		if (isTRUE(cwres))
			cat("End Date:", format(end), "(", i, "/", length(code.a), ")", code.a.f[i], "Factors C/W Okay.", format(Sys.time()), "\n", file = paste0("log/all", format(end, "%Y%m%d"), ".log"), append = TRUE)
		else
			cat("End Date:", format(end), "(", i, "/", length(code.a), ")", code.a.f[i], "Factors C/W Error!", format(Sys.time()), "\n", file = paste0("log/all", format(end, "%Y%m%d"), ".log"), append = TRUE)
		cat("Well Done! You've Got", code.a.f[i], "(", i, "/", length(code.a), ")", "Factors Writen to Database!\n")
	}
	cat("CONGUATULATION! FINALLLY ALL DONE!\n")
}

# Main Function for One Period per Routine
main.period <- function (con_data, con_factors, table.name = TABLE.PERIOD, end = "20161231") {
	end <- ymd(end)
	idx.sh <- "000002.SH"
	idx.sz <- "399107.SZ"
	dmon <-  as.yearmon(end) - as.yearmon(START)
	date.list <- START %m+% months(0:(12*dmon))
	for (i in 1:length(date.list)) {
		# For Shanghai's Stocks
		code.sh <- getIndexComp.W(idx.sh, ch_data_w, date.list[i])
		cwres.sh <- tryCatch(cwFactors_(code.sh, con_data, con_factors, table.name, date.list[i]), error = function(e) {cat(format(date.list[i]), "Shanghai's Factors C/W Error!", format(Sys.time()), "\n\t", e$message, "\n", file = "log/factorErrors.log", append = TRUE)})
		if (isTRUE(cwres.sh))
			cat(format(date.list[i]), "########## Shanghai's factors C/W Okay. ##########\n", file = paste0("log/", format(date.list[i], "%Y%m%d"), ".log"), append = TRUE)
		else
			cat(format(date.list[i]), "########## Shanghai's factors C/W Error. #########\n", file = paste0("log/", format(date.list[i], "%Y%m%d"), ".log"), append = TRUE)
		# For Shenzhen's Stocks
		code.sz <- getIndexComp.W(idx.sz, ch_data_w, date.list[i])
		cwres.sz <- tryCatch(cwFactors_(code.sz, con_data, con_factors, table.name, date.list[i]), error = function(e) {cat(format(date.list[i]), "Shenzhen's Factors C/W Error!", format(Sys.time()), "\n\t", e$message, "\n", file = "log/factorErrors.log", append = TRUE)})
		if (isTRUE(cwres.sz))
			cat(format(date.list[i]), "########## Shenzhen's factors C/W Okay. ##########\n", file = paste0("log/", format(date.list[i], "%Y%m%d"), ".log"), append = TRUE)
		else
			cat(format(date.list[i]), "########## Shenzhen's factors C/W Error. #########\n", file = paste0("log/", format(date.list[i], "%Y%m%d"), ".log"), append = TRUE)
		cat("Well Done! You've Got", format(date.list[i]), "(", i, "/", length(date.list), ")", "Factors Writen to Database!\n")
	}
	cat("CONGUATULATION! FINALLLY ALL DONE!\n")
}
