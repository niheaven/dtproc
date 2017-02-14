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


# Global Variable
TABLE.NAME <- "FactorsTest"
START <- ymd("19941231")

# source("getPrice.R", encoding = 'UTF-8')
source("src/writeFactors.R", chdir = TRUE, encoding = 'UTF-8')

if (!tryCatch(dbIsValid(ch_data), error = function(e) FALSE))
	ch_data <- dbConnect(RSQLServer::SQLServer(), "DBSERVER", file = "dbi/sql.yaml", database = "CAIHUI")
if (!tryCatch(dbIsValid(ch_data_w), error = function(e) FALSE))
	ch_data_w <- dbConnect(RSQLServer::SQLServer(), "DBSERVER", file = "dbi/sql.yaml", database = "WDF")
if (!tryCatch(dbIsValid(ch_factors), error = function(e) FALSE))
	ch_factors <- dbConnect(RMySQL::MySQL(), dbname = "factors", default.file = "dbi/.my.cnf")

source("test/test.R", encoding = "UTF-8")