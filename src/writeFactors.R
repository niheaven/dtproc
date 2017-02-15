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
#   writeFactors: Write Stock Factors to Database

# Calculate and Write Factors to Dababase, One Stock and Multiple Periods
cwFactors <- function (symbol, con_data, con_factors, table.name, end) {
	if (missing(end)) {
		end <- rollback(today())
	}
	else {
		end <- ymd(end)
	}
	if (nchar(symbol) == 9) {
		symbol.f <- symbol
		symbol <- strtrim(symbol, 6)
	}
	else {
		symbol.f <- fixCode(symbol)
	}
	facs <- .calcFactors(symbol, con_data, end)
	facs.df <- data.frame(Date = index(facs), Symbol = symbol.f, facs)
	append.trigger <- dbExistsTable(con_factors, table.name)
	dbWriteTable(con_factors, table.name, facs.df, row.names = FALSE, append = append.trigger)
}

# Calculate and Write Factors to Dababase, Multiple Stocks and One Period
cwFactors_ <- function (symbol.list, con_data, con_factors, table.name, date) {
	if (missing(date)) {
		date <- rollback(today())
	}
	else {
		date <- ymd(date)
	}
	if (nchar(symbol.list[1]) == 9) {
		symbol.list.f <- symbol.list
		symbol.list <- sapply(symbol.list, strtrim, 6)
	}
	else {
		symbol.list.f <- fixCode(symbol.list)
	}
	facs <- calcFactors_(symbol.list, con_data, date)
	facs.df <- data.frame(Date = date, Symbol = symbol.list.f, facs)
	append.trigger <- dbExistsTable(con_factors, table.name)
	dbWriteTable(con_factors, table.name, facs.df, row.names = FALSE, append = append.trigger)
}

# Write Factors for One Year Data
#.writeFactorsForOneYear <- function (factors, con_data) {
#	tableName <- year(index(factors))
#}