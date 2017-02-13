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

# Global Variable
TABLE.NAME <- "FactorsTest"

# Load calcFactors
source("calcFactors.R")

# Calculate and Write Factors to Dababase
cwFactors_ <- function (symbol.list, channel, end) {
	if (nchar(symbol.list[1]) == 9) {
		symbol.list.f <- symbol.list
		symbol.list <- sapply(symbol.list, strtrim, 6)
	}
	else {
		symbol.list.f <- fixCode(symbol.list)
	}
	symbol.name <- getName(symbol.list, channel, end)
	facs <- calcFactors_(symbol.list, channel, end)
	facs.df <- data.frame(Date = end, Symbol = symbol.list.f, facs)
	if (dbWriteTable(channel, TABLE.NAME, facs.df, row.names = FALSE, append = TRUE))
		cat(end, "All Factors is writen into database.\n", file = paste0("log/", end, ".log"), append = TRUE)
	else
		cat(end, "All Factors is writen into database.\n", file = paste0("log/", end, ".log"), append = TRUE)
	}
}

# Write Factors for One Year Data
.writeFactorsForOneYear <- function (factors, channel) {
	tableName <- year(index(factors))
}