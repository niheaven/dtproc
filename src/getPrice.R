#
#   dbproc: CAIHUI DataBase Processing 
#
#   Copyright (C) 2016  Hsiao-nan Cheung zxn@hffunds.cn
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
#   getPrice: Get Stocks Price (Adjusted or Unadjusted) (Wrapper)

source(".getPrice.R")

getPrice <- function (symbol, channel, end, window = "1 year", 
	period = "days", type = "price", adjust = TRUE) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	window <- as.list(strsplit(window, " ")[[1]])
	window[[1]] <- as.numeric(window[[1]])
	if ((length(window) != 2) || !is.character(window[[2]]) || !is.numeric(window[[1]])) 
		stop("Parameter window must be as \"1 day\", \"3 weeks\", etc.")
	window[[2]] <- match.arg(window[[2]], c("days", "weeks", "months", "quarters", "years"))
	start <- switch(window[[2]], 
		days = end + 1 - window[[1]], 
		weeks = end + 1 - window[[1]] * 7, 
		months = as.Date(as.yearmon(end) - window[[1]] / 12 + 1 / 12), 
		quarters = as.Date(as.yearmon(end) - window[[1]] / 4 + 1 / 12), 
		years = as.Date(as.yearmon(end) - window[[1]] + 1 / 12))
	start <- format(start, "%Y%m%d")
	end <- format(end, "%Y%m%d")
	ohlc <- .getPrice(symbol, channel, start, end, 
		period, type, adjust) 
	ohlc
}