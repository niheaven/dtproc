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
#   calcFactors: Calculate Stock Factors (Wrapper)

source(".calcFactors.R")
source(".calcFactors_.R")

calcFactors <- function (symbol, channel, end, period = "1 year", 
	vars = "EPS", type = "LYR")
{
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	period <- as.list(strsplit(period, " ")[[1]])
	period[[1]] <- as.numeric(period[[1]])
	if ((length(period) != 2) || !is.character(period[[2]]) || !is.numeric(period[[1]])) 
		stop("Parameter period must be as \"1 day\", \"3 weeks\", etc.")
	period[[2]] <- match.arg(period[[2]], c("days", "weeks", "months", "quarters", "years"))
	start <- switch(period[[2]], 
		days = end + 1 - period[[1]], 
		weeks = end + 1 - period[[1]] * 7, 
		months = as.Date(as.yearmon(end) - period[[1]] / 12), 
		quarters = as.Date(as.yearmon(end) - period[[1]] / 4), 
		years = as.Date(as.yearmon(end) - period[[1]]))
	start <- format(start, "%Y%m%d")
	end <- format(end, "%Y%m%d")
	
}

calcFactors_ <- function (symbol.list, channel, end) {
	symbol.name <- getName(symbol.list, channel)
	facs <- .calcFactors_(symbol.list[1], channel, end)
	cat("(", 1, "/", NROW(symbol.list), ") ", fixCode(symbol.list[1]), " ", symbol.name[1], "\n", sep = "")
	for (i in 2:NROW(symbol.list)) {
		facs <- rbind(facs, .calcFactors_(symbol.list[i], channel, end))
		cat("(", i, "/", NROW(symbol.list), ") ", fixCode(symbol.list[i]), " ", symbol.name[i], "\n", sep = "")
	}
	as.data.frame(facs, row.names = symbol.list)
}


.calcFactors_ <- function (symbol, channel, end) {
	val <- tryCatch(.calcValue_(symbol, channel, end), error = function(e) {cat(format(Sys.time()), fixCode(symbol), "Value Calc Error!\n", file = "factorErrors.log", append = TRUE); matrix(, nrow = 1, ncol = 16)})
	gro <- tryCatch(.calcGrowth_(symbol, channel, end), error = function(e) {cat(format(Sys.time()), fixCode(symbol), "Growth Calc Error!\n", file = "factorErrors.log", append = TRUE); matrix(, nrow = 1, ncol = 12)})
	qua <- tryCatch(.calcQuality_(symbol, channel, end), error = function(e) {cat(format(Sys.time()), fixCode(symbol), "Quality Calc Error!\n", file = "factorErrors.log", append = TRUE); matrix(, nrow = 1, ncol = 8)})
	mom <- tryCatch(.calcMomentum_(symbol, channel, end), error = function(e) {cat(format(Sys.time()), fixCode(symbol), "Momentem Calc Error!\n", file = "factorErrors.log", append = TRUE); matrix(, nrow = 1, ncol = 5)})
	tech <- tryCatch(.calcTech_(symbol, channel, end), error = function(e) {cat(format(Sys.time()), fixCode(symbol), "Tech Calc Error!\n", file = "factorErrors.log", append = TRUE); matrix(, nrow = 1, ncol = 11)})
	fac <- merge(val, gro, qua, mom, tech)
	colnames(fac) <- c("DividendYield_FY0", "DividendYield_FY1", "Price2EPS_LYR", 
		"EP_LYR", "EP_TTM", "EP_Fwd12M", "EP_FY0", "EP_FY1", "SP_TTM",
		"CashFlowYield_LYR", "CashFlowYield_FY0", "CashFlowYield_TTM", 
		"FreeCashFlowYield_TTM", "BP_LR", "BP_FY0_Median", "Sales2EV", 
		"SaleEarnings_SQ_YoY", "Earnings_SQ_YoY", "Sales_SQ_YoY", 
		"Earnings_LTG", "Sales_LTG", "Earnings_STG", "Sales_STG", "Earnings_LFG", 
		"Sales_LFG", "Earnings_SFG", "Sales_SFG", "Asset_STG", "ROE_LR", "ROA_LR", 
		"GrossMargin_TTM", "LTD2Equity_LR", "BerryRatio", "AssetTurnover", 
		"CurrentRatio", "EPS_FY0_Dispersion", "Momentum_1M", "Momentum_3M", "Momentum_12M", 
		"Momentum_12M_1M", "Momentum_60M", "LnFloatCap", "AmountAvg_1M", "NormalizedAbormalVolume", 
		"TurnoverAvg_1M", "TurnoverAvg_3M", "TurnoverAvg_1M_3M", "TSKEW", "ILLIQ", 
		"SmallTradeFlow", "MACrossover", "RealizedVolatility_1Y")
	xts(fac, as.Date(as.character(end), "%Y%m%d"))
}