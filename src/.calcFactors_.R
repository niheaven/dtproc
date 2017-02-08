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
#   .calcFactors0: Calculate Stock Factors One Period (Implementation)

# Load Supplementary Functions
source(".SupFun.R")

# Calculate Value Factors
.calcValue_ <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
#	start <- paste0(as.numeric(strtrim(format(end, "%Y%m%d"), 4)) - 1, "01", "01")
	start <- firstof(as.numeric(strtrim(format(end, "%Y%m%d"), 4)) - 2)
	val.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, DILUTEDEPS, PARENETP, BIZINCO FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' AND DECLAREDATE > '", format(start, "%Y%m%d"), "'ORDER BY DECLAREDATE")
	val.inc.d <- dbGetQuery(channel, val.inc.q)
	val.inc.d <- val.inc.d[, -1]
	val.inc.d0 <- xts(val.inc.d[val.inc.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(val.inc.d[val.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(val.inc.d[val.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	val.inc.d <- xts(val.inc.d[val.inc.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(val.inc.d[val.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(val.inc.d[val.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	val.inc.d <- merge(val.inc.d, xts(, index(val.inc.d0)))
	val.inc.d[index(val.inc.d0), ] <- val.inc.d0
	val.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARESHARRIGH, TOTALNONCLIAB, CURFDS FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' AND DECLAREDATE > '", format(start, "%Y%m%d"), "'ORDER BY DECLAREDATE")
	val.bal.d <- dbGetQuery(channel, val.bal.q)
	val.bal.d <- val.bal.d[, -1]
	val.bal.d0 <- xts(val.bal.d[val.bal.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(val.bal.d[val.bal.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(val.bal.d[val.bal.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	val.bal.d <- xts(val.bal.d[val.bal.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(val.bal.d[val.bal.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(val.bal.d[val.bal.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	val.bal.d <- merge(val.bal.d, xts(, index(val.bal.d0)))
	val.bal.d[index(val.bal.d0), ] <- val.bal.d0
	val.cf.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, MANANETR, ACQUASSETCASH FROM TQ_FIN_PROCFSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' AND DECLAREDATE > '", format(start, "%Y%m%d"), "'ORDER BY DECLAREDATE")
	val.cf.d <- dbGetQuery(channel, val.cf.q)
	val.cf.d <- val.cf.d[, -1]
	val.cf.d0 <- xts(val.cf.d[val.cf.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(val.cf.d[val.cf.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(val.cf.d[val.cf.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	val.cf.d <- xts(val.cf.d[val.cf.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(val.cf.d[val.cf.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(val.cf.d[val.cf.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	val.cf.d <- merge(val.cf.d, xts(, index(val.cf.d0)))
	val.cf.d[index(val.cf.d0), ] <- val.cf.d0
	val.rep.d <- merge(val.inc.d, val.bal.d, val.cf.d)
	val.rep.d <- val.rep.d[paste0(start, "/"), ]
	val.rep.d <- .report.na.fill(val.rep.d, seasonal = c(rep(TRUE, 3), rep(FALSE, 3), rep(TRUE, 2)))
	val.rep.ttm <- .report.calc.ttm(val.rep.d[, c("PARENETP", "BIZINCO", "MANANETR", "ACQUASSETCASH")])
	val.rep.lyr <- .report.get.lyr(val.rep.d[, c("DILUTEDEPS", "PARENETP", "MANANETR")])
	val.rep.lr <- last(val.rep.d[, c("PARESHARRIGH", "TOTALNONCLIAB", "CURFDS")])
	val.p.q <- paste0("SELECT TRADEDATE, LCLOSE, TCLOSE, TOTMKTCAP FROM TQ_QT_SKDAILYPRICE 
		WHERE SECODE = (SELECT SECODE FROM TQ_OA_STCODE 
			WHERE SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' AND TRADEDATE> '", 
		format(end - 14, "%Y%m%d"), "' ORDER BY TRADEDATE")
	val.p.d <- dbGetQuery(channel, val.p.q)
	val.p.d <- xts(val.p.d[, -1], as.Date(as.character(val.p.d[, 1]), "%Y%m%d"))
 	val.p <- vector()
	val.p["TCLOSE"] <- last(ifelse(val.p.d[, "TCLOSE"] == 0, val.p.d[, "LCLOSE"], val.p.d[, "TCLOSE"]))
	val.p["TOTMKTCAP"] <- last(val.p.d[, "TOTMKTCAP"]) * 10 ^ 4
	val <- vector()
	val["DividendYield_FY0"] <- NA / val.p["TCLOSE"]
	val["DividendYield_FY1"] <- NA / val.p["TCLOSE"]
	val["Price2EPS_LYR"] <- val.p["TCLOSE"] / val.rep.lyr[, "DILUTEDEPS"]
	val["EP_LYR"] <- val.rep.lyr[, "PARENETP"] / val.p["TOTMKTCAP"]
	val["EP_TTM"] <- val.rep.ttm["PARENETP"] / val.p["TOTMKTCAP"]
	val["EP_Fwd12M"] <- NA / val.p["TOTMKTCAP"]
	val["EP_FY0"] <- NA / val.p["TOTMKTCAP"]
	val["EP_FY1"] <- NA / val.p["TOTMKTCAP"]
	val["SP_TTM"] <- val.rep.ttm["BIZINCO"] / val.p["TOTMKTCAP"]
	val["CashFlowYield_LYR"] <- val.rep.lyr[, "MANANETR"] / val.p["TOTMKTCAP"]
	val["CashFlowYield_FY0"] <- NA / val.p["TCLOSE"]
	val["CashFlowYield_TTM"] <- val.rep.ttm["MANANETR"] / val.p["TOTMKTCAP"]
	val["FreeCashFlowYield_TTM"] <- (val.rep.ttm["MANANETR"] - val.rep.ttm["ACQUASSETCASH"]) / 
		val.p["TOTMKTCAP"]
	val["BP_LR"] <- val.rep.lr[, "PARESHARRIGH"] / val.p["TOTMKTCAP"]
	val["BP_FY0_Median"] <- NA / val.p["TCLOSE"]
	val["Sales2EV"] <- val.rep.ttm["BIZINCO"] / 
		(val.p["TOTMKTCAP"] + val.rep.lr[, "TOTALNONCLIAB"] - val.rep.lr[, "CURFDS"])
	xts(t(unlist(val)), end)
}

# Calculate Growth Factors
.calcGrowth_ <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	gro.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PERPROFIT, NETPROFIT, BIZINCO FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"'ORDER BY DECLAREDATE")
	gro.inc.d <- dbGetQuery(channel, gro.inc.q)
	gro.inc.d <- gro.inc.d[, -1]
	gro.inc.d0 <- xts(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	gro.inc.d <- xts(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	gro.inc.d <- merge(gro.inc.d, xts(, index(gro.inc.d0)))
	gro.inc.d[index(gro.inc.d0), ] <- gro.inc.d0
	gro.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, TOTASSET FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"'ORDER BY DECLAREDATE")
	gro.bal.d <- dbGetQuery(channel, gro.bal.q)
	gro.bal.d <- gro.bal.d[, -1]
	gro.bal.d0 <- xts(gro.bal.d[gro.bal.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	gro.bal.d <- xts(gro.bal.d[gro.bal.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	gro.bal.d <- merge(gro.bal.d, xts(, index(gro.bal.d0)))
	gro.bal.d[index(gro.bal.d0), ] <- gro.bal.d0
	names(gro.bal.d) <- "TOTASSET"
	gro.d <- merge(gro.inc.d, gro.bal.d)
	gro.d <- .report.na.fill(gro.d, seasonal = c(rep(TRUE, 3), FALSE))
	gro.d.us <- .report.unseasonal(gro.d, seasonal = c(rep(TRUE, 3), FALSE))
	gro <- vector()
	gro["SaleEarnings_SQ_YoY"] <- last(gro.d.us)[, "PERPROFIT"][[1]] / 
		gro.d.us[index(last(gro.d.us)) - 1, "PERPROFIT"][[1]] - 1
	gro["Earnings_SQ_YoY"] <- last(gro.d.us)[, "NETPROFIT"][[1]] / 
		gro.d.us[index(last(gro.d.us)) - 1, "NETPROFIT"][[1]] - 1
	gro["Sales_SQ_YoY"] <- last(gro.d.us)[, "BIZINCO"][[1]] / 
		gro.d.us[index(last(gro.d.us)) - 1, "BIZINCO"][[1]] - 1
	gro["Earnings_LTG"] <- ifelse((index(last(gro.d)) - 5) < index(gro.d[1]), 
		NA, last(gro.d)[, "NETPROFIT"][[1]] / 
		gro.d[index(last(gro.d)) - 5, "NETPROFIT"][[1]] - 1)
	gro["Sales_LTG"] <- ifelse((index(last(gro.d)) - 5) < index(gro.d[1]), 
		NA, last(gro.d)[, "BIZINCO"][[1]] / 
		gro.d[index(last(gro.d)) - 5, "BIZINCO"][[1]] - 1)
	gro["Earnings_STG"] <- last(gro.d)[, "NETPROFIT"][[1]] / 
		gro.d[index(last(gro.d)) - 1, "NETPROFIT"][[1]] - 1
	gro["Sales_STG"] <- last(gro.d)[, "BIZINCO"][[1]] / 
		gro.d[index(last(gro.d)) - 1, "BIZINCO"][[1]] - 1
	gro["Earnings_LFG"] <- NA
	gro["Sales_LFG"] <- NA
	gro["Earnings_SFG"] <- NA
	gro["Sales_SFG"] <- NA
	gro["Asset_STG"] <- last(gro.d)[, "TOTASSET"][[1]] / 
		gro.d[index(last(gro.d)) - 1, "TOTASSET"][[1]] - 1
	xts(t(gro), end)
}

# Calculate Quality Factors
.calcQuality_ <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
#	start <- paste0(as.numeric(strtrim(format(end, "%Y%m%d"), 4)) - 1, "01", "01")
	start <- firstof(as.numeric(strtrim(format(end, "%Y%m%d"), 4)) - 2)
	qua.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARENETP, BIZINCO, BIZCOST, SALESEXPE FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' AND DECLAREDATE > '", format(start, "%Y%m%d"), "'ORDER BY DECLAREDATE")
	qua.inc.d <- dbGetQuery(channel, qua.inc.q)
	qua.inc.d <- qua.inc.d[, -1]
	qua.inc.d0 <- xts(qua.inc.d[qua.inc.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	qua.inc.d <- xts(qua.inc.d[qua.inc.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	qua.inc.d <- merge(qua.inc.d, xts(, index(qua.inc.d0)))
	qua.inc.d[index(qua.inc.d0), ] <- qua.inc.d0
	qua.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARESHARRIGH, TOTASSET, TOTCURRASSET, 
		TOTALCURRLIAB, TOTALNONCLIAB FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' AND DECLAREDATE > '", format(start, "%Y%m%d"), "'ORDER BY DECLAREDATE")
	qua.bal.d <- dbGetQuery(channel, qua.bal.q)
	qua.bal.d <- qua.bal.d[, -1]
	qua.bal.d0 <- xts(qua.bal.d[qua.bal.d["REPORTTYPE"] == 3, -1:-3], 
		as.yearqtr(paste0(t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
		"-", t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
	qua.bal.d <- xts(qua.bal.d[qua.bal.d["REPORTTYPE"] == 1, -1:-3], 
		as.yearqtr(paste0(t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
		"-", t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
	qua.bal.d <- merge(qua.bal.d, xts(, index(qua.bal.d0)))
	qua.bal.d[index(qua.bal.d0), ] <- qua.bal.d0
	qua.rep.d <- merge(qua.inc.d, qua.bal.d)
	qua.rep.d <- qua.rep.d[paste0(start, "/"), ]
	qua.rep.d <- .report.na.fill(qua.rep.d, seasonal = c(rep(TRUE, 4), rep(FALSE, 5)))
	qua.rep.ttm <- .report.calc.ttm(qua.rep.d[, c("PARENETP", "BIZINCO", "BIZCOST", "SALESEXPE")])
	qua.rep.lr <- last(qua.rep.d[, c("PARESHARRIGH", "TOTASSET", 
		"TOTCURRASSET", "TOTALCURRLIAB", "TOTALNONCLIAB")])
	qua <- vector()
	qua["ROE_LR"] <- qua.rep.ttm["PARENETP"] / qua.rep.lr[, "PARESHARRIGH"]
	qua["ROA_LR"] <- qua.rep.ttm["PARENETP"] / qua.rep.lr[, "TOTASSET"]
	qua["GrossMargin_TTM"] <- (qua.rep.ttm["BIZINCO"] - qua.rep.ttm["BIZCOST"]) / 
		qua.rep.ttm["BIZINCO"] - 1
	qua["LTD2Equity_LR"] <- qua.rep.lr[, "TOTALNONCLIAB"] / qua.rep.lr[, "PARESHARRIGH"]
	qua["BerryRatio"] <- (qua.rep.ttm["BIZINCO"] - qua.rep.ttm["BIZCOST"]) / 
		qua.rep.ttm["SALESEXPE"]
	qua["AssetTurnover"] <- qua.rep.ttm["BIZINCO"] / qua.rep.lr[, "TOTASSET"]
	qua["CurrentRatio"] <- qua.rep.lr[, "TOTCURRASSET"] / qua.rep.lr[, "TOTALCURRLIAB"]
	qua["EPS_FY0_Dispersion"] <- NA
	xts(t(unlist(qua)), end)
}

# Calculate Momentum Factors
.calcMomentum_ <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	start <- as.Date(as.yearmon(end) - 60 / 12)
	mom.q <- paste0("SELECT TRADEDATE, TCLOSEAF 
		FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", as.character(symbol), "' 
		AND TRADEDATE >= '", format(start, "%Y%m%d"), "' 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	mom.d <- dbGetQuery(channel, mom.q)
	mom.d <- xts(mom.d[, -1], as.Date(as.character(mom.d[, 1]), "%Y%m%d"))
	mom <- vector()
	mom["Momentum_1M"] <- last(mom.d)[[1]] / last(mom.d[endpoints(mom.d)], 2)[1][[1]] - 1
	mom["Momentum_3M"] <- last(mom.d)[[1]] / last(mom.d[endpoints(mom.d)], 4)[1][[1]] - 1
	mom["Momentum_12M"] <- last(mom.d)[[1]] / last(mom.d[endpoints(mom.d)], 13)[1][[1]] - 1
	mom["Momentum_12M_1M"] <- mom["Momentum_12M"] - mom["Momentum_1M"]
	mom["Momentum_60M"] <- last(mom.d)[[1]] / mom.d[endpoints(mom.d)][1][[1]] - 1
	xts(t(mom), end)
}

# Calculate Technical Factors
.calcTech_ <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	start <- as.Date(as.yearmon(end) - 11 / 12)
	tech.q <- paste0("SELECT TRADEDATE, EXTCLOSE, TCLOSE, TCLOSEAF, AMOUNT, VOL, MKTSHARE 
		FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", as.character(symbol), "' 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	tech.d <- dbGetQuery(channel, tech.q)
	tech.d <- xts(tech.d[, -1], as.Date(as.character(tech.d[, 1]), "%Y%m%d"))
	tech.d.names <- names(tech.d)
	tech.d <- cbind(tech.d, .EMA(tech.d[, "TCLOSEAF"], 75), .EMA(tech.d[, "TCLOSEAF"], 180))
	names(tech.d) <- c(tech.d.names, "EMA75", "EMA180")
	tech.d <- tech.d[index(tech.d) >= start]
	tech.d[tech.d[, "TCLOSE"] == 0, "TCLOSE"] <- tech.d[tech.d[, "TCLOSE"] == 0, "EXTCLOSE"]
	tech.d <- cbind(tech.d, tech.d[, "VOL"] / 10^4 / tech.d[, "MKTSHARE"], 
		tech.d[, "TCLOSE"] / tech.d[, "EXTCLOSE"] - 1)
	tech <- vector()
	tech["LnFloatCap"] <- log(last(tech.d)[, "TCLOSE"] * last(tech.d)[, "MKTSHARE"] * 10^4)
	tech["AmountAvg_1M"] <- mean(tech.d[index(tech.d) >= as.Date(as.yearmon(end)), 
		"AMOUNT"], na.rm = TRUE)
	tech["NormalizedAbormalVolume"] <- mean(tech.d[index(tech.d) >= as.Date(as.yearmon(end)), 
		"VOL"], na.rm = TRUE) / mean(tech.d[, "VOL"], na.rm = TRUE)
	tech["TurnoverAvg_1M"] <- mean(tech.d[index(tech.d) >= as.Date(as.yearmon(end)), 
		"VOL.1"], na.rm = TRUE)
	tech["TurnoverAvg_3M"] <- mean(tech.d[index(tech.d) >= as.Date(as.yearmon(end) - 2 / 12), 
		"VOL.1"], na.rm = TRUE)
	tech["TurnoverAvg_1M_3M"] <- tech["TurnoverAvg_1M"] / tech["TurnoverAvg_3M"]
	tech["TSKEW"] <- moments::skewness(tech.d[, "TCLOSE.1"])
	tech["ILLIQ"] <- mean((tech.d[, "TCLOSE"] - tech.d[, "EXTCLOSE"]) / tech.d[, "AMOUNT"], na.rm = TRUE)
	tech["SmallTradeFlow"] <- NA
	tech["MACrossover"] <- (last(tech.d)[, "EMA75"] - last(tech.d)[, "EMA180"]) / last(tech.d)[, "EMA180"]
	tech["RealizedVolatility_1Y"] <- sd(tech.d[, "TCLOSE.1"])
	xts(t(tech), end)
}