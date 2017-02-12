#
#   pproc: Data Processing Based on CAIHUI Database
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
#   .calcFactors: Calculate Stock Factors (Implementation)

# Global Variable
START <- ymd("19941231")

# Load Supplementary Functions
source(".SupFun.R")

# Calculate Value Factors
.calcValue <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- rollback(today())
	}
	else {
		end <- ymd(end)
	}
	code <- .getCode(symbol, channel, end)
	val.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, DILUTEDEPS, PARENETP, BIZINCO FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	val.inc.d_ <- dbGetQuery(channel, val.inc.q)
	val.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARESHARRIGH, TOTALNONCLIAB, CURFDS FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	val.bal.d_ <- dbGetQuery(channel, val.bal.q)
	val.cf.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, MANANETR, ACQUASSETCASH FROM TQ_FIN_PROCFSTATEMENTNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	val.cf.d_ <- dbGetQuery(channel, val.cf.q)
	val.p.q <- paste0("SELECT TRADEDATE, LCLOSE, TCLOSE, TOTMKTCAP FROM TQ_QT_SKDAILYPRICE 
		WHERE SECODE = '", code[2], "' AND TRADEDATE <= '", 
		format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	val.p.d_ <- dbGetQuery(channel, val.p.q)
	val.p.d_ <- xts(val.p.d_[, -1], ymd(val.p.d_[, 1]))
	val.ep <- endpoints(val.p.d_)
	val.idx <- last.day(index(val.p.d_)[val.ep])
	val.idx <- val.idx[val.idx >= START]
	val <- xts(matrix(, nrow = length(val.idx), ncol = 16), val.idx)
	for (i in 1:length(val.idx)) {
		end.i <- val.idx[i]
		start.i <- firstof(year(end.i) - 2)
		val.inc.d <- val.inc.d_[ymd(val.inc.d_[, 1]) <= end.i, -1]
		if (NROW(val.inc.d) == 0) next
		val.bal.d <- val.bal.d_[ymd(val.bal.d_[, 1]) <= end.i, -1]
		if (NROW(val.bal.d) == 0) next
		val.cf.d <- val.cf.d_[ymd(val.cf.d_[, 1]) <= end.i, -1]
		if (NROW(val.cf.d) == 0) next
		val.inc.d0 <- xts(val.inc.d[val.inc.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(val.inc.d[val.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(val.inc.d[val.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		val.inc.d <- xts(val.inc.d[val.inc.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(val.inc.d[val.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(val.inc.d[val.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		val.inc.d <- merge(val.inc.d, xts(, index(val.inc.d0)))
		val.inc.d[index(val.inc.d0), ] <- val.inc.d0
		val.bal.d0 <- xts(val.bal.d[val.bal.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(val.bal.d[val.bal.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(val.bal.d[val.bal.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		val.bal.d <- xts(val.bal.d[val.bal.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(val.bal.d[val.bal.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(val.bal.d[val.bal.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		val.bal.d <- merge(val.bal.d, xts(, index(val.bal.d0)))
		val.bal.d[index(val.bal.d0), ] <- val.bal.d0
		val.cf.d0 <- xts(val.cf.d[val.cf.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(val.cf.d[val.cf.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(val.cf.d[val.cf.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		val.cf.d <- xts(val.cf.d[val.cf.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(val.cf.d[val.cf.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(val.cf.d[val.cf.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		val.cf.d <- merge(val.cf.d, xts(, index(val.cf.d0)))
		val.cf.d[index(val.cf.d0), ] <- val.cf.d0
		val.rep.d <- merge(val.inc.d, val.bal.d, val.cf.d)
		val.rep.d <- val.rep.d[paste0(start.i, "/"), ]
		val.rep.d <- .report.na.fill(val.rep.d, seasonal = c(rep(TRUE, 3), rep(FALSE, 3), rep(TRUE, 2)))
		val.rep.ttm <- .report.calc.ttm(val.rep.d[, c("PARENETP", "BIZINCO", "MANANETR", "ACQUASSETCASH")])
		val.rep.lyr <- .report.get.lyr(val.rep.d[, c("DILUTEDEPS", "PARENETP", "MANANETR")])
		val.rep.lr <- last(val.rep.d[, c("PARESHARRIGH", "TOTALNONCLIAB", "CURFDS")])
		val.p.d <- val.p.d_[index(val.p.d_) <= end.i, ]
		val.p <- vector()
		val.p["TCLOSE"] <- last(ifelse(val.p.d[, "TCLOSE"] == 0, val.p.d[, "LCLOSE"], val.p.d[, "TCLOSE"]))
		val.p["TOTMKTCAP"] <- last(val.p.d[, "TOTMKTCAP"]) * 10 ^ 4
		val[i, 1] <- NA / val.p["TCLOSE"]
		val[i, 2] <- NA / val.p["TCLOSE"]
		val[i, 3] <- val.p["TCLOSE"] / val.rep.lyr[, "DILUTEDEPS"]
		val[i, 4] <- val.rep.lyr[, "PARENETP"] / val.p["TOTMKTCAP"]
		val[i, 5] <- val.rep.ttm["PARENETP"] / val.p["TOTMKTCAP"]
		val[i, 6] <- NA / val.p["TOTMKTCAP"]
		val[i, 7] <- NA / val.p["TOTMKTCAP"]
		val[i, 8] <- NA / val.p["TOTMKTCAP"]
		val[i, 9] <- val.rep.ttm["BIZINCO"] / val.p["TOTMKTCAP"]
		val[i, 10] <- val.rep.lyr[, "MANANETR"] / val.p["TOTMKTCAP"]
		val[i, 11] <- NA / val.p["TCLOSE"]
		val[i, 12] <- val.rep.ttm["MANANETR"] / val.p["TOTMKTCAP"]
		val[i, 13] <- (val.rep.ttm["MANANETR"] - val.rep.ttm["ACQUASSETCASH"]) / 
			val.p["TOTMKTCAP"]
		val[i, 14] <- val.rep.lr[, "PARESHARRIGH"] / val.p["TOTMKTCAP"]
		val[i, 15] <- NA / val.p["TCLOSE"]
		val[i, 16] <- val.rep.ttm["BIZINCO"] / 
			(val.p["TOTMKTCAP"] + val.rep.lr[, "TOTALNONCLIAB"] - val.rep.lr[, "CURFDS"])
	}
	colnames(val) <- c("DividendYield_FY0", "DividendYield_FY1", "Price2EPS_LYR", 
		"EP_LYR", "EP_TTM", "EP_Fwd12M", "EP_FY0", "EP_FY1", "SP_TTM",
		"CashFlowYield_LYR", "CashFlowYield_FY0", "CashFlowYield_TTM", 
		"FreeCashFlowYield_TTM", "BP_LR", "BP_FY0_Median", "Sales2EV")
	.fill.na(val)
}

# Calculate Growth Factors
.calcGrowth <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- rollback(today())
	}
	else {
		end <- ymd(end)
	}
	code <- .getCode(symbol, channel, end)
	gro.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PERPROFIT, NETPROFIT, BIZINCO FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	gro.inc.d_ <- dbGetQuery(channel, gro.inc.q)
	gro.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, TOTASSET FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	gro.bal.d_ <- dbGetQuery(channel, gro.bal.q)
	gro.dt.q <- paste0("SELECT TRADEDATE FROM TQ_SK_DQUOTEINDIC 
		WHERE SYMBOL = '", as.character(symbol), "' AND TRADEDATE <= '", 
		format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	gro.dt.d <- ymd(dbGetQuery(channel, gro.dt.q)[, 1])
	gro.ep <- endpoints(gro.dt.d)
	gro.idx <- last.day(gro.dt.d[gro.ep])
	gro.idx <- gro.idx[gro.idx >= START]
	gro <- xts(matrix(, nrow = length(gro.idx), ncol = 12), gro.idx)
	for (i in 1:length(gro.idx)) {
		end.i <- gro.idx[i]
		start.i <- firstof(year(end.i) - 2)
		gro.inc.d <- gro.inc.d_[ymd(gro.inc.d_[, 1]) <= end.i, -1]
		if (NROW(gro.inc.d) == 0) next
		gro.bal.d <- gro.bal.d_[ymd(gro.bal.d_[, 1]) <= end.i, -1]
		if (NROW(gro.bal.d) == 0) next
		gro.inc.d0 <- xts(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		gro.inc.d <- xts(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		gro.inc.d <- merge(gro.inc.d, xts(, index(gro.inc.d0)))
		gro.inc.d[index(gro.inc.d0), ] <- gro.inc.d0
		gro.bal.d0 <- xts(gro.bal.d[gro.bal.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		gro.bal.d <- xts(gro.bal.d[gro.bal.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(gro.bal.d[gro.bal.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		gro.bal.d <- merge(gro.bal.d, xts(, index(gro.bal.d0)))
		gro.bal.d[index(gro.bal.d0), ] <- gro.bal.d0
		names(gro.bal.d) <- "TOTASSET"
		gro.rep.d <- merge(gro.inc.d, gro.bal.d)
		gro.rep.d <- .report.na.fill(gro.rep.d, seasonal = c(rep(TRUE, 3), FALSE))
		gro.rep.us <- .report.unseasonal(gro.rep.d, seasonal = c(rep(TRUE, 3), FALSE))
		gro.na <- (index(last(gro.rep.d)) - c(1, 5)) < index(gro.rep.d[1])
		gro[i, 1] <- na.or.value(gro.na[1], last(gro.rep.us)[, "PERPROFIT"][[1]] / 
			gro.rep.us[index(last(gro.rep.us)) - 1, "PERPROFIT"][[1]] - 1)
		gro[i, 2] <- na.or.value(gro.na[1], last(gro.rep.us)[, "NETPROFIT"][[1]] / 
			gro.rep.us[index(last(gro.rep.us)) - 1, "NETPROFIT"][[1]] - 1)
		gro[i, 3] <- na.or.value(gro.na[1], last(gro.rep.us)[, "BIZINCO"][[1]] / 
			gro.rep.us[index(last(gro.rep.us)) - 1, "BIZINCO"][[1]] - 1)
		gro[i, 4] <- na.or.value(gro.na[2], last(gro.rep.d)[, "NETPROFIT"][[1]] / 
			gro.rep.d[index(last(gro.rep.d)) - 5, "NETPROFIT"][[1]] - 1)
		gro[i, 5] <- na.or.value(gro.na[2], last(gro.rep.d)[, "BIZINCO"][[1]] / 
			gro.rep.d[index(last(gro.rep.d)) - 5, "BIZINCO"][[1]] - 1)
		gro[i, 6] <- na.or.value(gro.na[1], last(gro.rep.d)[, "NETPROFIT"][[1]] / 
			gro.rep.d[index(last(gro.rep.d)) - 1, "NETPROFIT"][[1]] - 1)
		gro[i, 7] <- na.or.value(gro.na[1], last(gro.rep.d)[, "BIZINCO"][[1]] / 
			gro.rep.d[index(last(gro.rep.d)) - 1, "BIZINCO"][[1]] - 1)
		gro[i, 8] <- NA
		gro[i, 9] <- NA
		gro[i, 10] <- NA
		gro[i, 11] <- NA
		gro[i, 12] <- na.or.value(gro.na[1], last(gro.rep.d)[, "TOTASSET"][[1]] / 
			gro.rep.d[index(last(gro.rep.d)) - 1, "TOTASSET"][[1]] - 1)
	}
	colnames(gro) <- c("SaleEarnings_SQ_YoY", "Earnings_SQ_YoY", "Sales_SQ_YoY", 
		"Earnings_LTG", "Sales_LTG", "Earnings_STG", "Sales_STG", "Earnings_LFG", 
		"Sales_LFG", "Earnings_SFG", "Sales_SFG", "Asset_STG")
	.fill.na(gro)
}
# Calculate Quality Factors
.calcQuality <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- rollback(today())
	}
	else {
		end <- ymd(end)
	}
	code <- .getCode(symbol, channel, end)
	qua.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARENETP, BIZINCO, BIZCOST, SALESEXPE FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	qua.inc.d_ <- dbGetQuery(channel, qua.inc.q)
	qua.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARESHARRIGH, TOTASSET, TOTCURRASSET, 
		TOTALCURRLIAB, TOTALNONCLIAB FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = '", code[1], "' AND REPORTTYPE IN ('1', '3') 
		AND DECLAREDATE <= '", format(end, "%Y%m%d"), "' ORDER BY DECLAREDATE")
	qua.bal.d_ <- dbGetQuery(channel, qua.bal.q)
	qua.dt.q <- paste0("SELECT TRADEDATE FROM TQ_SK_DQUOTEINDIC 
		WHERE SYMBOL = '", as.character(symbol), "' AND TRADEDATE <= '", 
		format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	qua.dt.d <- ymd(dbGetQuery(channel, qua.dt.q)[, 1])
	qua.ep <- endpoints(qua.dt.d)
	qua.idx <- last.day(qua.dt.d[qua.ep])
	qua.idx <- qua.idx[qua.idx >= START]
	qua <- xts(matrix(, nrow = length(qua.idx), ncol = 8), qua.idx)
	for (i in 1:length(qua.idx)) {
		end.i <- qua.idx[i]
		start.i <- firstof(year(end.i) - 2)
		qua.inc.d <- qua.inc.d_[ymd(qua.inc.d_[, 1]) <= end.i, -1]
		if (NROW(qua.inc.d) == 0) next
		qua.bal.d <- qua.bal.d_[ymd(qua.bal.d_[, 1]) <= end.i, -1]
		if (NROW(qua.bal.d) == 0) next
		qua.inc.d0 <- xts(qua.inc.d[qua.inc.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		qua.inc.d <- xts(qua.inc.d[qua.inc.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(qua.inc.d[qua.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		qua.inc.d <- merge(qua.inc.d, xts(, index(qua.inc.d0)))
		qua.inc.d[index(qua.inc.d0), ] <- qua.inc.d0
		qua.bal.d0 <- xts(qua.bal.d[qua.bal.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		qua.bal.d <- xts(qua.bal.d[qua.bal.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(qua.bal.d[qua.bal.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		qua.bal.d <- merge(qua.bal.d, xts(, index(qua.bal.d0)))
		qua.bal.d[index(qua.bal.d0), ] <- qua.bal.d0
		qua.rep.d <- merge(qua.inc.d, qua.bal.d)
		qua.rep.d <- qua.rep.d[paste0(start.i, "/"), ]
		qua.rep.d <- .report.na.fill(qua.rep.d, seasonal = c(rep(TRUE, 4), rep(FALSE, 5)))
		qua.rep.ttm <- .report.calc.ttm(qua.rep.d[, c("PARENETP", "BIZINCO", "BIZCOST", "SALESEXPE")])
		qua.rep.lr <- last(qua.rep.d[, c("PARESHARRIGH", "TOTASSET", 
			"TOTCURRASSET", "TOTALCURRLIAB", "TOTALNONCLIAB")])
		qua[i, 1] <- qua.rep.ttm["PARENETP"] / qua.rep.lr[, "PARESHARRIGH"]
		qua[i, 2] <- qua.rep.ttm["PARENETP"] / qua.rep.lr[, "TOTASSET"]
		qua[i, 3] <- (qua.rep.ttm["BIZINCO"] - qua.rep.ttm["BIZCOST"]) / 
			qua.rep.ttm["BIZINCO"] - 1
		qua[i, 4] <- qua.rep.lr[, "TOTALNONCLIAB"] / qua.rep.lr[, "PARESHARRIGH"]
		qua[i, 5] <- (qua.rep.ttm["BIZINCO"] - qua.rep.ttm["BIZCOST"]) / 
			qua.rep.ttm["SALESEXPE"]
		qua[i, 6] <- qua.rep.ttm["BIZINCO"] / qua.rep.lr[, "TOTASSET"]
		qua[i, 7] <- qua.rep.lr[, "TOTCURRASSET"] / qua.rep.lr[, "TOTALCURRLIAB"]
		qua[i, 8] <- NA
	}
	colnames(qua) <- c("ROE_LR", "ROA_LR", "GrossMargin_TTM", "LTD2Equity_LR", 
		"BerryRatio", "AssetTurnover", "CurrentRatio", "EPS_FY0_Dispersion")
	.fill.na(qua)
}

# Calculate Momentum Factors
.calcMomentum <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- rollback(today())
	}
	else {
		end <- ymd(end)
	}
	mom.p.q <- paste0("SELECT TRADEDATE, TCLOSEAF 
		FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", as.character(symbol), "' 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	mom.p.d <- dbGetQuery(channel, mom.p.q)
	mom.p.d <- xts(mom.p.d[, -1], ymd(mom.p.d[, 1]))
	mom.ep <- endpoints(mom.p.d)
	# If trading days are less then 1, 3, 12, or 60 months, momentum is NAs.
	mom.na <- (length(mom.ep) < c(3, 5, 14, 62))
	mom <- mom.p.d[mom.ep] / na.or.value(mom.na[1], lag(mom.p.d[mom.ep], 1)) - 1
	mom <- merge(mom, mom.p.d[mom.ep] / na.or.value(mom.na[2], lag(mom.p.d[mom.ep], 3)) - 1)
	mom <- merge(mom, mom.p.d[mom.ep] / na.or.value(mom.na[3], lag(mom.p.d[mom.ep], 12)) - 1)
	mom <- merge(mom, mom[, 3] - mom[, 1])
	mom <- merge(mom, mom.p.d[mom.ep] / na.or.value(mom.na[4], lag(mom.p.d[mom.ep], 60)) - 1)
	colnames(mom) <- c("Momentum_1M", "Momentum_3M", "Momentum_12M", 
		"Momentum_12M_1M", "Momentum_60M")
	index(mom) <- last.day(index(mom))
	mom <- mom[paste0(START, "/"), ]
	.fill.na(mom)
}

# Calculate Technical Factors
.calcTech <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- rollback(today())
	}
	else {
		end <- ymd(end)
	}
	tech.p.q <- paste0("SELECT TRADEDATE, EXTCLOSE, TCLOSE, TCLOSEAF, AMOUNT, VOL, MKTSHARE 
		FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", as.character(symbol), "' 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	tech.p.d <- dbGetQuery(channel, tech.p.q)
	tech.p.d <- xts(tech.p.d[, -1], ymd(tech.p.d[, 1]))
	tech.p.d.names <- names(tech.p.d)
	tech.p.d <- cbind(tech.p.d, .EMA(tech.p.d[, "TCLOSEAF"], 75), .EMA(tech.p.d[, "TCLOSEAF"], 180))
	names(tech.p.d) <- c(tech.p.d.names, "EMA75", "EMA180")
	tech.ep <- endpoints(tech.p.d)
	tech.na <- (length(tech.ep) < 3)
	tech.p.d[tech.p.d[, "TCLOSE"] == 0, "TCLOSE"] <- tech.p.d[tech.p.d[, "TCLOSE"] == 0, "EXTCLOSE"]
	tech.p.d <- cbind(tech.p.d, tech.p.d[, "VOL"] / 10^4 / tech.p.d[, "MKTSHARE"], 
		tech.p.d[, "TCLOSE"] / tech.p.d[, "EXTCLOSE"] - 1)
	tech <- log(tech.p.d[tech.ep, "TCLOSE"] * tech.p.d[tech.ep, "MKTSHARE"] * 10^4)
	tech <- merge(tech, na.or.value(tech.na, apply.monthly(tech.p.d[, "AMOUNT"], mean, na.rm = TRUE)))
	tech <- merge(tech, apply.monthly(tech.p.d[, "VOL"], mean, na.rm = TRUE) / 
		.roll.period.apply(tech.p.d[, "VOL"], tech.ep, 12, mean, na.rm = TRUE))
	tech <- merge(tech, na.or.value(tech.na, apply.monthly(tech.p.d[, "VOL.1"], mean, na.rm = TRUE)))
	tech <- merge(tech, .roll.period.apply(tech.p.d[, "VOL.1"], tech.ep, 3, mean, na.rm = TRUE))
	tech <- merge(tech, tech[, 4] / tech[, 5])
	tech <- merge(tech, .roll.period.apply(tech.p.d[, "TCLOSE.1"], tech.ep, 12, moments::skewness))
	tech <- merge(tech, .roll.period.apply(tech.p.d, tech.ep, 12, function (x) 
		mean((x[, "TCLOSE"] - x[, "EXTCLOSE"]) / x[, "AMOUNT"], na.rm = TRUE)))
	tech <- merge(tech, NA)
	tech <- merge(tech, (tech.p.d[tech.ep, "EMA75"] - tech.p.d[tech.ep, "EMA180"]) / tech.p.d[tech.ep, "EMA180"])
	tech <- merge(tech, .roll.period.apply(tech.p.d[, "TCLOSE.1"], tech.ep, 12, sd))
	colnames(tech) <- c("LnFloatCap", "AmountAvg_1M", "NormalizedAbormalVolume", 
		"TurnoverAvg_1M", "TurnoverAvg_3M", "TurnoverAvg_1M_3M", "TSKEW", "ILLIQ", 
		"SmallTradeFlow", "MACrossover", "RealizedVolatility_1Y")
	index(tech) <- last.day(index(tech))
	tech <- tech[paste0(START, "/"), ]
	.fill.na(tech)
}