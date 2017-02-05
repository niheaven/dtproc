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
#   .calcFactors: Calculate Stock Factors (Implementation)

# Load Supplementary Functions
source(".SupFun.R")

# Calculate Value Factors
.calcValue <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	val.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, DILUTEDEPS, PARENETP, BIZINCO FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' ORDER BY DECLAREDATE")
	val.inc.d_ <- dbGetQuery(channel, val.inc.q)
	val.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARESHARRIGH, TOTALNONCLIAB, CURFDS FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' ORDER BY DECLAREDATE")
	val.bal.d_ <- dbGetQuery(channel, val.bal.q)
	val.cf.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, MANANETR, ACQUASSETCASH FROM TQ_FIN_PROCFSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' ORDER BY DECLAREDATE")
	val.cf.d_ <- dbGetQuery(channel, val.cf.q)
	val.p.q <- paste0("SELECT TRADEDATE, LCLOSE, TCLOSE, TOTMKTCAP FROM TQ_QT_SKDAILYPRICE 
		WHERE SECODE = (SELECT SECODE FROM TQ_OA_STCODE 
			WHERE SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	val.p.d_ <- dbGetQuery(channel, val.p.q)
	val.p.d_ <- xts(val.p.d_[, -1], as.Date(as.character(val.p.d_[, 1]), "%Y%m%d"))
	end.ep <- endpoints(val.p.d_)
	val <- xts(matrix(, nrow = length(end.ep) - 1, ncol = 16), index(val.p.d_)[end.ep])
	for (i in 1:(length(end.ep) - 1)) {
		end.i <- index(val.p.d_)[end.ep[i + 1]]
		start.i <- firstof(as.numeric(strtrim(format(end.i, "%Y%m%d"), 4)) - 1)
		val.inc.d <- val.inc.d_[as.Date(as.character(val.inc.d_[, 1]), "%Y%m%d") 
			<= end.i, -1]
		if (NROW(val.inc.d) == 0) next
		val.bal.d <- val.bal.d_[as.Date(as.character(val.bal.d_[, 1]), "%Y%m%d") 
			<= end.i, -1]
		if (NROW(val.bal.d) == 0) next
		val.cf.d <- val.cf.d_[as.Date(as.character(val.cf.d_[, 1]), "%Y%m%d") 
			<= end.i, -1]
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
#		for (j in 1:NROW(val.rep.d)) {
#			year <- format(index(val.rep.d[j, ]), "%Y")
#			qtr <- format(index(val.rep.d[j, ]), "%q")
#			if (is.na(val.rep.d[j, "DILUTEDEPS"])) {
#				newd <- val.inc.d_[(val.inc.d_[, "REPORTYEAR"] == year) &  
#					(val.inc.d_[, "REPORTDATETYPE"] == qtr) & 
#					(val.inc.d_[, "REPORTTYPE"] == "1"), -1:-4]
#				if (NROW(newd) != 0)
#					coredata(val.rep.d[j, c("DILUTEDEPS", "PARENETP", "BIZINCO")]) <- newd
#
#			}
#			if (is.na(val.rep.d[j, "PARESHARRIGH"])) {
#				newd <- val.bal.d_[(val.bal.d_[, "REPORTYEAR"] == year) &  
#					(val.bal.d_[, "REPORTDATETYPE"] == qtr) & 
#					(val.bal.d_[, "REPORTTYPE"] == "1"), -1:-4]
#				if (NROW(newd) != 0)
#					coredata(val.rep.d[j, c("PARESHARRIGH", "TOTALNONCLIAB", "CURFDS")]) <- newd
#
#			}
#			if (is.na(val.rep.d[j, "MANANETR"])) {
#				newd <- val.cf.d_[(val.cf.d_[, "REPORTYEAR"] == year) &  
#					(val.cf.d_[, "REPORTDATETYPE"] == qtr) & 
#					(val.cf.d_[, "REPORTTYPE"] == "1"), -1:-4]
#				if (NROW(newd) != 0)
#					coredata(val.rep.d[j, c("MANANETR", "ACQUASSETCASH")]) <- newd
#
#			}
#		}
		val.rep.d <- .report.na.fill(val.rep.d, seasonal = c(rep(TRUE, 3), rep(FALSE, 3), rep(TRUE, 2)))
		val.rep.ttm <- .report.calc.ttm(val.rep.d[, c("PARENETP", "BIZINCO", "MANANETR", "ACQUASSETCASH")])
		val.rep.lyr <- as.data.frame(.report.calc.lyr(val.rep.d[, c("DILUTEDEPS", "PARENETP", "MANANETR")]))
		val.rep.lr <- as.data.frame(last(val.rep.d[, c("PARESHARRIGH", "TOTALNONCLIAB", "CURFDS")]))
		val.p.d <- val.p.d_[index(val.p.d_) <= end.i, ]
		val.p <- vector()
		val.p["TCLOSE"] <- ifelse(last(val.p.d[, "TCLOSE"]) == 0, val.p.d[, "LCLOSE"], val.p.d[, "TCLOSE"])
		val.p["TOTMKTCAP"] <- last(val.p.d[, "TOTMKTCAP"]) * 10 ^ 4
		val[i, 1] <- NA / val.p["TCLOSE"]
		val[i, 2] <- NA / val.p["TCLOSE"]
		val[i, 3] <- val.p["TCLOSE"] / val.rep.lyr["DILUTEDEPS"]
		val[i, 4] <- val.rep.lyr["PARENETP"] / val.p["TOTMKTCAP"]
		val[i, 5] <- val.rep.ttm["PARENETP"] / val.p["TOTMKTCAP"]
		val[i, 6] <- NA / val.p["TOTMKTCAP"]
		val[i, 7] <- NA / val.p["TOTMKTCAP"]
		val[i, 8] <- NA / val.p["TOTMKTCAP"]
		val[i, 9] <- val.rep.ttm["BIZINCO"] / val.p["TOTMKTCAP"]
		val[i, 10] <- val.rep.lyr["MANANETR"] / val.p["TOTMKTCAP"]
		val[i, 11] <- NA / val.p["TCLOSE"]
		val[i, 12] <- val.rep.ttm["MANANETR"] / val.p["TOTMKTCAP"]
		val[i, 13] <- (val.rep.ttm["MANANETR"] - val.rep.ttm["ACQUASSETCASH"]) / 
			val.p["TOTMKTCAP"]
		val[i, 14] <- val.rep.lr["PARESHARRIGH"] / val.p["TOTMKTCAP"]
		val[i, 15] <- NA / val.p["TCLOSE"]
		val[i, 16] <- val.rep.ttm["BIZINCO"] / 
			(val.p["TOTMKTCAP"] + val.rep.lr["TOTALNONCLIAB"] - val.rep.lr["CURFDS"])
	}
	colnames(val) <- c("DividendYield_FY0", "DividendYield_FY1", "Price2EPS_LYR", 
		"EP_LYR", "EP_TTM", "EP_Fwd12M", "EP_FY0", "EP_FY1", "SP_TTM",
		"CashFlowYield_LYR", "CashFlowYield_FY0", "CashFlowYield_TTM", 
		"FreeCashFlowYield_TTM", "BP_LR", "BP_FY0_Median", "Sales2EV")
	val
}

# Calculate Growth Factors
.calcGrowth <- function (symbol, channel, end) {
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
	gro.inc.d_ <- dbGetQuery(channel, gro.inc.q)
	gro.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, TOTASSET FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"'ORDER BY DECLAREDATE")
	gro.bal.d_ <- dbGetQuery(channel, gro.bal.q)
	gro.dt.q <- paste0("SELECT TRADEDATE FROM TQ_SK_DQUOTEINDIC 
		WHERE SYMBOL = '", as.character(symbol), "' AND TRADEDATE <= '", 
		format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	gro.dt.d <- as.Date(as.character(dbGetQuery(channel, gro.dt.q)[, 1]), "%Y%m%d")
	end.ep <- endpoints(gro.dt.d)
	gro <- xts(matrix(, nrow = length(end.ep) - 1, ncol = 12), gro.dt.d[end.ep])
	for (i in 1:(length(end.ep) - 1)) {
		gro.inc.d <- gro.inc.d_[as.Date(as.character(gro.inc.d_[, 1]), "%Y%m%d") 
			<= gro.dt.d[end.ep[i + 1]], -1]
		gro.inc.d0 <- xts(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		gro.inc.d <- xts(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(gro.inc.d[gro.inc.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		gro.inc.d <- merge(gro.inc.d, xts(, index(gro.inc.d0)))
		gro.inc.d[index(gro.inc.d0), ] <- gro.inc.d0
		gro.bal.d <- gro.bal.d_[as.Date(as.character(gro.bal.d_[, 1]), "%Y%m%d") 
			<= gro.dt.d[end.ep[i + 1]], -1]
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
		gro[i, 1] <- last(gro.d.us)[, "PERPROFIT"][[1]] / 
			gro.d.us[index(last(gro.d.us)) - 1, "PERPROFIT"][[1]] - 1
		gro[i, 2] <- last(gro.d.us)[, "NETPROFIT"][[1]] / 
			gro.d.us[index(last(gro.d.us)) - 1, "NETPROFIT"][[1]] - 1
		gro[i, 3] <- last(gro.d.us)[, "BIZINCO"][[1]] / 
			gro.d.us[index(last(gro.d.us)) - 1, "BIZINCO"][[1]] - 1
		gro[i, 4] <- ifelse((index(last(gro.d)) - 5) < index(gro.d[1]), 
			NA, last(gro.d)[, "NETPROFIT"][[1]] / 
			gro.d[index(last(gro.d)) - 5, "NETPROFIT"][[1]] - 1)
		gro[i, 5] <- ifelse((index(last(gro.d)) - 5) < index(gro.d[1]), 
			NA, last(gro.d)[, "BIZINCO"][[1]] / 
			gro.d[index(last(gro.d)) - 5, "BIZINCO"][[1]] - 1)
		gro[i, 6] <- last(gro.d)[, "NETPROFIT"][[1]] / 
			gro.d[index(last(gro.d)) - 1, "NETPROFIT"][[1]] - 1
		gro[i, 7] <- last(gro.d)[, "BIZINCO"][[1]] / 
			gro.d[index(last(gro.d)) - 1, "BIZINCO"][[1]] - 1
		gro[i, 8] <- NA
		gro[i, 9] <- NA
		gro[i, 10] <- NA
		gro[i, 11] <- NA
		gro[i, 12] <- last(gro.d)[, "TOTASSET"][[1]] / 
			gro.d[index(last(gro.d)) - 1, "TOTASSET"][[1]] - 1
	}
	colnames(gro) <- c("SaleEarnings_SQ_YoY", "Earnings_SQ_YoY", "Sales_SQ_YoY", 
		"Earnings_LTG", "Sales_LTG", "Earnings_STG", "Sales_STG", "Earnings_LFG", 
		"Sales_LFG", "Earnings_SFG", "Sales_SFG", "Asset_STG")
	gro
}
# Calculate Quality Factors
.calcQuality <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	qua.inc.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, DILUTEDEPS, PARENETP, BIZINCO FROM TQ_FIN_PROINCSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' ORDER BY DECLAREDATE")
	qua.inc.d_ <- dbGetQuery(channel, qua.inc.q)
	qua.bal.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, PARESHARRIGH, TOTALNONCLIAB, CURFDS FROM TQ_FIN_PROBALSHEETNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' ORDER BY DECLAREDATE")
	qua.bal.d_ <- dbGetQuery(channel, qua.bal.q)
	qua.cf.q <- paste0("SELECT DECLAREDATE, REPORTYEAR, REPORTDATETYPE, 
		REPORTTYPE, MANANETR, ACQUASSETCASH FROM TQ_FIN_PROCFSTATEMENTNEW 
		WHERE COMPCODE = (SELECT COMPCODE FROM TQ_OA_STCODE WHERE 
		SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND REPORTTYPE IN ('1', '3') AND DECLAREDATE <= '", format(end, "%Y%m%d"), 
		"' ORDER BY DECLAREDATE")
	qua.cf.d_ <- dbGetQuery(channel, qua.cf.q)
	qua.p.q <- paste0("SELECT TRADEDATE, LCLOSE, TCLOSE, TOTMKTCAP FROM TQ_QT_SKDAILYPRICE 
		WHERE SECODE = (SELECT SECODE FROM TQ_OA_STCODE 
			WHERE SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	qua.p.d_ <- dbGetQuery(channel, qua.p.q)
	qua.p.d_ <- xts(qua.p.d_[, -1], as.Date(as.character(qua.p.d_[, 1]), "%Y%m%d"))
	end.ep <- endpoints(qua.p.d_)
	qua <- xts(matrix(, nrow = length(end.ep) - 1, ncol = 16), index(qua.p.d_)[end.ep])
	for (i in 1:(length(end.ep) - 1)) {
		end.i <- index(qua.p.d_)[end.ep[i + 1]]
		start.i <- firstof(as.numeric(strtrim(format(end.i, "%Y%m%d"), 4)) - 1)
		qua.inc.d <- qua.inc.d_[as.Date(as.character(qua.inc.d_[, 1]), "%Y%m%d") 
			<= end.i, -1]
		if (NROW(qua.inc.d) == 0) next
		qua.bal.d <- qua.bal.d_[as.Date(as.character(qua.bal.d_[, 1]), "%Y%m%d") 
			<= end.i, -1]
		if (NROW(qua.bal.d) == 0) next
		qua.cf.d <- qua.cf.d_[as.Date(as.character(qua.cf.d_[, 1]), "%Y%m%d") 
			<= end.i, -1]
		if (NROW(qua.cf.d) == 0) next
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
		qua.cf.d0 <- xts(qua.cf.d[qua.cf.d["REPORTTYPE"] == 3, -1:-3], 
			as.yearqtr(paste0(t(qua.cf.d[qua.cf.d["REPORTTYPE"] == 3, "REPORTYEAR"]), 
			"-", t(qua.cf.d[qua.cf.d["REPORTTYPE"] == 3, "REPORTDATETYPE"]))))
		qua.cf.d <- xts(qua.cf.d[qua.cf.d["REPORTTYPE"] == 1, -1:-3], 
			as.yearqtr(paste0(t(qua.cf.d[qua.cf.d["REPORTTYPE"] == 1, "REPORTYEAR"]), 
			"-", t(qua.cf.d[qua.cf.d["REPORTTYPE"] == 1, "REPORTDATETYPE"]))))
		qua.cf.d <- merge(qua.cf.d, xts(, index(qua.cf.d0)))
		qua.cf.d[index(qua.cf.d0), ] <- qua.cf.d0
		qua.rep.d <- merge(qua.inc.d, qua.bal.d, qua.cf.d)
		qua.rep.d <- qua.rep.d[paste0(start.i, "/"), ]
#		for (j in 1:NROW(qua.rep.d)) {
#			year <- format(index(qua.rep.d[j, ]), "%Y")
#			qtr <- format(index(qua.rep.d[j, ]), "%q")
#			if (is.na(qua.rep.d[j, "DILUTEDEPS"])) {
#				newd <- qua.inc.d_[(qua.inc.d_[, "REPORTYEAR"] == year) &  
#					(qua.inc.d_[, "REPORTDATETYPE"] == qtr) & 
#					(qua.inc.d_[, "REPORTTYPE"] == "1"), -1:-4]
#				if (NROW(newd) != 0)
#					coredata(qua.rep.d[j, c("DILUTEDEPS", "PARENETP", "BIZINCO")]) <- newd
#
#			}
#			if (is.na(qua.rep.d[j, "PARESHARRIGH"])) {
#				newd <- qua.bal.d_[(qua.bal.d_[, "REPORTYEAR"] == year) &  
#					(qua.bal.d_[, "REPORTDATETYPE"] == qtr) & 
#					(qua.bal.d_[, "REPORTTYPE"] == "1"), -1:-4]
#				if (NROW(newd) != 0)
#					coredata(qua.rep.d[j, c("PARESHARRIGH", "TOTALNONCLIAB", "CURFDS")]) <- newd
#
#			}
#			if (is.na(qua.rep.d[j, "MANANETR"])) {
#				newd <- qua.cf.d_[(qua.cf.d_[, "REPORTYEAR"] == year) &  
#					(qua.cf.d_[, "REPORTDATETYPE"] == qtr) & 
#					(qua.cf.d_[, "REPORTTYPE"] == "1"), -1:-4]
#				if (NROW(newd) != 0)
#					coredata(qua.rep.d[j, c("MANANETR", "ACQUASSETCASH")]) <- newd
#
#			}
#		}
		qua.rep.d <- .report.na.fill(qua.rep.d, seasonal = c(rep(TRUE, 3), rep(FALSE, 3), rep(TRUE, 2)))
		qua.rep.ttm <- .report.calc.ttm(qua.rep.d[, c("PARENETP", "BIZINCO", "MANANETR", "ACQUASSETCASH")])
		qua.rep.lyr <- as.data.frame(.report.calc.lyr(qua.rep.d[, c("DILUTEDEPS", "PARENETP", "MANANETR")]))
		qua.rep.lr <- as.data.frame(last(qua.rep.d[, c("PARESHARRIGH", "TOTALNONCLIAB", "CURFDS")]))
		qua.p.d <- qua.p.d_[index(qua.p.d_) <= end.i, ]
		qua.p <- vector()
		qua.p["TCLOSE"] <- ifelse(last(qua.p.d[, "TCLOSE"]) == 0, qua.p.d[, "LCLOSE"], qua.p.d[, "TCLOSE"])
		qua.p["TOTMKTCAP"] <- last(qua.p.d[, "TOTMKTCAP"]) * 10 ^ 4
		qua[i, 1] <- NA / qua.p["TCLOSE"]
		qua[i, 2] <- NA / qua.p["TCLOSE"]
		qua[i, 3] <- qua.p["TCLOSE"] / qua.rep.lyr["DILUTEDEPS"]
		qua[i, 4] <- qua.rep.lyr["PARENETP"] / qua.p["TOTMKTCAP"]
		qua[i, 5] <- qua.rep.ttm["PARENETP"] / qua.p["TOTMKTCAP"]
		qua[i, 6] <- NA / qua.p["TOTMKTCAP"]
		qua[i, 7] <- NA / qua.p["TOTMKTCAP"]
		qua[i, 8] <- NA / qua.p["TOTMKTCAP"]
	}
	colnames(qua) <- c("ROE_LR", "ROA_LR", "GrossMargin_TTM", "LTD2Equity_LR", 
		"BerryRatio", "AssetTurnover", "CurrentRatio", "EPS_FY0_Dispersion")
	qua
}

# Calculate Momentum Factors
.calcMomentum <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	mom.q <- paste0("SELECT TRADEDATE, TCLOSEAF 
		FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", as.character(symbol), "' 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	mom.d <- dbGetQuery(channel, mom.q)
	mom.d <- xts(mom.d[, -1], as.Date(as.character(mom.d[, 1]), "%Y%m%d"))
	end.ep <- endpoints(mom.d)
	mom <- mom.d[end.ep] / lag(mom.d[end.ep], 1) - 1
	mom <- merge(mom, mom.d[end.ep] / lag(mom.d[end.ep], 3) - 1)
	mom <- merge(mom, mom.d[end.ep] / lag(mom.d[end.ep], 12) - 1)
	mom <- merge(mom, mom[, 3] - mom[, 1])
	mom <- merge(mom, mom.d[end.ep] / lag(mom.d[end.ep], 60) - 1)
	colnames(mom) <- c("Momentum_1M", "Momentum_3M", "Momentum_12M", 
		"Momentum_12M_1M", "Momentum_60M")
	mom
}

# Calculate Technical Factors
.calcTech <- function (symbol, channel, end) {
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	tech.q <- paste0("SELECT TRADEDATE, EXTCLOSE, TCLOSE, TCLOSEAF, AMOUNT, VOL, MKTSHARE 
		FROM TQ_SK_DQUOTEINDIC WHERE SYMBOL = '", as.character(symbol), "' 
		AND TRADEDATE <= '", format(end, "%Y%m%d"), "' ORDER BY TRADEDATE")
	tech.d <- dbGetQuery(channel, tech.q)
	tech.d <- xts(tech.d[, -1], as.Date(as.character(tech.d[, 1]), "%Y%m%d"))
		tech.d.names <- names(tech.d)
	tech.d <- cbind(tech.d, .EMA(tech.d[, "TCLOSEAF"], 75), .EMA(tech.d[, "TCLOSEAF"], 180))
	names(tech.d) <- c(tech.d.names, "EMA75", "EMA180")
	end.ep <- endpoints(tech.d)
	tech.d[tech.d[, "TCLOSE"] == 0, "TCLOSE"] <- tech.d[tech.d[, "TCLOSE"] == 0, "EXTCLOSE"]
	tech.d <- cbind(tech.d, tech.d[, "VOL"] / 10^4 / tech.d[, "MKTSHARE"], 
		tech.d[, "TCLOSE"] / tech.d[, "EXTCLOSE"] - 1)
	tech <- log(tech.d[end.ep, "TCLOSE"] * tech.d[end.ep, "MKTSHARE"] * 10^4)
	tech <- merge(tech, apply.monthly(tech.d[, "AMOUNT"], mean, na.rm = TRUE))
	tech <- merge(tech, apply.monthly(tech.d[, "VOL"], mean, na.rm = TRUE) / 
		.roll.period.apply(tech.d[, "VOL"], end.ep, 12, mean, na.rm = TRUE))
	tech <- merge(tech, apply.monthly(tech.d[, "VOL.1"], mean, na.rm = TRUE))
	tech <- merge(tech, .roll.period.apply(tech.d[, "VOL.1"], end.ep, 3, mean, na.rm = TRUE))
	tech <- merge(tech, tech[, 4] / tech[, 5])
	tech <- merge(tech, .roll.period.apply(tech.d[, "TCLOSE.1"], end.ep, 12, moments::skewness))
	tech <- merge(tech, .roll.period.apply(tech.d, end.ep, 12, function (x) 
		mean((x[, "TCLOSE"] - x[, "EXTCLOSE"]) / x[, "AMOUNT"], na.rm = TRUE)))
	tech <- merge(tech, NA)
	tech <- merge(tech, (tech.d[end.ep, "EMA75"] - tech.d[end.ep, "EMA180"]) / tech.d[end.ep, "EMA180"])
	tech <- merge(tech, .roll.period.apply(tech.d[, "TCLOSE.1"], end.ep, 12, sd))
	colnames(tech) <- c("LnFloatCap", "AmountAvg_1M", "NormalizedAbormalVolume", 
		"TurnoverAvg_1M", "TurnoverAvg_3M", "TurnoverAvg_1M_3M", "TSKEW", "ILLIQ", 
		"SmallTradeFlow", "MACrossover", "RealizedVolatility_1Y")
	tech
}