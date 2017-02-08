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
#   .getPrice: Get Stocks Price (Adjusted or Unadjusted) (Implementation)

.getPrice <- function (symbol, channel, start, end, 
	period = "days", type = "price", adjust = TRUE) {
	if (missing(start)) {
		beg <- as.Date("1900-01-01")
	}
	else {
		beg <- as.Date(as.character(start), "%Y%m%d")
	}
	if (missing(end)) {
		end <- Sys.Date()
	}
	else {
		end <- as.Date(as.character(end), "%Y%m%d")
	}
	if (beg > end) 
		stop("Start date must be before end date.")
	if (beg > Sys.Date()) 
		stop("Start date is after today's date.")
	period <- match.arg(period, c("days", "weeks", "months", "quarters"))
	type <- match.arg(type, c("price", "split"))
	if (adjust == TRUE)
		adjust <- "change"
	if (adjust == FALSE) 
		adjust <- "unadjusted"
	adjust <- match.arg(adjust, c("change", "split", "unadjusted"))
	if (type == "price") {
		if (adjust == "change") {
			ohlc <- .getPrice(symbol, channel, start, period = "days", adjust = FALSE)
			pchg.q <- paste0("SELECT TQ_QT_SKDAILYPRICE.TRADEDATE, TQ_QT_SKDAILYPRICE.PCHG 
				FROM TQ_QT_SKDAILYPRICE INNER JOIN TQ_OA_STCODE 
				ON TQ_QT_SKDAILYPRICE.SECODE = TQ_OA_STCODE.SECODE 
				AND TQ_OA_STCODE.SYMBOL = '", as.character(symbol), "' 
				AND TQ_OA_STCODE.SETYPE = '101' ORDER BY TQ_QT_SKDAILYPRICE.TRADEDATE")
			pchg <- dbGetQuery(channel, pchg.q)
			pchg <- xts(pchg[, -1], as.Date(as.character(pchg[, 1]), "%Y%m%d"))
			pchg <- pchg[index(ohlc)]
			pchg <- 1 / (1 + pchg / 100)
			pchg <- lag(pchg, -1)
			pchg[length(pchg)] <- 1
			coredata(pchg) <- cumprod(coredata(pchg)[length(pchg):1])[length(pchg):1]
			adj.cl <- pchg * as.numeric(Cl(last(ohlc)))
			cn <- colnames(ohlc)
			ohlc <- cbind(ohlc, ohlc[, "Close"], ohlc[, "Volume"])
			colnames(ohlc) <- c(cn, "Adjusted", "Adj.Volume")
			ohlc[, "Adjusted"] <- pchg * as.numeric(Cl(last(ohlc)))
			ohlc[, "Adj.Volume"] <- ohlc[, "Volume"] * ohlc[, "Close"] / 
				ohlc[, "Adjusted"]
		}
		else if (adjust == "split") {
			ohlc <- .getPrice(symbol, channel, start, period = "days", adjust = FALSE)
			divspl <- .getPrice(symbol, channel, start, 
				type = "split")
			ohlc <- merge(ohlc, divspl, all = TRUE)
			if (NROW(divspl) != 0) {
				adj <- adjRatios(ohlc[, "Split"], ohlc[, "Div"], 
					ohlc[, "Close"])
				s.ratio <- adj[, 1]
				d.ratio <- adj[, 2]
				cn <- colnames(ohlc)
				ohlc <- cbind(ohlc, ohlc[, "Close"], ohlc[, "Volume"])
				colnames(ohlc) <- c(cn, "Adjusted", "Adj.Volume")
				ohlc[, "Adjusted"] <- ohlc[, "Close"] * d.ratio * 
					s.ratio
				ohlc[, "Adj.Volume"] <- ohlc[, "Volume"] * (1/d.ratio)
			}
		}
		else {
			price.q <- paste0("SELECT TRADEDATE, TOPEN, THIGH, TLOW, TCLOSE, VOL 
				FROM TQ_QT_SKDAILYPRICE 
				WHERE SECODE = (SELECT SECODE FROM TQ_OA_STCODE 
					WHERE SYMBOL = '", as.character(symbol), "' AND SETYPE = '101') 
				ORDER BY TRADEDATE") #AND TRADEDATE >= ", start, " 
			ohlc <- dbGetQuery(channel, price.q)
			ohlc[ohlc == 0] <- NA
			ohlc[5] <- na.locf(ohlc[5])
			for (i in 2:4) ohlc[is.na(ohlc[i]), 5] -> ohlc[is.na(ohlc[i]), i]
			ohlc <- xts(ohlc[, -1], as.Date(as.character(ohlc[, 1]), "%Y%m%d"))
			colnames(ohlc) <- c("Open", "High", "Low", "Close", "Volume")
			ohlc[is.na(ohlc)] <- 0
		}
		if (period != "days") {
			if (adjust == "unadjusted") {
				vol <- ohlc[, "Volume"]
				ohlc <- ohlc[, c("Open", "High", "Low", "Close")]
				ohlc <- to.period(ohlc, period)
				colnames(ohlc) <- c("Open", "High", "Low", "Close")
				vol <- period.apply(vol, endpoints(vol, period), colSums)                                                                    
				ohlc <- merge(ohlc, vol)
			}
			else {
				vol <- ohlc[, c("Volume", "Adj.Volume")]
				adj <- ohlc[, "Adjusted"]
				ohlc <- ohlc[, c("Open", "High", "Low", "Close")]
				ohlc <- to.period(ohlc, period)
				colnames(ohlc) <- c("Open", "High", "Low", "Close")
				ohlc <- merge(ohlc, adj, join = "left")
				vol <- period.apply(vol, endpoints(vol, period), colSums)
				ohlc <- merge(ohlc, vol)
			}
		}
		ohlc <- ohlc[, c("Open", "High", "Low", "Close", "Volume", 
			"Adjusted", "Adj.Volume")]
	}
	else {
		pr.q <- paste0("SELECT DIVITYPE, XDRDATE, LISTDATE, CASHDVARRBEGDATE, 
			PRETAXCASHMAXDV, PROBONUSRT, TRANADDRT, CHANGERT 
			FROM TQ_SK_PRORIGHTS WHERE PUBLISHDATE != '19000101' 
			AND GRAOBJTYPE IN ('1', '2') AND DIVITYPE != '0' 
			AND SECODE = (SELECT SECODE FROM TQ_OA_STCODE WHERE SYMBOL = '", 
			as.character(symbol), "' AND SETYPE = '101') ORDER BY PUBLISHDATE")
		ohlc <- dbGetQuery(channel, pr.q)
		ohlc[is.na(ohlc)] <- 0
		pr.param <- data.frame(c("1", "XDRDATE", "PRETAXCASHMAXDV"), 
			c("4", "XDRDATE", "CHANGERT"), c("5", "XDRDATE", "PROBONUSRT"), 
			c("8", "XDRDATE", "TRANADDRT"), c("9", "XDRDATE", "CHANGERT"), 
			c("A", "LISTDATE", "PROBONUSRT"), c("B", "LISTDATE", "PRETAXCASHMAXDV"), 
			c("F", "LISTDATE", "PRETAXCASHMAXDV"), c("G", "LISTDATE", "PROBONUSRT"), 
			c("H", "LISTDATE", "PROBONUSRT"), c("I", "LISTDATE", "CHANGERT"), 
			c("Q", "LISTDATE", "PROBONUSRT"), c("R", "CASHDVARRBEGDATE", "PRETAXCASHMAXDV"), 
			c("U", "LISTDATE", "TRANADDRT"), row.names = c("Type", "Date", "Value"), 
			stringsAsFactors = FALSE)
		ohlc <- apply(pr.param, 2, function(param) 
			{xts(ohlc[ohlc[, "DIVITYPE"] == as.character(param[1]), as.character(param[3])], 
			as.Date(as.character(ohlc[ohlc[, "DIVITYPE"] == as.character(param[1]), 
			as.character(param[2])]), "%Y%m%d"))})
		names(ohlc) <- c("1", "4", "5", "8", "9", "A", "B", "F", "G", "H", "I", "Q", "R", "U")
		div <- merge(ohlc$'1', ohlc$'B', ohlc$'F', ohlc$'R', fill = 0)
		div <- xts(apply(div, 1, sum), index(div))/10
		spl <- merge(ohlc$'5', ohlc$'8', ohlc$'A', ohlc$'G', ohlc$'H', 
			ohlc$'Q', ohlc$'U', fill = 0)
		spl <- 10/(10 + xts(apply(spl, 1, sum), index(spl)))
		spl <- merge(spl, 1/ohlc$'4', 1/ohlc$'9', 1/ohlc$'I', fill = 1)
		spl <- xts(apply(spl, 1, prod), index(spl))
		ohlc <- merge(Adj.Div = div, Split = spl, all = TRUE)
		if (NROW(ohlc) == 0) 
			return(ohlc)
		if (all(is.na(ohlc[, "Split"]))) {
			s.ratio <- rep(1, NROW(ohlc))
		}
		else {
			s.ratio <- adjRatios(splits = ohlc[, "Split"])[, 1]
		}
		ohlc <- cbind(ohlc, ohlc[, "Adj.Div"] * (1/s.ratio))
		colnames(ohlc)[3] <- "Div"
		ohlc <- ohlc[, c("Div", "Split", "Adj.Div")]
	}
	ohlc[paste(beg, end, sep = "/"), ]
}