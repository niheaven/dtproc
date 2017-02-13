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
#   .SupFun: Supplementary Functions

# Fill NAs for NaN or Inf
.fill.na <- function (x) {
	x[is.infinite(x)] <- NA
	x[is.nan(x)] <- NA
	x
}

# Last Day of Months
# Forked from vignette of package:lubridate
last.day <- function (date) {
	ceiling_date(date, "month") - days(1)
}

# NA or Value?
na.or.value <- function (na.trigger, value) {
	if (na.trigger)
		return(NA)
	else
		return(value)
}

# Rolling Period Apply, e.g. Period 1 Year for Rolling 1 Month
# Forked and Modified from xts::period.apply()
.roll.period.apply <- function (x, INDEX, n, FUN, ...) {
	if (length(INDEX) < n + 1)
		return(NA)
	x <- try.xts(x, error = FALSE)
	FUN <- match.fun(FUN)
	xx <- sapply(n:(length(INDEX) - 1), function(y) {
		FUN(x[(INDEX[y - n + 1] + 1):INDEX[y + 1]], ...)
	})
	if (!is.vector(xx)) 
		xx <- t(xx)
	if (is.null(colnames(xx)) && NCOL(x) == (NCOL(xx) + n - 1)) 
		colnames(xx) <- colnames(x)
	reclass(xx, x[INDEX[(n + 1):length(INDEX)]])
}

# Fill NAs in Yearly Data
.na.fill <- function (x) {
	xx <- unlist(x)
	x <- xx
	if (length(x) == 1)
		return(x)
	if (length(x) == 2) {
		if (is.na(x[1]))
			x[1] <- x[2] / 2
		return(x)
		}
	if (length(x) == 3) {
		if (is.na(x[1]))
#			x[1] <- x[2] / 2
			if (is.na(x[3])) {
				x[1] <- x[2] / 2
			}
			else {
				x[1] <- tryCatch(uniroot(function(x) x ^ 2 - (x[2] + x[3]) * x + x[2] ^ 2,
					interval = c(x[2]/4, x[2]))$root, error = function(e) {x[2] / 2})
			}
#		if (is.na(x[3]))
#			x[3] <- x[1] - x[2] + x[2] ^ 2 / x[1]
		return(x)
	}
	if (is.na(x[2]))
		x[2] <- x[4] / 2
	x_na <- (x[4] / x[2] - 1) ^ (1 / 2)
	x[1] <- x[2] / (1 + x_na)
	x[3] <- x[1] * x_na ^ 2 + x[2]
	x[!is.na(xx)] <- xx[!is.na(xx)]
	return(x)
}

# Expanding Seasonal Reports and Filling the NAs
.report.na.fill <- function (x, seasonal) {
	x <- try.xts(x, error = FALSE)
	xx <- merge(x, xts(, seq(floor(first(index(x))), last(index(x)), 
		by = 0.25)), join = "right")
	for (i in 1:NCOL(xx)) {
		na1 <- first(index(xx)[!is.na(xx[, i])])
#		na2 <- last(index(xx)[is.na(xx[, i])])
#		na <- (index(xx) >= floor(na1)) & (index(xx) < ceiling(na2 + 0.25))
		na <- (index(xx) >= floor(na1))
		if (!any(na))
			next
		if (seasonal[i]) {
			xx[na, i] <- unlist(by(xx[na, i], floor(index(xx[na, i])), .na.fill))
		}
		else {
			xx[floor(na1), i] <- xx[na1, i] / ((na1 - floor(na1)) * 4 + 1)
			xx[na, i] <- na.approx(xx[na, i])
		}
	}
	return(xx)
}

##################################################
## Abandon .report.na.fill Method 1
## May Give Negative Value
##################################################
#.report.na.fill <- function (x, seasonal) {
#	x <- try.xts(x, error = FALSE)
#	if (length(seasonal) != NCOL(x))
#		stop("\"seasonal\" must be as long as NCOL(x).")
#	xx <- merge(x, xts(, seq(floor(first(index(x))), last(index(x)), 
#		by = 0.25)), join = "right")
#	for (i in 1:NCOL(xx)) {
#		na1 <- first(index(xx)[!is.na(xx[, i])])
#		na2 <- (index(xx) >= floor(na1))
#		xx[floor(na1), i] <- xx[na1, i] / ((na1 - floor(na1)) * 4 + 1)
#		if (seasonal[i]) {
#			xx[na2, i] <- na.StructTS(xx[na2, i])
#		}
#		else {
#			xx[na2, i] <- na.approx(xx[na2, i])
#		}
#	}
#	return(xx)
#}

##################################################
## Abandon .report.na.fill Method 2
##################################################
#.report.na.fill_ <- function (x, seasonal) {
#	x <- try.xts(x, error = FALSE)
#	if (length(seasonal) != NCOL(x))
#		stop("\"seasonal\" must be as long as NCOL(x).")
#	xx <- merge(x, xts(, seq(floor(first(index(x))), last(index(x)), 
#		by = 0.25)), join = "right")
#	if (anyNA(first(xx))) 
#		xx[1, seasonal] <- xx[index(first(xx)) + 3 / 4, seasonal] / 4
#	for (i in 1:NCOL(xx)) {
#		if (seasonal[i]) {
#			xx[, i] <- na.StructTS(xx[, i])
#		}
#		else {
#			xx[index(na.approx(xx[, i])), i] <- na.approx(xx[, i])
#			for (j in 1:3)
#				xx[j, i] <- xx[4, i][[1]] * xx[j + 4, i][[1]] / xx[8, i][[1]]
#		}
#			xx[, i] <- x[, i]
#	}
#	rbind(xx, x[index(x) > index(last(xx))])
#}

##################################################
## Abandon .report.na.fill Method 3
##################################################
#.report.na.fill <- function (x, seasonal) {
#	x <- try.xts(x, error = FALSE)
#	if (length(seasonal) != NCOL(x))
#		stop("\"seasonal\" must be as long as NCOL(x).")
#	xx <- merge(x, xts(, seq(floor(first(index(x))), floor(last(index(x)) - 1) + 3 / 4, 
#		by = 0.25)), join = "right")
#	for (i in 1:NCOL(xx)) {
#		xx.lm <- vector()
#		if (seasonal[i]) {
#			for (j in 4:1) {
#				xx.lm <- cbind(xx.lm, coredata(xx[(index(xx) %% 1) == (j - 1) / 4, i]))
#				if (all(is.na(xx.lm[, 5 - j])))
#					xx.lm[, 5 - j] <- (5 - j) / 4 * xx.lm[, 1]
#				else
#					xx.lm[is.na(xx.lm[, 5 - j]), 5 - j] <- lm(xx.lm[, 5 - j] ~ xx.lm[, 1] - 1)[[1]] * 
#						xx.lm[is.na(xx.lm[, 5 - j]), 1]
#				coredata(xx[(index(xx) %% 1) == (j - 1) / 4, i]) <- xx.lm[, 5 - j]
#			}
#		}
#		else {
#			xx[index(na.approx(xx[, i])), i] <- na.approx(xx[, i])
#			for (j in 4:1) {
#				xx.lm <- cbind(xx.lm, coredata(xx[(index(xx) %% 1) == (j - 1) / 4, i]))
#				if (all(is.na(xx.lm[, 5 - j])))
#					xx.lm[, 5 - j] <- (5 - j) / 4 * xx.lm[, 1]
#				else
#					xx.lm[is.na(xx.lm[, 5 - j]), 5 - j] <- lm(xx.lm[, 5 - j] ~ xx.lm[, 1] - 1)[[1]] * 
#						xx.lm[is.na(xx.lm[, 5 - j]), 1]
#				coredata(xx[(index(xx) %% 1) == (j - 1) / 4, i]) <- xx.lm[, 5 - j]
#			}
#		}
#	}
#	return(rbind(xx, x[index(x) > index(last(xx))]))
#}

# Calculate Single Season Data from Cumulative Data
.report.unseasonal <- function (x, seasonal) {
	x <- try.xts(x, error = FALSE)
	if (length(seasonal) != NCOL(x))
		stop("\"seasonal\" must be as long as NCOL(x).")
	xx <- x
	xx[, seasonal] <- xx[, seasonal] - lag(xx[, seasonal])
	xx[(index(xx) %% 1) == 0, seasonal] <- x[(index(xx) %% 1) == 0, seasonal]
	return(xx)
}

# Bind Above Two
.report.trans <- function (x, seasonal = rep(TRUE, NCOL(x))) {
	x <- try.xts(x, error = FALSE)
	if (length(seasonal) != NCOL(x))
		stop("\"seasonal\" must be as long as NCOL(x).")
	x <- .report.na.fill(x, seasonal)
	x <- .report.unseasonal(x, seasonal)
	return(x)
}

# Calculate TTM
.report.calc.ttm <- function (x) {
	x <- try.xts(x, error = FALSE)
	xx <- x - lag(x)
	xx[(index(xx) %% 1) == 0, ] <- x[(index(xx) %% 1) == 0, ]
	x.ttm <- colSums(last(xx, 4))
	x.ttm[is.na(last(x))] <- colSums(last(xx[, is.na(last(x))], 5), na.rm = TRUE)
	return(x.ttm)
}

# Get LYR
.report.get.lyr <- function (x) {
	x <- try.xts(x, error = FALSE)
	return(last(x[(index(x) %% 1) == 0.75]))
}

# Modified EMA Function
.EMA <- function (x, n = 10, ...) {
	if (length(x) < n) {
		return(NA)
	}
	else {
		return(EMA(x, n, ...))
	}
}

# Fix Stocks Code
fixCode <- function(code) {
	if (NROW(code) == 1) {
		if (nchar(code) == 9) {
			strtrim(code, width = 6)
		}
		else {
			code <- switch(nchar(code), paste0("00000", code), paste0("0000", code), 
				paste0("000", code), paste0("00", code), paste0("0", code), code)
			if (strtrim(code, 1) == "6") {
				code <- paste0(code, ".SH")
			}
			else {
				code <- paste0(code, ".SZ")
			}
			code
		}
	}
	else {
		if (nchar(code[1]) == 9) {
			sapply(code, strtrim, width = 6)
		}
		else {
			sapply(code, fixCode)
		}
	}
}

# Get Stock Name
getName <- function(symbol, channel, date) {
	if (missing(date)) {
		date <- rollback(today())
	}
	else {
		date <- ymd(date)
	}
	symbol <- sapply(symbol, strtrim, 6)
	name <- dbGetQuery(channel, paste0("SELECT SYMBOL, SESNAME, BEGINDATE, ENDDATE 
		FROM TQ_OA_STCODE WHERE SYMBOL IN (", 
		toString(paste0("'", symbol, "'")), ") AND SETYPE = '101'"))
	name[name[, 4] == "19000101", 4] <- "20991231"
	name[, 3] <- ymd(name[, 3])
	name[, 4] <- ymd(name[, 4])
	name <- name[(name[, 3] <= date) & (name[, 4] >= date), 1:2]
	name <- data.frame(name[, 2], row.names = fixCode(name[, 1]))
	as.character(name[fixCode(symbol), ])
}

# Get CompCode and SeCode (for Internal Use)
.getCode <- function(symbol, channel, date) {
	if (missing(date)) {
		date <- rollback(today())
	}
	else {
		date <- ymd(date)
	}
	symbol <- sapply(symbol, strtrim, 6)
	code <- dbGetQuery(channel, paste0("SELECT SYMBOL, COMPCODE, SECODE, 
		BEGINDATE, ENDDATE FROM TQ_OA_STCODE WHERE SYMBOL IN (", 
		toString(paste0("'", symbol, "'")), ") AND SETYPE = '101'"))
	code[code[, 5] == "19000101", 5] <- "20991231"
	code[, 4] <- ymd(code[, 4])
	code[, 5] <- ymd(code[, 5])
	code <- code[(code[, 4] <= date) & (code[, 5] >= date), 1:3]
	code <- data.frame(code[, 2:3], row.names = fixCode(code[, 1]))
	code[fixCode(symbol), ]
}

# Get Index Components
getIndexComp <- function(code, channel, date) {
	code <- dbGetQuery(channel, paste0("SELECT SAMPLECODE FROM TQ_IX_COMP
		WHERE SYMBOL = ", toString(paste0("'", code, "'")), " 
		AND SELECTEDDATE <= '", date, "' AND (OUTDATE > '", date, "'
		OR USESTATUS = '1') ORDER BY SAMPLECODE"))
	as.matrix(code)
}