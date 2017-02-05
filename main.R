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
#   main: Main Source File

require(DBI)
require(quantmod)

# source("getPrice.R", encoding = 'UTF-8')
source("src/calcFactors.R", chdir = TRUE, encoding = 'UTF-8')

ch_data = dbConnect(RMySQL::MySQL(), dbname = "CAIHUI", default.file = "./dbi/.my.cnf")
dbSendQuery(ch_data, "SET NAMES gbk") # for Windows
# ch_data = dbConnect(RSQLite::SQLite(), "../../DBs/CaihuiDB.db3")
ch_factors = dbConnect(RMySQL::MySQL(), dbname = "factors", default.file = "./dbi/.my.cnf")
dbSendQuery(ch_factors, "SET NAMES gbk") # for Windows
# ch_factors = dbConnect(RSQLite::SQLite(), "../../DBs/FactorsDB.db3")

sym = "600795"
channel <- ch_data
symbol <- sym
end <- 20161231
