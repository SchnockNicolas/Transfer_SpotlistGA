library(odbc)
library(magrittr)

SQL_Driver <- function()
{
  x <- odbcListDrivers(keep = getOption("odbc.drivers_keep"), filter = getOption("odbc.drivers_filter") )
  
  as.character(x[which(x$attribute == "Driver"), "value"])
}


SQL_Connection <- function(Server, Database, uid, pwd, driver = NULL)
{
  # Server <- "mediadatatransfer.database.windows.net" ; Database <- "MediaData_Transfer" ; uid <- "Nico" ; pwd <- "#Compagnie1984" ; driver <-  "/usr/local/lib/libmsodbcsql.17.dylib"
  
  if(is.null(driver)) driver <-  SQL_Driver()
  
  conn <- dbConnect(odbc::odbc(), Driver = driver, 
                    Server = Server, Database = Database, 
                    uid = uid, pwd = pwd)
  
  conn
}



# dbListFields(SQL_Connection(), "TV_DP_North")


#import_SLQl
sql_insert_direct <- function(table, x,conn, cols = NULL, constraints = NULL)
{
  # constraints => ce sont les dimensions. Si c'est mentionné, le script va faire un upsert avec les contraints comme clé .
  # constraints = c("Channel_CIM_ID", "Year", "Month", "Week", "TimeBand", "Timeshift", "Targets")
  # x <- df[, colnames(df)[!(colnames(df) %in% c("shr%", "Channel"))]] ; table <- "TV_Breaks_North" ; conn = NULL ; x <- x[, colnames(x)[colnames(x) != "shr%"]] ; cols = NULL
  # conn <- SQL_Connection("mediadatatransfer.database.windows.net" , "MediaData_Transfer", "Nico", "#Compagnie1984")
  if(class(x) != "data.frame") { stop("Not a Dataframe")}
  
  colnames(x) <- gsub(" ", "_", colnames(x))
  colnames(x) <- gsub("[.]", "_", colnames(x))
  
  noms_col <- colnames(x) #else {noms_col <- cols}
  noms_colsSQL <- if(is.null(cols)) { noms_colsSQL <- noms_col } else { noms_colsSQL <- cols }
  
  packet <- 1
  nbrLignes <- nrow(x)
  colonnes <- paste0("[", do.call("paste", c(lapply(c(noms_colsSQL), paste), sep="],[")), "]")
  
  classes <- do.call("cbind", lapply(x, class))
  
  # print(classes)
  
  # Préparation des  types de variables
  
  for(col in 1:length(x))
  {
    x[, col] <- gsub("[']","''",x[, col])
    x[, col] <- strtrim(x[, col], 255)
    
    if(classes[1, noms_col[col]] == "character" | classes[1, noms_col[col]] == "list"
       | classes[1, noms_col[col]] == "Date"
       | classes[1, noms_col[col]] == "factor") { x[, col] <- ifelse(is.null(x[, col]) | is.na(x[, col]) , "NULL" , paste0("'", x[, col], "'") )
    }
  }
  
  packetSize <- 1000
  # Packets de 1000
  for (packet in (1:(ceiling(nrow(x) / packetSize))))
  {
    #  print(packet)
    # Calcul des rangees de debut et de fin
    
    row_debut <- 1 + (packet - 1) * packetSize
    row_fin <- (packet * packetSize)  
    
    if(nrow(x) <= row_fin) { row_fin <- nrow(x)}
    
    # SQLSTR 
    SQLSTR <- paste0("Insert into [", table , "] (", colonnes  , ") VALUES ")  
    
    virgule <- 0
    
    for (i in row_debut:row_fin)
    {
      
      valeurs <- paste(x[i, ],collapse = ",")
      
      if(virgule!=0) {SQLSTR <- paste0(SQLSTR, ",")  }
      
      virgule<-1
      
      SQLSTR <- paste0(SQLSTR, "(", valeurs, ") ")  
      
    }
    
    SQLSTR <- paste0(SQLSTR, ";")
    
    if(!is.null(constraints)) SQLSTR <- SQLMerge(conn, table, SQLSTR, constraints, colnames(x))
    
    # print(SQLSTR)
    
    print(paste(row_debut," - ", row_fin, " / ", nbrLignes ))
    print( system.time({ y <- tryCatch(dbSendQuery(conn, SQLSTR) %>% dbFetch(), error = function(e) { print(substr(e,0, 500)); return(NULL) } )} ))
    
    if(is.null(y)) { print("SQL ERROR") ; return(NULL)}
    
    #   df <- sqlQuery(conn, SQLSTR)
    
    
    
    # if(length(df) > 0) { return(df)}
    
  }
  
  TRUE
}


SQLMerge <- function(con, tableName, insertClause, constraints, clnames)
{
  
  #  tableName <- "TVSpotlist" ; con <- conn #SQL_Connection("mediadatatransfer.database.windows.net" , "MediaData_Transfer", "Nico", "#Compagnie1984")
  
  # Noms des colonnes
  SQLSTR <- paste0("SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" , tableName ,  "'")
  result <- dbSendQuery(con, SQLSTR)
  def <- dbFetch(result) %>% filter(COLUMN_NAME %in% clnames)
  
  SQLSTR <- paste0("declare @Source table ( ")
  
  for(i in c(1:nrow(def)))
  {
    if(i > 1) SQLSTR <- paste0(SQLSTR, ', ')
    SQLSTR <- paste0(SQLSTR, "[", def[i, "COLUMN_NAME"], "] ", def[i, "DATA_TYPE"])
    if(!is.na(def[i, "CHARACTER_MAXIMUM_LENGTH"])) SQLSTR <- paste0(SQLSTR, "(", def[i, "CHARACTER_MAXIMUM_LENGTH"], ")")
  }
  SQLSTR <- paste0(SQLSTR, ")")
  
  
  # MERGE part
  
  #constraints <- c("Channel_CIM_ID", "Year", "Month", "Week", "TimeBand", "Timeshift", "Targets")
  measures <- def$COLUMN_NAME[!(def$COLUMN_NAME %in% constraints)]
  
  SQLSTR2 <- paste0("MERGE ", tableName, " as target ",
                    " using @Source as sc on ")
  for(i in c(1:length(constraints)))
  {
    if(i > 1) SQLSTR2 <- paste0(SQLSTR2, ' and ')
    SQLSTR2 <- paste0(SQLSTR2, "target.[", constraints[i] , "] = sc.[", constraints[i] ,"]")
  }
  
  # Si match, update les mesues
  SQLSTR3 <- paste0("WHEN matched then UPDATE set ")
  for(i in c(1:length(measures)))
  {
    if(i > 1) SQLSTR3 <- paste0(SQLSTR3, ', ')
    SQLSTR3 <- paste0(SQLSTR3, "target.[", measures[i] , "] = sc.[", measures[i] ,"]")
    
  }
  
  # Si pas de match, update les mesues
  cols <- ""
  vals <- ""
  for(i in c(1:length(def$COLUMN_NAME)))
  {
    if(i > 1) { cols <- paste0(cols, ', ') ; vals <- paste0(vals, ', ') }
    
    cols <- paste0(cols, "[", def$COLUMN_NAME[i] ,"]")
    vals <-  paste0(vals, "sc.[", def$COLUMN_NAME[i] ,"]")
    
  }
  
  SQLSTR4<- paste0("WHEN not matched then INSERT (", cols, ") VALUES (",vals,");")
  
  
  paste(SQLSTR,
        gsub(paste0("[", tableName , "]"), "@Source", insertClause, fixed = T),
        SQLSTR2,
        SQLSTR3,
        SQLSTR4)
}
