### Function for inserting data into a SQL Server database ###
## Before calling this function, wrangle your data frame so that column names and data types match those of the database table you are
## inserting into.
## df:	tibble; the data frame you are inserting into the database
## table.name:	string; the name of the database table, including schema. Format as schema.tablename (e.g. data.Visit)
## conn:	pool object; database connection (generated using pool package, see the code below this fxn for an example)
## has.guid:	boolean; does the data to be inserted have a GUID?
## keep.guid:	boolean; should the GUID be stored permanently in the database?
## col.guid:	string; the name of the GUID column (must be same in df and in database table if keep.guid is TRUE) (ignored if has.guid is FALSE)
## cols.key:	named list of key column(s) in the database table

uploadData <- function(df, table.name, conn, has.guid = TRUE, keep.guid = FALSE, col.guid = "GlobalID", cols.key = list(ID = integer())) {
  # Build SQL statements
  sql.insert <- ""
  sql.before <- ""
  sql.after <- ""
  
  colnames.key <- names(cols.key)
  
  if (has.guid & keep.guid) {  # GUID permanently stored in DB
    cols <- names(df)  # Assume names of columns, incl. GUID column, match those in the database exactly
    tcols <- paste0("t.", cols) # target columns
    scols <- paste0("s.", cols) # source survey123 columns
    updateSet <- paste0(tcols," = ",scols) # matching the column names
    
    cols <- paste(cols, collapse = ", ")
    tcols <- paste(tcols, collapse = ", ")
    scols <- paste(scols, collapse = ", ")
    updateSet <- paste(updateSet, collapse = ", ")
    
    placeholders <- rep("?", length(names(df)))
    placeholders <- paste(placeholders, collapse = ", ")
    sql.insert <- paste0("MERGE ", table.name, " t USING dbo.TempSource s ","ON (s.GlobalID = t.GlobalID) WHEN MATCHED THEN UPDATE SET ", updateSet," WHEN NOT MATCHED BY TARGET THEN INSERT (", cols, ") VALUES (", scols, ") ",
                         "OUTPUT ", paste0("INSERTED.", colnames.key, collapse = ", "), ", INSERTED.", col.guid, " INTO InsertOutput ;")
    

  } else if (has.guid & !keep.guid) {  # Create temporary GUID column in order to return a GUID-ID crosswalk
    cols <- names(df)
    cols[grep(col.guid, cols)] <- "GUID_DeleteMe"  # Replace GUID column name to make clear that it is temporary
    cols <- paste(cols, collapse = ", ")
    
    placeholders <- rep("?", length(names(df)))
    placeholders <- paste(placeholders, collapse = ", ")
    sql.insert <- paste0("INSERT INTO ", table.name, "(", cols, ") ",
                         "OUTPUT ", paste0("INSERTED.", colnames.key, collapse = ", "), ", INSERTED.GUID_DeleteMe INTO InsertOutput ",
                         "VALUES (",
                         placeholders,
                         ") ")
    sql.before = paste0("ALTER TABLE ", table.name, " ADD GUID_DeleteMe uniqueidentifier")
    sql.after = paste0("ALTER TABLE ", table.name, " DROP COLUMN GUID_DeleteMe")
    
  } else if (!has.guid) {  # No GUID at all
    cols <- names(df)  # Assume names of columns, incl. GUID column, match those in the database exactly
    cols <- paste(cols, collapse = ", ")
    
    placeholders <- rep("?", length(names(df)))
    placeholders <- paste(placeholders, collapse = ", ")
    sql.insert <- paste0("INSERT INTO ", table.name, "(", cols, ") ",
                         "OUTPUT ", paste0("INSERTED.", colnames.key, collapse = ", "), " INTO InsertOutput ",
                         "VALUES (",
                         placeholders,
                         ") ")
  }
  
  print(sql.insert)# testing
  print(scols) # testing
  
  sql.inserted <- "SELECT * FROM InsertOutput"
  
  
  
  # # insert temp target table - for testing. The merge works but not with the insert output
  # poolWithTransaction(pool = conn, func = function(conn) {
  #   #dbCreateTable(conn, "TempSource", df)
  #   #dbAppendTable(conn, "TempSource", df)
  #   dbRemoveTable(conn, "TempSource")
  # 
  #   #qry <- dbSendQuery(conn, sql.insert)
  #   #dbBind(qry, as.list(df))
  #   #dbBind(qry, df)
  #   #dbFetch(qry)
  #   #dbClearResult(qry)
  #   })
  

  
  # Perform insert
  keys <- tibble()
  keys <- poolWithTransaction(pool = conn, func = function(conn) {
    temp.types <- cols.key
    temp.types[[col.guid]] <- character()
    temp.table <- tibble(!!!temp.types)
    dbCreateTable(conn, "InsertOutput", temp.table)
    # add temp tables
    dbCreateTable(conn, "TempSource", df)
    dbAppendTable(conn, "TempSource", df)

    # If needed, create a temporary column to store the GUID
    if (str_length(sql.before) > 0) {
      dbSendQuery(conn, sql.before)
    }

    qry <- dbSendQuery(conn, sql.insert)
    #dbBind(qry, as.list(df))
    dbFetch(qry)
    dbClearResult(qry)

    res <- dbSendQuery(conn, sql.inserted)
    inserted <- dbFetch(res) %>%
      as_tibble()
    dbClearResult(res)

    dbRemoveTable(conn, "InsertOutput")

    # If needed, delete the temporary GUID column
    if (str_length(sql.after) > 0) {
      dbSendQuery(conn, sql.after)
    }

    return(inserted)
  })

  if (!has.guid) {
    keys <- select(keys, ID)
  }
  
  keys[col.guid] <- tolower(keys[[col.guid]])
  return(keys)
}
