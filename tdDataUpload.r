
tdDataUpload <- function(connection, connection_fast = NULL, data.base.name = NULL, table.name, data, replace = T, fast = F, primaryIndex = NULL , partitionDate = NULL, volatile = F, na.default = NULL) {
  
  ##################
  #
  # version 4 - 2016.1.20
  # for fast load please provide second connection with connection string 
  # "jdbc:teradata://your-td-server/database=ACCESS_VIEWS,TYPE=FASTLOADCSV,tmode=ANSI,charset=UTF8,LOB_SUPPORT=OFF"
  #
  ##################
  
  jni.transform = function(setter, x) {
    switch(setter,
           'setInt' = as.integer(x),
           'setFloat' = .jfloat(x),
           'setDate' = .jnew("java/sql/Date",.jlong(as.numeric(as.POSIXct(x)))*1000),
           'setString' = as.character(x)
    )
  }
  
  tdExistsTable = function(dbname, tablename,connection) {
    sql = sprintf("select tablename, databasename from dbc.tables where databasename = '%s' and tablename = '%s'", toupper(dbname), tablename)
    result = dbGetQuery(connection,sql)
    return (nrow(result) > 0)
  }
  
  tdInsertPS = function(ps, df) {
    classes = unlist(lapply(df,data.class))
    numeric.indices = which(classes == 'numeric')
    if(length(numeric.indices) > 0) {
      int.indices = which(unlist(lapply(df[,numeric.indices,drop=F],is.integer)))
      classes[numeric.indices][int.indices] = 'integer'
    }
    java.setters = sapply(classes, function(i) {
      switch(i,
             'integer' = 'setInt',
             'numeric' = 'setFloat',
             'factor' = 'setString',
             'character' = 'setString',
             'Date' = 'setDate')
    })
    javatype = .jnew("java/sql/Types")
    java.typeint = sapply(classes, function(i) {
      switch(i,
             'integer' = .jfield(javatype, "I", "INTEGER"),
             'numeric' = .jfield(javatype, "I", "FLOAT"),
             'factor' = .jfield(javatype, "I", "VARCHAR"),
             'character' = .jfield(javatype, "I", "VARCHAR"),
             'Date' = .jfield(javatype, "I", "DATE"))
    })
    
    sstime <- Sys.time()
    for (i in 1:nrow(df)) {
      
      r = df[i,]
      for (j in 1:ncol(df)) {
        value = unlist(r[,j])
        if (is.null(value) | is.na(value) | is.nan(value)) {
          .jcall(ps,"V","setNull",as.integer(j), java.typeint[j])
        }
        else {
          .jcall(ps,"V",java.setters[j],as.integer(j), jni.transform(java.setters[j], value))
        }
      }
      .jcall(ps,"V","addBatch")
      if (i %% 10000 == 0) { .jcall(ps,"[I","executeBatch") ;cat(paste(format(i ,big.mark=",",scientific=FALSE),"rows inserted, ", format(Sys.time() - sstime) ,"per 10,000 rows. \n")); sstime <- Sys.time()}
    }
    if (! nrow(df) %% 10000 == 0) { .jcall(ps,"[I","executeBatch") }
    return (ps)
  }
  
  dbBuildTableDefinition =
    function (target.table, df, field.types = NULL, primaryIndex = NULL, partitionDate = NULL, volatile, pi_column, pd_column...)
    {
      if (is.null(field.types)) {
        field.types <- lapply(df, tdDataType)
      }
      flds <- paste(names(field.types), field.types)
      base = sprintf("CREATE MULTISET %s TABLE %s, no fallback, no before journal, no after journal (%s)", 
                     ifelse(volatile == T, "VOLATILE", "") ,
                     target.table,
                     paste(flds, collapse = ",\n"))
      if (!is.null(primaryIndex))
        base = sprintf("%s primary index(%s)", base, pi_column)
      if (!is.null(partitionDate))
        base = sprintf("%s PARTITION BY RANGE_N(%s BETWEEN DATE '2005-01-01' AND DATE '2020-12-31' EACH INTERVAL '1' DAY, NO RANGE)", base, pd_column)
      if (volatile == T)
        base = sprintf("%s ON COMMIT PRESERVE ROWS", base)
      return (base)
    }
  
  tdDataType = function (obj)
  {
    rs.class <- data.class(obj)
    rs.mode <- storage.mode(obj)
    if (rs.class %in% c("integer", "int")) "bigint"
    else switch(rs.class,
                character = "varchar(1024)",
                logical = "byteint",
                Factor = "varchar(1024)",
                Date = "DATE format 'YYYY-MM-DD'",
                POSIXct = "timestamp(0)",
                numeric = "float",
                "varchar(1024)")
  }
  
  # start, prep variable
  
  cat("\n")
  
  conn       <- connection
  data.base  <- data.base.name
  table.name <- table.name
  conn_f     <- connection_fast  
  data       <- data.frame(data)
  
  if(fast == T) {
    if(is.null(connection_fast)) {stop('connection_fast is needed for fastload. assign 2nd connection with "TYPE=FASTLOADCSV,tmode=ANSI" in connection script.')}
    if(is.null(primaryIndex)) {stop('primaryIndex is needed for large amount of data. assign column(s) to be PI with providing column number(s) , e.g., c(1:5).')}
    if(volatile == T) {stop('no volatile table for fastload')}
  }
  
  if(volatile == T){ target.table <- table.name } else { target.table <- paste0(data.base, ".", table.name) }
  
  colnames(data) <- gsub("\\.", "_", colnames(data))
  col.names <- colnames(data)
  
  if(fast == T) {
    replace <- TRUE
    
    if(sum(ifelse(is.na(data),1,0))>0){
      if(!is.null(na.default)) {
        data[is.na(data)] <- na.default
      } else {
        cat(paste(" source table has NA value(s) and no default value provided. record(s) will be dropped when using fastload. \n" , sep=""))
      }
    }
  }
  
  
  
  if(!is.null(primaryIndex)) {
    replace <- TRUE
    pi_column <- colnames(data)[primaryIndex]
    pi_column <- gsub("\\.", "_", pi_column)
    pi_column <- paste(pi_column, collapse=',')
  } else {pi_column <- NULL}
  
  if(!is.null(partitionDate)) {
    pd_column <- colnames(data)[partitionDate]
    pd_column <- gsub("\\.", "_", pd_column)
    pd_column <- paste(pd_column, collapse=',')
  } else {pd_column <- NULL}
  
  # check table if exist
  if(volatile != T){
    if(!tdExistsTable(data.base, table.name, conn)){
      cat(paste(target.table," doesn't exist, create new table. \n" , sep=""))
      dbSendUpdate(conn, dbBuildTableDefinition(target.table, data, NULL, primaryIndex, partitionDate, volatile, pi_column, pd_column))
    } else {
      if(replace == T){
        cat(paste(target.table," exists, drop and rebuild the table. \n" , sep=""))
        dbSendUpdate(conn, paste("drop table ",target.table,";", sep=""))
        dbSendUpdate(conn, dbBuildTableDefinition(target.table, data, NULL, primaryIndex, partitionDate, volatile, pi_column, pd_column))
      } else {
        cat(paste(target.table," exists, keep the table and append data. \n" , sep=""))
      }
    }
  } else {
    cat(paste("create volatile table ",target.table,". \n" , sep=""))
    dbSendUpdate(conn, dbBuildTableDefinition(target.table, data, NULL, primaryIndex, partitionDate, volatile, pi_column, pd_column))
  }
  
  if(fast == T){
    
    #in case last job failed 
    if(tdExistsTable(data.base, paste0(table.name,"_err_1"), conn)) dbSendUpdate(conn, paste0("drop table ",target.name,"_err_1"))
    if(tdExistsTable(data.base, paste0(table.name,"_err_2"), conn)) dbSendUpdate(conn, paste0("drop table ",target.name,"_err_2"))    
    if(tdExistsTable(data.base, paste0(table.name,"_ERR_1"), conn)) dbSendUpdate(conn, paste0("drop table ",target.name,"_ERR_1"))
    if(tdExistsTable(data.base, paste0(table.name,"_ERR_2"), conn)) dbSendUpdate(conn, paste0("drop table ",target.name,"_ERR_2"))
    
    sstime <- Sys.time()
    cat("fastload starting. \n")
    prepared.statement <- .jcall(conn_f@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", paste("insert into ",target.table," values (",paste(rep("?",length(col.names)), collapse=","),")", sep = ""))
    
    # write table to csv (TODO: write to pipe?)
    cat("fastload preparing. \n")
    my.csv.file <- tempfile()
    on.exit(unlink(my.csv.file))
    
    # colnames is required in the CSV (or the 1st line is ignored)
    write.table(data, file = my.csv.file, sep = ",", quote = FALSE, row.names = FALSE, na="?")
    
    java.file   <- .jnew("java/io/File", my.csv.file)
    java.stream <- .jnew("java/io/FileInputStream", java.file)
    
    # upload the data
    prepared.statement$setAsciiStream(1L, java.stream)
    cat("fastload uploading. \n")
    
    prepared.statement$executeUpdate()
    cat(paste0("fastload done. ",  format(nrow(data),big.mark=",",scientific=FALSE) , " rows inserted in ", format(Sys.time() - sstime), ". \n"))
    
    cat("fastload cleaning.")
    
    # let's erase the crap that the fastload leaves behind
    dbSendUpdate(conn, paste0("drop table ", data.base, ".", table.name, "_ERR_1;"))
    dbSendUpdate(conn, paste0("drop table ", data.base, ".", table.name, "_ERR_2;"))
    
  } else {
    # column number
    
    sstime <- Sys.time()
    cat("upload starting. \n")
    .jcall(conn@jc,"V","setAutoCommit",FALSE)
    ps = .jcall(conn@jc,"Ljava/sql/PreparedStatement;","prepareStatement", paste("insert into ",target.table," values (",paste(rep("?",length(col.names)), collapse=","),")", sep = ""))
    
    ps = tdInsertPS(ps, data)
    dbCommit(conn)
    
    # close 
    .jcall(ps,"V","close")
    cat(paste0("upload done. ", format(nrow(data),big.mark=",",scientific=FALSE) , " rows inserted in ", format(Sys.time() - sstime), ". \n"))
    cat("upload cleaning.")
    .jcall(conn@jc,"V","setAutoCommit",TRUE)
    
  }
  
  # ending
  cat("\n\n")
}

