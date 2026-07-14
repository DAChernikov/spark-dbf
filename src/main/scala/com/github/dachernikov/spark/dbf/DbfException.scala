package com.github.dachernikov.spark.dbf

/** Base exception for DBF datasource failures with user-facing diagnostics. */
class DbfException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

