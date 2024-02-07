module HtmlTables
    using SQLite
    using DataFrames
    using Tables

    export htmlTable, executeInsert, retrievePKtuplesForTable, htmlTableForQuery
    
    """
    getDbPath() will retrieve the path of the target db from the environment variable "htmltables_dbpath".
    """
    function getDbPath()
        dbpath = joinpath(ENV["htmltables_dbpath"])
        return dbpath
    end
    """
    Annotation to be completed
    """
    function htmlTableHeaders(SqlResult)
        function retrieveHeaderNames(sqlResult)
            headerNames = []
            for name in SqlResult.names
                push!(headerNames, string(name))    
            end
            headerNames
        end

        headerNames = retrieveHeaderNames(SqlResult)
        
        htmlHeaderRow = "<tr>"
        for headerName in headerNames
            header = "<th>"*headerName*"</th>"
            htmlHeaderRow = htmlHeaderRow * header
        end
        result = htmlHeaderRow*"</tr>"

        return result
    end
    """
    Annotation to be completed
    """
    function htmlTableData(SqlResult)
        df = DataFrame(SqlResult)
        tbl = Tables.rowtable(df)
        
        result = ""
        
        for row in Tables.rows(tbl)
            htmlDataRow = "<tr>"
            for field in row
                htmlDataRow = htmlDataRow*"<td>"*string(field)*"</td>"
            end
            htmlDataRow = htmlDataRow*"</tr>"
            result = result * htmlDataRow
        end 
        return result
    end
    """
    The function retrievePKtuplesForTable(tableName) takes an argument tableName: a table present in the target database and returns an array that contains string-joined uniquely identifying attribute values that identify the tuples present in the table. The function is designed to serve as an input for an iteration that generates html-form options. The inteded use is to create html pages allowing to insert relationships between entities that exist in two or more tables.
    """
    function retrievePKtuplesForTable(tableName)
        dbpath = getDbPath()
        # Part 1: determine for a provided table which are the primary key field lists
        
        SqlStatement  = "SELECT l.name FROM pragma_table_info('$tableName') as l WHERE l.pk <> 0;"
        databaseConnection = SQLite.DB(dbpath)
        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)
        
        df = DataFrame(SqlResult)
        tbl = Tables.rowtable(df)
        
        fieldList = ""
        for row in Tables.rows(tbl)
            if fieldList == ""
                fieldList = string(row[1])
            else
                fieldList = fieldList * ", " * string(row[1])
            end
        end
        
        # Part 2: get the PK attribute tuples in that table
        
        SqlStatement = "SELECT $fieldList FROM $tableName"
        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)

        df = DataFrame(SqlResult)
        tbl = Tables.rowtable(df)

        pkTuples = []
        for row in Tables.rows(tbl)
            arrayEl = ""
            for field in row
                if arrayEl == ""
                    arrayEl = string(field)
                else
                    arrayEl = arrayEl * "_" * string(field)
                end
            end
            push!(pkTuples, arrayEl)
        end 
        pkTuples
    end
    """
    The function htmlTable(tableName) takes an argument tableName: a table present in the target database and returns a well-formatted html table containing all tuples present in that table. Each tuple contains all attributes. It depends on the internal functions htmlTableHeaders() and htmlTableData() for the construction of the header row an the tuples rows.
    """
    function htmlTable(tableName)#,dbpath)
        dbpath = getDbPath()
        
        databaseConnection = SQLite.DB(dbpath)
        
        SqlStatement = "SELECT * FROM $tableName"
        
        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)

        headers = htmlTableHeaders(SqlResult)
        data = htmlTableData(SqlResult)
        return "<table>"*headers*data*"</table>"
    end
    """
    The function htmlTableForQuery(SqlStatement) takes an argument SqlStatement: an SQL statement that will be executed against the database. Given the SQL statement was a SELECT ... FROM ... it returns a well-formatted html table containing all tuples returned by the query.  It depends on the internal functions htmlTableHeaders() and htmlTableData() for the construction of the header row an the tuples rows.

    WARNING: there is no checking of the type of query provided so this couls lead to malicious operations against the database.
    """
    function htmlTableForQuery(SqlStatement)
        dbpath = getDbPath()
        databaseConnection = SQLite.DB(dbpath)
        
        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)

        headers = htmlTableHeaders(SqlResult)
        data = htmlTableData(SqlResult)
        return "<table>"*headers*data*"</table>"
    end
    """
    Annotation to be completed
    """
    function executeInsert(SqlStatement)
        dbpath = getDbPath()
        databaseConnection = SQLite.DB(dbpath)

        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)
        return SqlResult
    end
end
