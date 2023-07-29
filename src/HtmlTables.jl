module HtmlTables
    using SQLite
    using DataFrames
    using Tables

    export htmlTable, executeInsert, retrievePKtuplesForTable, htmlTableForQuery

    DATABASEFILE = ENV["databasefilepath"]
    
    #DATABASEFILE = "/Users/pietvanderpaelt/sc622webApp/weaponshop.sqlite"


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

    function retrievePKtuplesForTable(tableName)
        # Part 1: determine for a provided table which are the primary key field lists
        
        SqlStatement  = "SELECT l.name FROM pragma_table_info('$tableName') as l WHERE l.pk <> 0;"
        #DATABASEFILE = "C:\\Users\\van der paelt.p\\My Drive\\SC622\\SC622_AY22-23\\Databases\\weaponshop.sqlite"
        databaseConnection = SQLite.DB(DATABASEFILE)
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

    function htmlTable(tableName)
        
        
        databaseConnection = SQLite.DB(DATABASEFILE)
        
        SqlStatement = "SELECT * FROM $tableName"
        
        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)

        headers = htmlTableHeaders(SqlResult)
        data = htmlTableData(SqlResult)
        return "<table>"*headers*data*"</table>"
    end

    function htmlTableForQuery(SqlStatement)
        
        databaseConnection = SQLite.DB(DATABASEFILE)
        
        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)

        headers = htmlTableHeaders(SqlResult)
        data = htmlTableData(SqlResult)
        return "<table>"*headers*data*"</table>"
    end

    function executeInsert(SqlStatement)
        databaseConnection = SQLite.DB(DATABASEFILE)

        SqlResult = DBInterface.execute(databaseConnection, SqlStatement)
        return SqlResult
    end
end
