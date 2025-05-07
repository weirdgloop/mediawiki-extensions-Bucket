bucket = {}
local php

function bucket.setupInterface( options )
    -- Remove setup function
    bucket.setupInterface = nil

    -- Copy the PHP callbacks to a local variable, and remove the global
    php = mw_interface
    mw_interface = nil

    -- Do any other setup here

    -- Install into the mw global
    mw = mw or {}
    mw.ext = mw.ext or {}
    mw.ext.bucket = bucket
    mw.bucket = bucket

    -- Indicate that we're loaded
    package.loaded['mw.ext.bucket'] = bucket
end

-- function bucket.put(bucket_name, data)
--     return php.put(bucket_name, data)
-- end

local QueryBuilder = {}

function QueryBuilder:new(tableName)
    -- set everything up here...
    local queryBuilder = {
        tableName = tableName, --TODO Force lower case and replace spaces with underscores
        selects = {},
        wheres = {op = "AND", operands = {}},
        categories = {op = "AND", operands = {}},
        joins = {},
        orderBy = nil,
        subversion = ""
    }
    setmetatable(queryBuilder, self)
    self.__index = function(tbl, key)
        return function(...)
            return QueryBuilder[key](tbl, ...)
        end
    end
    return queryBuilder
end

function QueryBuilder:select(...)
    self.selects = {...}
    return self
end

function QueryBuilder:where(...)
    table.insert(self.wheres.operands, {...})
    return self
end

function QueryBuilder:whereCategory(condition)
    table.insert(self.categories.operands, condition)
    return self
end

--Put selects in the normal select function, prepended with the table name.
--tableName is the string of a table to join
--cond is an array of two elements, which are the two columns to join on.
function QueryBuilder:join(tableName, cond)
    table.insert(self.joins, {tableName = tableName, cond = cond})
    return self
end

function QueryBuilder:limit(arg)
    self.limit = arg
    return self
end

function QueryBuilder:offset(arg)
    self.offset = arg
    return self
end

function QueryBuilder:orderBy(fieldName, direction)
    -- TODO throw an error if we set order by twice?
    self.orderBy = {fieldName = fieldName, direction = direction}
    return self
end

function QueryBuilder:run()
    return php.run(self)
end

function QueryBuilder:sub(identifier)
    self.subversion = identifier
    return self
end

function QueryBuilder:put(data)
    return php.put(self.tableName, self.subversion, data)
end

function bucket.put(bucket_name, data)
    return php.put(bucket_name, '', data)
end

function bucket.Or(...)
    return {op = "OR", operands = {...}}
end

function bucket.And(...)
    return {op = "AND", operands = {...}}
end

function bucket.Not(arg)
    return {op = "NOT", operand = arg}
end

setmetatable(bucket, {
    __call = function(tbl, tableName)
        return QueryBuilder:new(tableName)
    end
})

return bucket
