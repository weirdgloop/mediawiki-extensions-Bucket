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
        joins = {}
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

function QueryBuilder:join(tableName, fieldName, selectFields)
    table.insert(self.joins, {tableName = tableName, fieldName = fieldName, selectFields = selectFields})
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

function QueryBuilder:run()
    return php.run(self)
end

function QueryBuilder:runJson()
    return php.runJson(self)
end

function bucket.put(bucket_name, data)
    return php.put(bucket_name, data)
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
