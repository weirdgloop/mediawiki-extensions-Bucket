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

local QueryBuilder = {}

function QueryBuilder:new(tableName)
    -- set everything up here...
    local queryBuilder = {
        tableName = tableName,
        selects = {},
        wheres = {op = "AND", operands = {}},
        joins = {},
        orderBy = nil,
        subversion = "",
        debug = false
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
    for k, v in pairs({...}) do
        assertPossibleSelect(v)
        table.insert(self.selects, v)
    end
    return self
end

function QueryBuilder:where(...)
    --TODO parse the incoming where.
    table.insert(self.wheres.operands, {...})
    return self
end

--Put selects in the normal select function, prepended with the table name.
--tableName is the string of a table to join
function QueryBuilder:join(tableName, columnOne, columnTwo)
    assertPossibleField(tableName)
    assertPossibleField(columnOne)
    assertPossibleField(columnTwo)

    local bucketOne = string.match(columnOne, '([^%.]+)%.') --Grabs the bucket name, or nil if none is specified.
    local bucketTwo = string.match(columnTwo, '([^%.]+)%.')

    if bucketOne == bucketTwo then -- We cannot join a table with itself
        printError('bucket-query-invalid-join', 5, mw.text.jsonEncode({tableName, columnOne, columnTwo}))
    end

    table.insert(self.joins, {tableName = tableName, cond = {columnOne, columnTwo}})
    return self
end

function QueryBuilder:limit(arg)
    if type(arg) ~= "number" then
        printError('bucket-query-must-be-numeric', 5, 'limit()')
    end
    self.limit = arg
    return self
end

function QueryBuilder:offset(arg)
    if type(arg) ~= "number" then
        printError('bucket-query-must-be-numeric', 5, 'offset()')
    end
    self.offset = arg
    return self
end

function QueryBuilder:orderBy(fieldName, direction)
    -- TODO parse
    self.orderBy = {fieldName = fieldName, direction = direction}
    return self
end

function QueryBuilder:print_sql()
    self.debug = true
    return self
end

function QueryBuilder:run()
    local result = php.run(self)

    if type(result) == "table" then
        if self.debug then
            mw.log(result[2])
        end
        return result[1]
    else
        error(result, 3) -- Specify that the erroring code is 3 calls up the chain, which is the user facing module
        return nil
    end
end

function QueryBuilder:sub(identifier)
    self.subversion = identifier
    return self
end

function QueryBuilder:put(data)
    -- TODO parse
    php.put(self, data)
end

function bucket.Or(...)
    -- TODO parse
    return {op = "OR", operands = {...}}
end

function bucket.And(...)
    -- TODO parse
    return {op = "AND", operands = {...}}
end

function bucket.Not(...)
    -- TODO parse
    return {op = "NOT", operand = {...}}
end

function bucket.Null()
    return "&&NULL&&"
end

setmetatable(bucket, {
    __call = function(tbl, tableName)
        return QueryBuilder:new(tableName)
    end
})

-- Validation functions
function assertPossibleField(fieldName)
    if not php.isPossibleField(fieldName) then
        printError('bucket-invalid-name-warning', 5, fieldName)
    end
end

function assertPossibleSelect(fieldName)
    if (not php.isCategory(fieldName)) and (not php.isPossibleField(fieldName)) then
        printError('bucket-invalid-name-warning', 5, fieldName)
    end
end

function printError(msg, depth, ...)
    error(mw.message.new(msg, ...):plain(), depth)
end

return bucket
