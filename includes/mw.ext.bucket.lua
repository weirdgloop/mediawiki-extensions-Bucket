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
        assertPossibleField(v)
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

    if bucketOne ~= tableName and bucketTwo ~= tableName then -- One of the columns must be for the joined table
        printError('bucket-query-invalid-join', 5, mw.text.jsonEncode({tableName, columnOne, columnTwo}))
    end

    table.insert(self.joins, {tableName = tableName, cond = {columnOne, columnTwo}})
    return self
end

function QueryBuilder:limit(arg)
    if type(arg) ~= "number" then
        printError('bucket-query-must-be-type', 5, 'limit()', 'number')
    end
    self.limit = arg
    return self
end

function QueryBuilder:offset(arg)
    if type(arg) ~= "number" then
        printError('bucket-query-must-be-type', 5, 'offset()', 'number')
    end
    self.offset = arg
    return self
end

-- Direction is optional, defaults to ASC
function QueryBuilder:orderBy(fieldName, direction)
    assertPossibleField(fieldName)
    if direction == nil then
        direction = "ASC" -- Default value
    end
    local originalDirection = direction
    local direction = string.upper(direction)
    if direction ~= "ASC" and direction ~= "DESC" then
        printError('bucket-query-order-by-direction', 5, originalDirection)
    end
    self.orderBy = {fieldName = fieldName, direction = direction}
    return self
end

function QueryBuilder:printSQL()
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
    if type(identifier) ~= 'string' then
        printError('bucket-query-must-be-type', 5, 'sub()', 'string')
    end
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
-- We cannot definitively validate while in lua, since its always possible 
-- to modify the QueryBuilder data structures directly from untrusted lua code
-- However this validation allows us to return meaningful error messages to users instead of waiting for .run()
function assertPossibleField(fieldName)
    if (not isCategory(fieldName)) and (not isPossibleField(fieldName)) then
        printError('bucket-schema-invalid-field-name', 5, fieldName or '')
    end
end

-- This is equivalent to Bucket.php field name validation, but is kept in lua for performance.
function isPossibleField(fieldName)
    if fieldName and type(fieldName) == 'string' and string.match(fieldName, '^[a-zA-Z0-9_.]+$') then
        return true
    end
    return false
end

function isCategory(fieldName)
    if fieldName and type(fieldName) == 'string' and string.match(fieldName, '^Category:') then
        return true
    end
    return false
end

-- Return a lua error using a mediawiki message
function printError(msg, depth, ...)
    error(mw.message.new(msg, ...):plain(), depth)
end

return bucket
