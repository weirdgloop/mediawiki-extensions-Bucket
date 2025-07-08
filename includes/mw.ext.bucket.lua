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

function QueryBuilder:new(bucketName)
    if not isPossibleField(bucketName) then
        printError('bucket-invalid-name-warning', 5, bucketName)
    end
    -- set everything up here...
    local queryBuilder = {
        bucketName = bucketName,
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
    local args = standardizeWhere({...})
    table.insert(self.wheres.operands, args)
    return self
end

-- There are multiple accepted formats for where conditions.
-- This function takes any accepted condition, and outputs a condition formatted as {field, operator, value}
-- .where{{"a", ">", 0}, {"b", "=", "5"}})
-- .where("Category:Foo")
-- .where(a = 1, b = 2)
-- .where("id", 100)
-- .where("id", ">", 100)
function standardizeWhere(...)
    local val = ...

    if hasOp(val) then
        if val['operands'] then
            local children = {}
            for k, operand in pairs(val['operands']) do
                if k ~= 'op' then
                    -- If we have a single child that is just an array of operands, then it is equivalent to the op being set on the child
                    if (not operand['op']) and val['op'] and type(operand) == "table" and type(operand[1]) == "table" and #operand[1] > 0 then
                        operand['op'] = val['op']
                    end
                    table.insert(children, standardizeWhere(operand))
                end
            end
            if #children > 1 then
                return {op = val['op'], operands = children}
            else
                -- Both AND or OR don't apply if there is only a single child
                return children[1]
            end
        else
            local operand = standardizeWhere(val['operand'])
            if type(operand) ~= "table" then
                operand = {operand}
            end
            return {op = 'NOT', operand = operand}
        end
    elseif type(val) == "table" then
        if #val > 0 and type(val[1]) == "table" then
            -- .where{{"a", ">", 0}, {"b", "=", "5"}})
            local op = val['op'] and val['op'] or 'AND' -- Apply implicit AND
            return standardizeWhere({op = op, operands = val})
        elseif #val == 0 then -- The # operator only counts unnamed variables
            -- .where({a = 1, b = 2})
            local operands = {}
            for k, v in pairs(val) do
                table.insert(operands, {k, '=', v})
            end
            if #operands > 1 then
                return standardizeWhere({op = 'AND', operands = operands})
            else
                return standardizeWhere(operands[1])
            end
        elseif val[1] and val[2] then
            -- .where({a, "foo"})
            if #val == 2 then
                return {val[1], '=', val[2]}
            else
                if val[3] == bucket.Null() and val[2] ~= '=' and val[2] ~= '!=' then
                    printError('bucket-query-null-invalid-operator', 5, val[2] or '')
                end
                return {val[1], val[2], val[3]}
            end
        end
    else -- If we aren't a table
        if type(val) == "string" then
            -- .where("Category:Foo")
            if not isCategory(val) then
                printError('bucket-schema-invalid-field-name', 5, val or '')
            end
            return val
        end
    end
    printError('bucket-query-where-confused', 5, val or '')
end

function hasOp(condition)
    return type(condition) == "table"
        and condition['op']
        and ( condition['operands'] or condition['operand'] )
end

--bucketName is the string of a bucket to join, with fieldOne == fieldTwo being the join condition.
function QueryBuilder:join(bucketName, fieldOne, fieldTwo)
    assertPossibleField(bucketName)
    assertPossibleField(fieldOne)
    assertPossibleField(fieldTwo)

    local bucketOne = string.match(fieldOne, '([^%.]+)%.') --Grabs the bucket name, or nil if none is specified.
    local bucketTwo = string.match(fieldTwo, '([^%.]+)%.')

    if bucketOne == bucketTwo then -- We cannot join a bucket with itself
        printError('bucket-query-invalid-join', 5, mw.text.jsonEncode({bucketName, fieldOne, fieldTwo}))
    end

    if bucketOne ~= bucketName and bucketTwo ~= bucketName then -- One of the fields must be for the joined table
        printError('bucket-query-invalid-join', 5, mw.text.jsonEncode({bucketName, fieldOne, fieldTwo}))
    end

    table.insert(self.joins, {bucketName = bucketName, cond = {fieldOne, fieldTwo}})
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
    if type(data) ~= 'table' then
        printError('bucket-put-syntax-error', 5)
    end
    for field, value in pairs(data) do
        assertPossibleField(field)
        if field == '_index' or field == '_page_id' or field == 'page_name' or field == 'page_name_sub' then
            printError('bucket-put-system-field', 5, field)
        end
        if type(value) == 'function' then
            printError('bucket-put-function-value', 5)
        end
    end
    php.put(self, data)
end

function bucket.Or(...)
    return {op = "OR", operands = {...}}
end

function bucket.And(...)
    return {op = "AND", operands = {...}}
end

function bucket.Not(...)
    return {op = "NOT", operand = {...}}
end

function bucket.Null()
    return "&&NULL&&"
end

setmetatable(bucket, {
    __call = function(tbl, bucketName)
        return QueryBuilder:new(bucketName)
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
