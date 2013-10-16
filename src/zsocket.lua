--[[----------------------------------------------------------------------------

zsocket.lua

    @author Christophe Berbizier (cberbizier@peersuasive.com)
    @license GPLv3
    @copyright 


(c) 2013, Peersuasive Technologies

------------------------------------------------------------------------------]]

--[[ REQUIREMENTS ]]------------------------------------------------------------

local zmq = require "zmq"

--[[ MODULE ]]------------------------------------------------------------------

local meta = {}
meta.__index = meta


--[[ METHODS ]]-----------------------------------------------------------------

--[[--
connect to redis socket and set ID
--]]
local function connect(self, addr)
    -- ... connect
    local ok, err = self.socket:connect( addr )
    if not ok then return nil, err end
    self.socket_id = self.socket:getopt(zmq.IDENTITY)
    return ok
end

--[[--
send command to redis with socket id
--]]
local function send(self, data)
    self.socket:send( self.socket_id, zmq.SNDMORE )
    self.socket:send( string.format(data), zmq.SNDMORE )
end

--[[--
read data and cache unused data, zmq won't keep them waiting the way a BSD socket would
--]]
local function receive(self, len, ...)
    if len == nil then len = '*l' end
    local line = nil
    if not (self.cache and #self.cache>0) then
        -- discard ID
        self.socket:recv()
        self.cache, err = self.socket:recv()
    end
    if ("*l"==len) then
        local pos, xpos = self.cache:find("\r?\n")
        pos, xpos = pos or 0, xpos or 0
        local margin = xpos - pos
        line = self.cache:sub( 0, pos-1 )
        self.cache = self.cache:sub(pos+1+margin)
    elseif ("*a"==len) then
        line = self.cache
        self.cache = ""
    elseif ("number"==type(len)) then
        line = self.cache:sub(0, len)
        self.cache = self.cache:sub( len+1 )
    else
        error("Can't understand len type: " .. tostring(len))
    end

    if not err then 
        return line 
    else
        return nil, err
    end
end

--[[--
close connection, terminate context
--]]
local function close(self, ...)
    self.socket:setopt(zmq.LINGER, 0)
    self.socket:close()
    self.context:term()
    self.cache = nil
end


--[[--
init socket
--]]
local function __call(self, ...)
    local self = {}
    self.connect    = connect
    self.send       = send
    self.receive    = receive
    self.close      = close
    self.shutdown   = close
    self.context    = zmq.init(1)
    self.socket     = self.context:socket( zmq.STREAM )
    self.socket_id  = ""
    self.cache      = ""

    return self
end

meta.__call = __call
local xmeta = setmetatable( {}, meta )

module(...)
return xmeta
