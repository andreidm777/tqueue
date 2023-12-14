require('strict').on()

local constant = require('queue.constant')
local state = require('queue.state')
local fiber  = require('fiber')
local log = require('log')

tube={}
tube.__index=tube

local FIFOTTL_FORMAT = {
    {name = 'task_id', type = 'unsigned'},
    {name = 'status', type = 'string'},
    {name = 'ttl', type = 'number'},
    {name = 'ready_at', type = 'number'},
    {name = 'ttr', type = 'number'},
    {name = 'pri', type = 'number'},
    {name = 'data', type = '*'}
}

local i_task_id  = 1
local i_status   = 2
local i_ttl      = 3
local i_ready_at = 4
local i_ttr      = 5
local i_pri      = 6
local i_data     = 7

function tube.create_tube(name, opts)
    local space_opts         = {}
    opts = opts or {}
    local if_not_exists      = opts.if_not_exists or false

    space_opts.format = FIFOTTL_FORMAT

    local space = box.space[name]
    if if_not_exists and space then
        -- Validate the existing space.
        return space
    end

    space = box.schema.create_space(name, space_opts)
    space:create_index('primary', {
        unique        = true,
        type          = 'tree',
        parts         = {i_task_id, 'unsigned'}
    })
    space:create_index('status', {
        type          = 'tree',
        parts         = {i_status, 'string', i_ready_at, 'number', i_pri, 'number'}
    })
    space:create_index('ttl', {
        type          = 'tree',
        parts         = {i_ttl, 'number'}
    })
    return space
end

function tube.new(name, opts)
    return setmetatable({
        name = name,
        space = box.space[name],
        ttr    = opts.ttr or constant.TTR_DEFAULT,
        ttl    = opts.ttl or constant.MAX_TIMEOUT,
    },
    { __index = tube })
end

function tube:start_worker(cancel_func)
    while true do        
        local now = fiber.time()
        -- remove ttl expired task
        for _, t in self.space.index.ttl:pairs({now}, {iterator = box.index.LT}) do
            if cancel_func() then
                return
            end
            self.space:delete(t[i_task_id])
            fiber.sleep(0)
        end
        -- return to ready ttr files
        for _, t in self.space.index.status:pairs({state.TAKEN, now}, {iterator = box.index.LT}) do
            if cancel_func() then
                return
            end
            self.space:update({ t[i_task_id]}, {
                { '=' , i_status, state.READY },
            })
            fiber.sleep(0)
        end
        fiber.sleep(0.01)
        -- # TODO: return in work taken task by ttr (state = TAKEN)
    end
end

function tube:put(data, opts)
    opts = opts or {}

    local now = fiber.time()

    local max = self.space.index.primary:max()
    local id = max and max[i_task_id] + 1 or 0

    local tuple  = {
        id,
        state.READY,
        now + (opts.ttl and tonumber(opts.ttl) or self.ttl),
        now + (opts.delay and tonumber(opts.delay) or 0),
        opts.ttr and tonumber(opts.ttr) or self.ttr,
        opts.pri and tonumber(opts.pri) or 0,
        data,
    }
    
    return self.space:insert(tuple)
end

function tube:take(timeout)
    local task = nil
    local now = fiber.time()
    for _, t in self.space.index.status:pairs({state.READY}) do
        if t[i_ready_at] > now then
            -- task with delay
            break
        end
        if not is_expired(t) then
            task = t
            break
        end
        fiber.sleep(0.0001)
    end

    if task == nil then
        return
    end
    
    task = self.space:update(task[i_task_id], {
        { '=', i_status, state.TAKEN },
        { '=', i_ready_at, now + (task[i_ttr] or self.ttr)  },
    })
    return task
end

function tube:ack(id)
    return self.space:delete(id)
end

function tube:release(id, opts)
    local task = self.space:get{id}
    if task == nil then
        return
    end
    
    task = self.space:update(id, {
        { '=', i_status, state.READY },
        { '=', i_ready_at, task[i_ready_at] + (opts.delay and tonumber(opts.delay) or 0) }
    })
    
    return task
end

function tube:peek(id)
    return self.space:get{id}
end

function tube:bury(id)
    local task = self:peek(id)
    if not task then
        return
    end
    if task[i_status] == state.BURIED then
        return task
    end
    task = self.space:update(task[i_task_id], {
        { '=', i_status, state.BURIED },
        { '=', i_ready_at, task[i_ready_at] + self.ttl },
    })
    return task
end

function tube:kick(count)
    for i = 1, count do
        local task = self.space.index.status:min{ state.BURIED }
        if task == nil then
            return i - 1
        end
        if task[i_status] ~= state.BURIED then
            return i - 1
        end

        task = self.space:update(task[i_task_id], {
            { '=', i_status, state.READY },
            { '=', i_ready_at, fiber.time() },
        })
    end
    return count
end

function tube:delete(id)
    return self.space:delete(id)
end

function tube:truncate()
    self.space:truncate()
end

function tube:normalize_task(task)
    return task and task:transform(3, 4)
end

function tube:support_workers()
    return true
end

function is_expired(task)
    return (task[i_ttl] <= fiber.time())
end

return tube