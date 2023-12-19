require('strict').on()

local log = require('log')
local constant = require('queue.constant')
local supervisor = require('queue.fibers.supervisor')
local fiber = require('fiber')

local tube = {}
tube.__index = tube

local drivers = {
    fifottl = require('queue.drivers.fifottl'),
    xfifottl = require('queue.drivers.xfifottl')
}

local function check_state()
    if (box.info.election.state == constant.LEADER and box.info.ro == false) then
        return true
    end
    if (box.cfg.election_mode == 'off' and box.info.ro == false) then
        return true
    end
    log.error("got data from service in bad mode")
    return false
end

tube.statistics = {}

function tube:stat(name, count)
    self.statistics[name] = (self.statistics[name] or 0) + count 
end

function tube.new(name, type_queue, opts)
    opts = opts or {}
    return setmetatable({
        driver = drivers[type_queue].new(name, opts),
        name   = name,
        ttr    = opts.ttr or constant.TTR_DEFAULT,
        ttl    = opts.ttl,
    },{
        __index = tube,
    })
end

function tube.create_tube(name, type_queue, opts)
    opts = opts or {}
    if box.space[name] then
        if not opts.if_not_exists then
            log.error("create exists tube without if_not_exists flag" .. name)
            return nil, false
        end
        return box.space[name], true
    end
    local driver = drivers[type_queue]
    if driver then
        return driver.create_tube(name, opts), false
    end
    return nil, false
end

function tube:start_worker()
    if not self.driver:support_workers() then
        return
    end
    local worker = supervisor.register_worker(self.name, function(cancel_func)
        fiber.self():name('worker_'..self.name)
        self.driver:start_worker(cancel_func)
    end)
    if box.info.election.state == 'leader' then
        supervisor.start_worker(worker)
    end
end

function tube:grant(user, args)
    if not check_state() then
        return
    end
    local function tube_grant_space(user, name, tp)
        box.schema.user.grant(user, tp or 'read,write', 'space', name, {
            if_not_exists = true,
        })
    end

    local function tube_grant_func(user, name)
        box.schema.func.create(name, { if_not_exists = true })
        box.schema.user.grant(user, 'execute', 'function', name, {
            if_not_exists = true
        })
    end

    args = args or {}

    tube_grant_space(user, 'queues', 'read')
    tube_grant_space(user, self.name)
    session.grant(user)

    if args.call then
        local prefix = (args.prefix or 'queue.tube') .. ('.%s:'):format(self.name)
        tube_grant_func(user, prefix .. 'put')
        tube_grant_func(user, prefix .. 'take')
        tube_grant_func(user, prefix .. 'touch')
        tube_grant_func(user, prefix .. 'ack')
        tube_grant_func(user, prefix .. 'release')
        tube_grant_func(user, prefix .. 'peek')
        tube_grant_func(user, prefix .. 'bury')
        tube_grant_func(user, prefix .. 'kick')
        tube_grant_func(user, prefix .. 'delete')
    end

    if args.truncate then
        local prefix = (args.prefix or 'queue.tube') .. ('.%s:'):format(self.name)
        tube_grant_func(user, prefix .. 'truncate')
    end
end

function tube:put(data, opts)
    if not check_state() then
        return nil
    end
    opts = opts or {}
    local task = self.driver:put(data, opts)
    self:stat('put',1)
    return self.driver:normalize_task(task)
end

function tube:take(timeout)
    if not check_state() then
        return nil
    end
    timeout = timeout and tonumber(timeout) or constant.TIMEOUT_TAKE_DEFAULT
    local task = self.driver:take()
    
    if task ~= nil then
        self:stat('take', 1)
        return self.driver:normalize_task(task)
    end
    
    local started = fiber.time() + timeout
    while true do
        task = self.driver:take()

        if task ~= nil then
            self:stat('take', 1)
            return self.driver:normalize_task(task)
        end
        if fiber.time() >= started then
            break
        end
        fiber.sleep(0.001)
    end
end

function tube:ack(id)
    if not check_state() then
        return nil
    end
    -- #TODO: add check is state is taken
    local result = self.driver:normalize_task(
        self.driver:ack(id)
    )
    self:stat('ack', 1)
    return result
end

function tube:release(id, opts)
    if not check_state() then
        return nil
    end
    self:stat('release', 1)
    return self.driver:normalize_task(self.driver:release(id, opts))
end

function tube:peek(id)
    if not check_state() then
        return nil
    end
    local task = self.driver:peek(id)
    if task == nil then
        error(("Task %s not found"):format(tostring(id)))
        return
    end
    self:stat('peek', 1)
    return self.driver:normalize_task(task)
end

function tube:bury(id)
    if not check_state() then
        return nil
    end
    local task = self.driver:bury(id)
    if task then
        self:stat('peek', 1)
    end
    return task and self.driver:normalize_task(task)
end

function tube:kick(count)
    if not check_state() then
        return nil
    end
    count = count or 1
    self:stat('kick', 1)
    return self.driver:kick(count)
end

function tube:delete(id)
    if not check_state() then
        return nil
    end
    self:stat('delete', 1)
    return self.driver:normalize_task(self.driver:delete(id))
end

-- drop tube
function tube:drop()
    if not check_state() then
        return nil
    end
    
    supervisor.drop_worker(self.name)

    box.space[self.name]:drop()
    box.space.queues:delete{self.name}
    return true
end

-- truncate tube
-- (delete everything from tube)
function tube:truncate()
    if not check_state() then
        return
    end
    self.driver:truncate()
end

return tube
