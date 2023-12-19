require('strict').on()

local fiber = require('fiber')
local log = require('log')

local constant = require('queue.constant')

local PENDING = 'pending'
local RUNNING = 'running'

local M = {}
M.__index = M

M.in_reload = false

M.workers = {}

function M.register_worker(name, func)
    if M.workers[name] then
        log.warn(name.." allready exists")
        return M.workers[name]
    end

    M.workers[name] = {
            name = name,
            func = func,
            state = PENDING,
    }

    return M.workers[name]
end

function M.cancel(worker)
    return function()
        if fiber.testcancel() then
            worker.chan:put(true)
            return true
        end
        return false
    end
end

function M.start_worker(worker)
    if worker.state == RUNNING then
        log.info('workers '..worker.name..' allready started')
        return
    end
    worker.state = RUNNING
    worker.chan = fiber.channel()
    worker.fiber_id = fiber.create(worker.func, M.cancel(worker))
end

function M.stop_worker(name)
    if M.workers[name].state == RUNNING then
        fiber.kill(M.workers[name].fiber_id)
    end
    if M.workers[name].chan:get(1) ~= nil then
        log.info("stop worker: "..name)
        M.workers[name].state = PENDING
    end
end

function M.drop_worker(name)
    if M.workers[name].state == RUNNING then
        fiber.kill(M.workers[name].fiber_id)
        if M.workers[name].chan:get(1) ~= nil then
            log.info("stop worker: "..name)
            M.workers[name] = nil
        end
    end
end

function M.start_workers()
    for name, worker in ipairs(M.workers) do
        M.start_worker(worker)
    end
end

function M.stop_workers()
    for name, worker in ipairs(M.workers) do
        M.stop_worker(worker)
    end
end

return M

