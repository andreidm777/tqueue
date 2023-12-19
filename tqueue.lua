require('strict').on()

local constant = require('queue.constant')
local queue    = require('queue.queue')
local queues   = require('queue.db.queues')
local log      = require('log')
local sup      = require('queue.fibers.supervisor')
local fiber    = require('fiber')

local M={}
M.__index=M

function M.create_tube(name, type_queue, opts)   
    local space, allready_exists = queue.create_tube(name, type_queue, opts)
    if not space then
        error("cannot create space")
        return nil
    end
    
    if not allready_exists then
        queues.add_queue(name, type_queue, opts)
        local qu = queue.new(name, type_queue, opts)
        M.tube[name] = qu
        qu:start_worker()
    end

    return space
end

M.tube = {}

function M.queue_loads()

    local space = box.space[queues.SPACE_QUEUES]

    if not space then
        queues.create()
    end
    
    space = box.space[queues.SPACE_QUEUES]

    for _, i in space.index.names:pairs{} do
        local opts = {
            ttl = i[3],
            ttr = i[4],
        }
        log.info('start workers'..i[1])
        local qu = queue.new(i[1], i[2], opts)
        M.tube[i[1]] = qu
        qu:start_worker()
    end
end


fiber.create(function()
    fiber.self():name('election_mode_checker')
    if box.election_mode == 'off' then
        while true do
            if not box.info.ro then
                M.queue_loads()
                sup.start_workers()
                box.ctl.wait_ro()
            else
                sup.stop_workers()
                box.ctl.wait_rw()
            end
        end
    else
        box.ctl.on_election(function()
            if not sup.in_reload then
                if box.info.election.state == constant.LEADER then
                    box.ctl.wait_rw()
                    M.queue_loads()
                    sup.start_workers()
                else
                    sup.stop_workers()
                end
            end
        end)
    end
end)


return M
