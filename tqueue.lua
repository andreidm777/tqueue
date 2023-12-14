require('strict').on()

local constant = require('queue.constant')
local queue = require('queue.queue')
local queues = require('queue.db.queues')

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

    for _, i in space.index.names:pairs{} do
        local opts = {
            ttl = i[3],
            ttr = i[4],
        }   
        local qu = queue.new(i[1], i[2], opts)
        M.tube[i[1]] = qu
        qu:start_worker()
    end
end

M.queue_loads()

return M