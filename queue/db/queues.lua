require('strict').on()

local constant = require('queue.constant')

M = {}
M.__index = M

M.SPACE_QUEUES = 'queues'

function M.create()

    local space_opts         = { if_not_exists = true }
    
    space_opts.format = {
        {name = 'name', type = 'string'},
        {name = 'type', type = 'string'},
        {name = 'ttl',  type = 'number'},
        {name = 'ttr',  type = 'number'}
    }

    local space = box.schema.create_space(M.SPACE_QUEUES, space_opts)
    space:create_index('names', {
        type          = 'tree',
        parts         = {1, 'string'}
    })

end

function M.add_queue(name, queue_type, opts)
    opts = opts or {}
    local ttl = opts.ttl and tonumber(opts.ttl) or constant.MAX_TIMEOUT
    box.space[M.SPACE_QUEUES]:insert{
        name, queue_type, ttl, opts.ttr and tonumber(opts.ttr) or constant.TTR_DEFAULT
    }
end

return M