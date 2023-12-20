local fiber = require('fiber')
local os = require('os')
local log = require('log')
local fio   = require('fio')
local popen = require('popen')
local netbox = require('net.box')

local test = require('tap').test()

test:plan(4)

local tarantools = {}
local tarantools_dir = {}

function make_config()
    for i = 1, 3 do
        local replication = '{\'127.0.0.1:20011\',\'127.0.0.1:20012\',\'127.0.0.1:20013\'}'
        local listen = '127.0.0.1:2001'..i
        local dir_replica = fio.tempdir()
        log.info(dir_replica)
        local config = [[
            box.cfg {
                election_mode='candidate',
                replication_synchro_quorum=2,
                replication_synchro_timeout=1,
                replication_timeout=0.25,
                election_timeout=0.25,
                read_only = false,
                replication = ]] .. replication ..
                ',\n listen = \'' .. listen ..
                '\',\n wal_dir = \'' .. dir_replica ..
                '\',\n memtx_dir' .. ' = \'' .. dir_replica ..
                '\',\n log' .. ' = \'' ..
                fio.pathjoin(dir_replica, 'tarantool.log') .. '\'' ..[[
            }
                package.path = ']]..fio.cwd()..'/?.lua;\' .. package.path;'..[[
                queue = require('tqueue')
                box.once('bootstrap', function()
                box.schema.user.grant('guest', 'read,write,create,execute,usage', 'universe', nil, {if_not_exists = true});
                queue.create_tube('test_msg', 'fifottl', { if_not_exists = true });
                end)
            ]]
        local fh = fio.open(dir_replica..'/queue.lua', {'O_RDWR', 'O_APPEND', 'O_CREAT'}, tonumber('666',8))
        fh:write(config)
        fh:close()
        tarantools[i] = popen.shell('tarantool '..dir_replica..'/queue.lua', nil)
        tarantools_dir[i] = dir_replica
    end
end

make_config()

fiber.sleep(20)

local conns = {}

local leader = nil

for i = 1, 3 do
    conns[i] = netbox.connect('127.0.0.1:2001'..i, {wait_connected = true})
    local info = conns[i]:call('box.info')
    if info.election.state == 'leader' then
        leader = conns[i]
    end
end

test:ok(leader:call('queue.tube.test_msg:put',{ '1' }), 'ok put task')

local task = leader:call('queue.tube.test_msg:take')

test:ok(task, 'get task ok')
test:is(task[1], 0, 'task id is valid')
test:ok(leader:call('queue.tube.test_msg:ack', { 0 }), 'ack tasj ok')


for i = 1, 3 do
    tarantools[i]:kill()
    tarantools[i]:wait()
    for _, v in pairs(fio.glob(tarantools_dir[i]..'/*')) do
        fio.unlink(v)
    end
    fio.rmdir(tarantools_dir[i])
end

os.exit(test:check() and 0 or 1)