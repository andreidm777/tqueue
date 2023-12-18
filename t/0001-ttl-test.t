package.path = './?.lua;' .. package.path;

box.cfg{listen = 3301, wal_mode='none'}
box.schema.user.grant('guest', 'read,write,execute', 'universe')

queue = require('tqueue')
local fiber = require('fiber')
local os = require('os')
local log = require('log')

local test = require('tap').test()

test:plan(4)

local tube = queue.create_tube('test_msg', 'fifottl')

test:ok(tube, 'tube created')
test:ok(box.space.test_msg, 'space created')
test:is(tube.name, 'test_msg', 'name checked')

test:test('test put in queue', function(test)
    test:plan(2)
   
    queue.tube.test_msg:put('1', { ttl = 3 })

    fiber.sleep(4)

    local task = queue.tube.test_msg:take(0.15)
    test:isnil(task, 'task exists')

    test:ok(queue.tube.test_msg.statistics['put'] == 1, 'task put it')
end)

os.exit(test:check() and 0 or 1)