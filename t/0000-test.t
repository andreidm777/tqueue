#!/usr/bin/env tarantool

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
    test:plan(3)
   

    local producer = fiber.create(function()
        while true do
            queue.tube.test_msg:put('1')
            fiber.sleep(0.2)
        end
    end)

    fiber.sleep(3)

    test:ok(queue.tube.test_msg.statistics['put'] == 15, 'task put it')

    local task = queue.tube.test_msg:take(.15)
    test:ok(task, 'task get')
    test:is(task[1], 0, 'task id ok')
end)

os.exit(test:check() and 0 or 1)