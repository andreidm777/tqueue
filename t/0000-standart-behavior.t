#!/usr/bin/env tarantool

package.path = './?.lua;' .. package.path;

box.cfg{listen = 3301, wal_mode='none'}
box.schema.user.grant('guest', 'read,write,execute', 'universe')

queue = require('tqueue')
local fiber = require('fiber')
local os = require('os')
local log = require('log')

local console = require('console')

local test = require('tap').test()

test:plan(4)

local tube = queue.create_tube('test_msg', 'fifottl')

test:ok(tube, 'tube created')
test:ok(box.space.test_msg, 'space created')
test:is(tube.name, 'test_msg', 'name checked')

test:test('test put in queue', function(test)
    test:plan(49)
   

    local producer = fiber.create(function()
        for i = 1, 15 do
            queue.tube.test_msg:put('1')
            fiber.sleep(0.1)
        end
    end)

    fiber.sleep(3)

    test:ok(queue.tube.test_msg.statistics['put'] == 15, 'task put it')
    for i = 1, 16 do
        local task = queue.tube.test_msg:take(0.15)
        if i < 16 then
            test:ok(task, 'task get' .. i)
            test:is(task[1], i - 1, 'task id ok' .. i)
            test:ok(queue.tube.test_msg:ack(i - 1), 'ack task' .. i)
        else
            test:isnil(task, 'task not exists')
        end
    end
    test:is(queue.tube.test_msg.statistics['ack'],15,'count ack')
    test:is(queue.tube.test_msg.statistics['take'],15,'take ack')
end)

--console.start()
os.exit(test:check() and 0 or 1)