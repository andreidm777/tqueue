import msgpack

header = msgpack.dumps({1: 1, 0: 8})
body = msgpack.dumps({39: 'local i=box.info() return not i.ro and i.status == "running" and i.election.state=="leader" and "HAPR" .. "OXYOK"', 33: []})
length = msgpack.dumps(len(header + body))
print((length + header + body).hex())