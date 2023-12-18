# tqueue


Lite and modified version from tarantool https://github.com/tarantool/queue, 
and supports all drivers from the original

created because original version not support read_only replics, and automated leader election

use haproxy config for auto connect ot leader

```yaml

global

defaults
    mode tcp
    timeout connect   5s
    timeout client    5s
    timeout server    5s

listen tnt
    bind 127.0.0.1:4401
    mode tcp
    log 127.0.0.1:5050 kern
    option tcplog
    option log-health-checks

    # <--- BEGIN
    tcp-check expect string Tarantool
    # Python:
    # >>> import msgpack
    # >>> header = msgpack.dumps({1: 1, 0: 8})
    # >>> body = msgpack.dumps({39: 'local i=box.info() return not i.ro and i.status == "running" and i.election.state=="leader" and "HAPR" .. "OXYOK"', 33: []})
    # >>> length = msgpack.dumps(len(header + body))
    # >>> (length + header + body).hex()
    tcp-check send-binary 7c82010100088227d9716c6f63616c20693d626f782e696e666f28292072657475726e206e6f7420692e726f20616e6420692e737461747573203d3d202272756e6e696e672220616e6420692e656c656374696f6e2e73746174653d3d226c65616465722220616e6420224841505222202e2e20224f58594f4b222190
    tcp-check expect string HAPROXYOK
    # <--- END

    server localhost 127.0.0.1:3300 check
    server localhost1 127.0.0.1:3301 check
    server localhost2 127.0.0.1:3302 check
```



