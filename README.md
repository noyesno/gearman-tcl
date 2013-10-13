TclGearman
==========

A Tcl implementation of Gearman Client/Worker/Admin interface 

Load the package:

```
load libtclgearman.so
```

A basic Gearman Client:

```
set host [lindex $argv 0]
set client [gearman::client create $host]

$client config -client_id "client_tclgearman"

set result [$client submit "reverse" "Hello Tcl Gearman"]

puts $result
```

A basic Gearman Worker:

```
set server [lindex $argv 0]
set worker [gearman::worker create $server]

$worker register "reverse" "task_reverse"

proc task_reverse {worker data} {
  puts "request data = $data"
  set response_data [string reverse $data]
  return $response_data
}

$worker work
```
