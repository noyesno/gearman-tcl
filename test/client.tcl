
load ./libtclgearman.so


set client [gearman::client create [lindex $argv 0]] ;# localhost
puts "client = $client"

$client config -client_id "client_tclgearman"

set result [$client submit "reverse" "hello Tcl"]

puts "result = $result"

set result [$client submit -background -uuid "ta" "reverse" "hello Tcl"]
puts "result = $result"

puts [info commands gearman::client*]

set result [$client submit "echoback" "hello Tcl"]
puts "result = $result"


set result [$client submit "echoback" "hello Tcl 2"]
puts "result = $result"


