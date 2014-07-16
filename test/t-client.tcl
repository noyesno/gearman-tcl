
set auto_path [linsert $auto_path 0 lib]
package require gearman

set gearman::debug 1

set host "localhost"
if {$argc>0} {
  set host [lindex $argv 0]
}

set client [gearman::client create $host]
puts [$client submit "wc" "123456789"]
puts [$client submit "disk-echo" "123456789"]
puts [$client submit "wc" "123456789"]
puts [$client submit "disk-echo" "987654321"]
$client close

