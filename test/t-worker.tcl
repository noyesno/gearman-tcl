
set auto_path [linsert $auto_path 0 lib]
package require gearman

set gearman::debug 1

proc task-wc {args} {
  return "hello wc"
}

set host "localhost"
if {$argc>0} {
  set host [lindex $argv 0]
}


set worker [gearman::worker create $host]
$worker register wc task-wc

$worker work

