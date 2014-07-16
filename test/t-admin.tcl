
set auto_path [linsert $auto_path 0 lib]
package require gearman

set gearman::debug 1

set host "localhost"
if {$argc>0} {
  set host [lindex $argv 0]
}

#----------------------------------------------------------------#
# Self Test                                                      #
#----------------------------------------------------------------#

set host [lindex $argv 0]
set admin [gearman::admin create $host]

puts [gearman::admin version]
puts [$admin getpid]
puts [$admin workers]
puts [gearman::admin work]
$admin close

