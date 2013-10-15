

load ./libtclgearman.so

set server [lindex $argv 0]

set admin [gearman::admin create $server] ;# localhost
puts "admin = $admin"

puts [$admin version]
puts [$admin getpid]

puts [$admin status]
puts [$admin workers]
puts [$admin jobs]

