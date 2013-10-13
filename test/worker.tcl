

load ./libtclgearman.so


set worker [gearman::worker create [lindex $argv 0]] ;# localhost
puts "worker = $worker"

$worker register "echoback" "echo-back"


proc echo-back {worker data} {
  puts "$worker data = $data"
  return $data-echo-back
}

$worker work

