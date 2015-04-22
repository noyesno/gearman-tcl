# vim:set syntax=tcl: #

package provide supervisor::gearman::worker 0.1

package require gearman

namespace eval worker {
  variable worker ""
  variable tasks  ""
}

proc worker::register {task {entry -}} {
  variable tasks
  variable worker

  if {$worker eq ""} {
    puts "DEBUG: pre register $task $entry"
    lappend tasks $task $entry
    return
  }

  if {$entry eq "-"} { set entry $task }
  puts "DEBUG: register $task $entry"
  $worker register $task $entry

  supervisor::register $task $entry
}

proc worker::boot {{tasks ""}} {
  variable worker

  puts "DEBUG: create worker"
  set worker [ gearman::worker create ]
  puts "DEBUG: create worker OK"

  foreach {task entry} [concat $worker::tasks $tasks] {
    if {$entry eq "-"} { set entry $task }
    register $task $entry
  }

  $worker work

  return
}
