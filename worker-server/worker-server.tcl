#/usr/bin/tclsh

package require Tcl 8.5

package require Tclx
package require gearman


set stop_work 0   ;# TODO: remove this
set state 1       ;# TODO: use $state  : -1 = stop, 0 = begin , 1 = run

#-------------------------------------------------------#
# Worker Instance                                       #
#-------------------------------------------------------#
proc start_worker {functions} {
  set ::worker [gearman::worker create]
  $::worker config -id "[id host]:[pid]"
  register_function $functions
  $::worker work
}
proc close_worker {} {
  $::worker close
}

proc register_function {functions} {
  foreach func $functions {
    $::worker register $func accept
  }
}

proc accept {worker workload} {
  set interp [interp create]
  puts "worker work"
  return [clock seconds]
}

#-------------------------------------------------------#
# Worker Manager                                        #
#-------------------------------------------------------#



proc fork_worker {functions} {
  set pid [fork]
  if {$pid==0} {
    start_worker $functions
    exit
  } else {
    puts "DEBUG: start woker $pid"
    #set workers($pid) [clock seconds]
    dict set ::workers $pid ctime     [clock seconds]
    dict set ::workers $pid functions $functions
  }
}



proc restart_workers {sig} {
  foreach pid [dict keys $::workers]  {
    puts "DEBUG: TERM woker $pid"
    kill TERM $pid
  }
}

proc stop_workers {sig} {
  puts "DEBUG: stop work"
  set ::stop_work 1
  foreach pid [dict keys $::workers]  {
    puts "DEBUG: kill woker $pid"
    kill KILL $pid
  }
}



proc wait_workers {} {
  set pid ""
  catch {lassign [wait -nohang -untraced] pid type code}
  if {$pid ne ""} {
    # restart worker
    puts "DEBUG: waited $pid Type $code"
    set functions [dict get $::workers $pid functions]
    dict unset ::workers $pid
    if {!$::stop_work} {
      puts "DEBUG: restart woker to replace $pid"
      fork_worker $functions
    }
  }

  # stop check
  # runtime check

  if {$::stop_work && [dict size $::workers]==0} {
    set ::forever 0
    return
  }

  after 1000 wait_workers ;# use 100ms? 50ms?
}

proc init_workers {config} {
  set function_group [dict get $config function_group]
  foreach functions $function_group {
    fork_worker $functions
    after 50 ;# wait 50ms
  }
}

#-------------------------------------------------------#
# Main                                                  #
#-------------------------------------------------------#

signal trap {HUP}  {restart_workers %S}
signal trap {TERM} {stop_workers %S}

set config [dict create]
dict set config function_group {
  {a b c d}
  {c b }
  {c}
}

init_workers $config
wait_workers

vwait forever

