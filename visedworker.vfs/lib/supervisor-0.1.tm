# vim:set syntax=tcl: #

package require Tcl 8.5

package require Tclx

package provide supervisor 0.1

package require supervisor::gearman::worker 0.1

#-------------------------------------------------------#

namespace eval supervisor {
  variable stat  [dict create mode "parent"]
  variable child [dict create]

  variable -stop 0
}

proc supervisor::boot {args} {
  set repo [lindex $args 0]

  supervisor::signal
  supervisor::load $repo
}

proc supervisor::load {repo} {
  foreach worker [glob -dir $repo *.tcl] {
    supervisor::fork $worker
  }
}

proc supervisor::register {task {entry "-"}} {

  puts "supervisor::register $task $entry"
}


proc supervisor::fork {worker} {
  variable child

  set pid [::fork]

  if {$pid<0} {
    error "fork failed"
  }

  if {$pid==0} {
    # child
    supervisor::spawn $worker
    exit
  } else {
    # parent
    puts "DEBUG: forked pid=$pid"
    dict set child $pid worker    $worker
    dict set child $pid pid       $pid
    dict set child $pid ctime     [clock seconds]
    dict set child $pid mtime     [file mtime $worker]
  }
}

proc supervisor::spawn {worker} {
  variable stat

  dict set $stat mode  "child"

  source $worker
}

proc supervisor::wait {{forever ""}} {
  variable child
  variable -stop

  set pid ""
  catch {lassign [::wait -nohang -untraced] pid type code}
  if {$pid ne ""} {
    # restart worker
    puts "DEBUG: waited $pid Type $code"

    if {${-stop}} {
      puts "DEBUG: supervisor stop. skip restart $pid"
    } else {
      set worker [dict get $child $pid worker]
      dict unset child $pid
      fork $worker
    }
  }

  # stop check
  # runtime check

  if {${-stop} && [dict size $child]==0} {
    set ::forever 0
    return
  }

  after 1000 ::supervisor::wait ;# use 100ms? 50ms?

  if {$forever ne ""} {
    vwait ::$forever
  }
  return
}


proc supervisor::restart {sig} {
  variable child

  foreach pid [dict keys $child]  {
    puts "DEBUG: TERM woker $pid"
    kill TERM $pid
  }
}

proc supervisor::stop {sig} {
  variable child
  variable -stop

  set -stop 1

  foreach pid [dict keys $child]  {
    puts "DEBUG: KILL woker $pid"
    kill TERM $pid
  }
}


# QUIT: graceful shutdown
# HUP: restart? reload config?
# USR1: reopen logs
# USR2: restart

proc supervisor::signal {} {
  ::signal trap {HUP}  {::supervisor::restart %S}
  ::signal trap {TERM} {::supervisor::stop %S}
}

#===========================================================#
# main                                                      #
#===========================================================#

# supervisor::boot
# supervisor::wait forever

