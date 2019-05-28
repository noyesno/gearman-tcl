#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#


package require gearman::protocol
package provide gearman::worker 0.1

namespace eval gearman::worker {
  proc instance {this} {
    set ns [namespace current]
    return $ns'$this
  }

  proc create {args} {
    set this [gearman::protocol::connect {*}$args]

    set ns [namespace current]
    set worker [instance $this]

    interp alias {} $worker {} ${ns}::call $this

    return $worker
  }

  proc close {this} {
    gearman::protocol::close $this
  }

  proc call {this subcmd args} {
    $subcmd $this {*}$args
  }

}


proc gearman::worker::config {this args} {
  variable {}
  array set kargs {-id ""}

  set argc [llength $args]
  for {set i 0} {$i<$argc} {incr i} {
     set arg [lindex $args $i]
     switch -glob -- $arg {
       -id      {set kargs(-id)      [lindex $args [incr i]]}
     }
  }

  if {$kargs(-id) ne ""} {
    set ($this,id) $kargs(-id)
    gearman::protocol::send $this SET_CLIENT_ID $kargs(-id)
  }
}


# register func callback
proc gearman::worker::register {this args} {
  variable {}
  array set kargs {-id "" -timeout 0}

  set func     [lindex $args end-1]
  set callback [lindex $args end]

  set argc [llength $args]
  for {set i 0 ; incr argc -2} {$i<$argc} {incr i} {
     set arg [lindex $args $i]
     switch -glob -- $arg {
       -id      {set kargs(-id)      [lindex $args [incr i]]}
       -timeout {set kargs(-timeout) [lindex $args [incr i]]}
     }
  }

  if {$callback eq ""} {
    gearman::protocol::send $this CANT_DO $func
    unset ($this,callback,$func)
  } elseif {$kargs(-timeout)>0} {
    gearman::protocol::send $this CAN_DO_TIMEOUT $func $kargs(-timeout)
    set ($this,callback,$func) $callback
  } else {
    gearman::protocol::send $this CAN_DO $func
    set ($this,callback,$func) $callback
  }
}

proc gearman::worker::work {this args} {
  variable {}

  array set kargs {-blocking 1 -sleep 0 -uniq 0 -all 0}
  array set kargs $args

  # -step 1
  # _sleep $this
  # TODO: catch errro

  if {$kargs(-uniq)} {
    set cmd_grab GRAB_JOB_UNIQ
  } elseif {$kargs(-all)} {
    set cmd_grab GRAB_JOB_ALL
  } else {
    set cmd_grab GRAB_JOB
  }

  set next_stat $cmd_grab
  while 1 {

    ::update ;# to allow event loop triggered by worker

    set command $next_stat
    switch -exact -- $command {
      ERROR {
        puts "Error: $err"
      }

      GRAB_JOB_UNIQ -
      GRAB_JOB_ALL  -
      GRAB_JOB {
        gearman::protocol::send $this $command
        set reply     [gearman::protocol::recv $this]
        set next_stat [lindex $reply 0]
      }

      JOB_ASSIGN  {
	debug "JOB $reply"
        set uniq "" ; set reducer ""
	lassign [lindex $reply 1] job func data

        set ($this,$job) [list -id $job -uniq $uniq -reducer $reducer]
	_work $this $job $func $data
        unset ($this,$job)

        set next_stat $cmd_grab
      }

      JOB_ASSIGN_UNIQ  {
	debug "JOB UNIQ $reply"
        set uniq "" ; set reducer ""
	lassign [lindex $reply 1] job func uniq data

        set ($this,$job) [list -id $job -uniq $uniq -reducer $reducer]
	_work $this $job $func $data
        unset ($this,$job)

        set next_stat $cmd_grab
      }

      JOB_ASSIGN_ALL  {
	debug "JOB ALL $reply"
        set uniq "" ; set reducer ""
	lassign [lindex $reply 1] job func uniq reducer data

        set ($this,$job) [list -id $job -uniq $uniq -reducer $reducer]
	_work $this $job $func $data
        unset ($this,$job)

        set next_stat $cmd_grab
      }

      NO_JOB  {
        if {!$kargs(-blocking)} {
          # Not in blocking mode, return.
          return 1
          set next_stat @RETURN
        } elseif {$kargs(-sleep) == 0} {
          set next_stat PRE_SLEEP
        } elseif {$kargs(-sleep) > 0} {
          debug "sleep $kargs(-sleep)"
          after $kargs(-sleep)    ;# sleep some time
          set next_stat $cmd_grab
        } else {
          debug "nosleep, grab"
          # ... continue ...
          set next_stat $cmd_grab
        }
      }

      PRE_SLEEP {
        debug "pre_sleep"
        gearman::protocol::send $this PRE_SLEEP
        set reply     [gearman::protocol::recv $this]
        set next_stat [lindex $reply 0]
      }

      NOOP  {
        debug "noop"
        set next_stat $cmd_grab
      }

      eof {
        # ...
        debug "see eof"
	break
      }
      timeout {
        # ...
      }
      default {
	error "unknown stat $next_stat" ;# TODO
      }
    } ;# end switch
  } ;# end while

  return
}


proc gearman::worker::_sleep {this} {
  debug "pre_sleep"
  gearman::protocol::send $this PRE_SLEEP
}

proc gearman::worker::_work {this job func data} {
  variable {}
  # TODO: check $func existance

  debug "task $job $func"

  set callback $($this,callback,$func)

  set worker [instance $this] ;# TODO

  set jobproc ::gearman::worker::job@$job
  set token [interp alias {} $jobproc {} ::gearman::worker::jobcall $this $job]
  set ok 0

  if [catch {

    dict set ($this,$job) -done 0
    set result [uplevel #0 [list $callback $jobproc $data]]
    set ok 1

    if {![dict get $($this,$job) -done]} {
      gearman::protocol::send $this WORK_COMPLETE $job $result
    }
  } err] {
    puts "Worker Error: $err"
    # XXX: only forward to client when "OPTION_REQ exceptions" is set by client
    if {1} {
      gearman::protocol::send $this WORK_EXCEPTION $job $err
    } else {
      gearman::protocol::send $this WORK_FAIL $job
    }
  }

  if {0 && $ok} {
    set reducer [dict get $($this,$job) -reducer]
    if {$reducer ne ""} {
      # aggregate
      # submit $reducer $result
    }
  }

  interp alias {} $token {}   ;# delete job alias

  return $ok
}

proc gearman::worker::jobcall {this job act args} {
  variable {}

  switch -- $act {
    "info" {
      return $($this,$job)
    }
    "data" {
       set data [lindex $args 0]
       gearman::protocol::send $this WORK_DATA $job $data
    }
    "status" {
      lassign $args numerator denominator
      gearman::protocol::send $this WORK_STATUS $job $numerator $denominator
    }
    "warn" {
      set data [lindex $args 0]
      gearman::protocol::send $this WORK_WARNING $job $data
    }
    "fail" {
      if {[llength $args]==0} {
        gearman::protocol::send $this WORK_FAIL $job
      } else {
        set errmsg [lindex $args 0]
        gearman::protocol::send $this WORK_EXCEPTION $job $errmsg
      }
    }

    "done" {
      set result [lindex $args 0]
      gearman::protocol::send $this WORK_COMPLETE $job $result
      dict set ($this,$job) -done 1
    }

    "wait" {
      set done 0
      while 1 {
        if {$done} break
        set done 1
        dict for {job_id job_result} [dict get $($this,$job) reducer] {
          if {$job_result eq ""} {
            # job not done
            set done 0
            break
          }
        } ;# end check jobs
      } ;# end while
    }

    "map" {
      # TODO:
      lassign $args task data
      set uuid ""
      gearman::protocol::send $this SUBMIT_JOB $task $uuid $data

      set reply [gearman::protocol::recv $this]
      set reply_cmd [lindex $reply 0]
      if {$reply_cmd eq "JOB_CREATED"} {
        set map_jobid [lindex $reply 1]
        dict set ($this,$job) reducer $map_jobid ""
      } else {
        # TODO: Error
      }
    }

    "submit" {
      # TODO:
      lassign $args task data
      set uuid ""
      gearman::protocol::send $this SUBMIT_JOB $task $uuid $data

      set reply [gearman::protocol::recv $this]
      set reply_cmd [lindex $reply 0]
      if {$reply_cmd eq "JOB_CREATED"} {
        set map_jobid [lindex $reply 1]
        # XXX: forget it
      } else {
        # TODO: Error
      }
    }

    "reduce" {
      lassign $args data
      set uuid ""
      set task [dict get $($this,$job) -reducer]
      gearman::protocol::send $this SUBMIT_JOB $task $uuid $data
    }
  } ;# end switch
  return
}


proc gearman::worker::unknown {args} {
  set t [lindex $args 1]
  lset args 1 [lindex $args 2]
  lset args 2 $t

  return $args
}

namespace eval gearman::worker {
  namespace ensemble create \
    -map        {submit "submit"} \
    -subcommand {"create" "submit" "close" "config"} \
    -unknown ::gearman::worker::unknown
}

return

  * Add support of reconnect when lost connection.


