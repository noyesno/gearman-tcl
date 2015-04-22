#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#


package require gearman::protocol
package provide gearman::worker 0.1

namespace eval gearman::worker {
  proc create {args} {
    set this [gearman::protocol::connect {*}$args]

    set ns [namespace current]
    interp alias {} $ns'$this {} ${ns}::call $this

    return $ns'$this
  }

  proc close {this} {
    gearman::protocol::close $this
  }

  proc call {this subcmd args} {
    $subcmd $this {*}$args
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
       -id      {set kargs(-id) [lindex $args [incr i]}
       -timeout {set kargs(-timeout) [lindex $args [incr i]}
     }
  }

  if {$kargs(-id) ne ""} {
    gearman::protocol::send $this SET_CLIENT_ID $func
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

  array set kargs {-blocking 1 -sleep 100}
  array set kargs $args

  #_sleep $this
  #TODO: catch errro

  set res [list _GRAB_]
  while 1 {
    set stat [lindex $res 0]
    switch -- $stat {
      _GRAB_ {
        gearman::protocol::send $this GRAB_JOB
      }

      _EOF_ {
        break
      }

      NO_JOB  {
        if {!$kargs(-blocking)} {
          return 1
        }

	#-- _sleep $this
        if {$kargs(-sleep) == 0} {
          debug "pre_sleep"
          gearman::protocol::send $this PRE_SLEEP
        } elseif {$kargs(-sleep) > 0} {
          debug "sleep $kargs(-sleep)"
          after $kargs(-sleep)
          set res [list _GRAB_]
          continue
        } else {
          debug "nosleep, grab"
          # ... continue ...
          set res [list _GRAB_]
          continue
        }
      }

      NOOP  {
        debug "noop"
        gearman::protocol::send $this GRAB_JOB
      }

      JOB_ASSIGN  {
	debug "JOB $res"
	lassign [lindex $res 1] job func data
	_work $this $job $func $data

        gearman::protocol::send $this GRAB_JOB
      }

      default {
	error "..." ;# TODO
      }
    } ;# end switch

    set res [gearman::protocol::recv $this]
  } ;# end while
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

  set worker "-" ;# TODO

  if [catch {
    set result [uplevel #0 [list $callback $worker $data]]
  } err] {
    set result "Worker Error: $err"
    puts $result
  }

  # send $this WORK_DATA   $job $result
  gearman::protocol::send $this WORK_COMPLETE $job $result
  # send $this NO_JOB      $job $result
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
    -subcommand {"create" "submit" "close"} \
    -unknown ::gearman::worker::unknown
}

return

  * Add support of reconnect when lost connection.


