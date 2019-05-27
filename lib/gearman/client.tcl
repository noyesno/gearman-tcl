#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#

package require gearman::protocol
package provide gearman::client 0.1

namespace eval gearman::client {
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




proc gearman::client::submit {this args} {
  variable {}

  set task [lindex $args end-1]
  set data [lindex $args end]

  array set kargs {-priority "" -background 0 -uuid "" -timeout 3000 -epoch 0}

  set argc [llength $args]
  for {set i 0 ; incr argc -2} {$i<$argc} {incr i} {
     set arg [lindex $args $i]
     switch -glob -- $arg {
       -back*   {set kargs(-background) 1}
       -high    {set kargs(-priority) "high"}
       -low     {set kargs(-priority) "low"}
       -id      -
       -uuid    {set kargs(-uuid)    [lindex $args [incr i]]}
       -timeout {set kargs(-timeout) [lindex $args [incr i]]}
       -epoch   {set kargs(-epoch)   [lindex $args [incr i]]}
     }
  }

  set cmd "SUBMIT_JOB"
  if {$kargs(-epoch) > 0} {
    set cmd "SUBMIT_JOB_EPOCH"
    gearman::protocol::send $this $cmd $task $kargs(-uuid) $kargs(-epoch) $data
  } else {
    if {$kargs(-priority) ne ""} {
      append cmd "_[string touuper $(-priority)]"
    }
    if {$kargs(-background)} {
      append cmd "_BG"
    }
    gearman::protocol::send $this $cmd $task $kargs(-uuid) $data
  }


  if {$kargs(-epoch) > 0} {
    set job [gearman::protocol::recv $this]
    debug "EPOCH JOB: $job"
    return $job
  } elseif {$kargs(-background)} {
    set job [gearman::protocol::recv $this]
    debug "BG JOB: $job"
    return $job
  }

  #TODO: add timeout
  set result ""
  set data   ""
  for {set n 0} {1} {incr n} {
    set res [gearman::protocol::recv $this $kargs(-timeout)]

    switch -- [lindex $res 0] {
      "JOB_CREATED" {
        debug "JOB $res"
      }
      "WORK_COMPLETE" {
        set result [lindex $res 1 1]
      }
      "WORK_FAIL" {
        error "WORK_FAIL"
        break
      }
      "WORK_EXCEPTION"   {
        # TODO: -onerror
        set result [lindex $res 1 1]
        error "WORK_EXCEPTION: $result"
        break
      }
      "WORK_DATA"   {
        # TODO: -ondata
        lassign [lindex $res 1] job partial_data  
        append data $partial_data
      }
      "WORK_WARNING" {
        puts "Warn: [lindex $res 1 1]"
      }
      "WORK_STATUS" {
        debug "Progress: [lindex $res 1]"
        #TODO# $kargs(-progress)
      }
      default       {
        # TODO:
        error "Error: unknown gearman protocol [lindex $res 0]"
        break
      }
    }
  }
  if {$data ne ""} {
    debug "data   = $data"
  }
  debug "result = $result"
  return $result
}




proc gearman::client::unknown {args} {
  set t [lindex $args 1]
  lset args 1 [lindex $args 2]
  lset args 2 $t

  return $args
}

namespace eval gearman::client {
  namespace ensemble create \
    -map        {submit "submit"} \
    -subcommand {"create" "submit" "close"} \
    -unknown ::gearman::client::unknown
}

