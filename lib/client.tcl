#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#

namespace eval gearman::client {
  set debug 0

  proc debug {args} {
    variable debug
    if {!$debug} return
    puts "DEBUG: [join $args]"
  }

  array set protocol {
      SUBMIT_JOB           {   7     {JOB_CREATED WORK_DATA WORK_WARNING WORK_STATUS WORK_COMPLETE WORK_FAIL WORK_EXCEPTION}  }
      JOB_CREATED          {   8     -                                                                                        }
      NO_JOB               {  10     -                                                                                        }
      WORK_STATUS          {  12     -                                                                                        }
      WORK_COMPLETE        {  13     -                                                                                        }
      WORK_FAIL            {  14     -                                                                                        }
      GET_STATUS           {  15     STATUS_RES                                                                               }
      ECHO_REQ             {  16     ECHO_RES                                                                                 }
      ECHO_RES             {  17     -                                                                                        }
      SUBMIT_JOB_BG        {  18     JOB_CREATED                                                                              }
      ERROR                {  19     -                                                                                        }
      STATUS_RES           {  20     -                                                                                        }
      SUBMIT_JOB_HIGH      {  21     {JOB_CREATED WORK_DATA WORK_WARNING WORK_STATUS WORK_COMPLETE WORK_FAIL WORK_EXCEPTION}  }
      OPTION_REQ           {  26     OPTION_RES                                                                               }
      OPTION_RES           {  27     -                                                                                        }
      WORK_DATA            {  28     -                                                                                        }
      WORK_WARNING         {  29     -                                                                                        }
      SUBMIT_JOB_HIGH_BG   {  32     {JOB_CREATED}                                                                            }
      SUBMIT_JOB_LOW       {  33     {JOB_CREATED WORK_DATA WORK_WARNING WORK_STATUS WORK_COMPLETE WORK_FAIL WORK_EXCEPTION}  }
      SUBMIT_JOB_LOW_BG    {  34     {JOB_CREATED}                                                                            }
      SUBMIT_JOB_SCHED     {  35     {JOB_CREATED}                                                                            }
      SUBMIT_JOB_EPOCH     {  36     {JOB_CREATED}                                                                            }
  }

  variable sock ""

  array set lut ""

  proc init {} {
    variable lut
    variable protocol

    foreach {type meta} [array get protocol] {
      set id [lindex $meta 0]
      set lut($id) $type
    }
  }

  init


  proc connect {this host {port 4730}} {
    variable {}

    set sock [socket $host $port]
    #fconfigure $sock -blocking 0
    #fileevent $sock readable [list gearman::client::recv]

    set ($this,sock) $sock
  }

  proc close {this} {
    variable {}

    ::close $($this,sock)
  }


  proc send {this type args} {
    variable {}
    set sock $($this,sock)

    set data [join $args "\0"]
    #set data [encoding convertto utf-8 $data]
    set data [binary format "a*" $data]
    set size [string length $data]
    set buffer [binary format a4IIa* "\0REQ" $type $size $data]
    debug "REQ $type $size [join [split $data "\0"]]"
    puts -nonewline $sock $buffer
    flush $sock
  }

  proc recv {this {timeout 3000}} {
    variable {}
    set sock $($this,sock)

    variable lut

    #-- fconfigure $sock -blocking 0
    #-- if {[chan pending input $sock]<12} {
    #--   puts "DEBUG: pending = [chan pending input $sock]"
    #--   fconfigure $sock -blocking 1
    #--   return ""
    #-- }


    # variable buffer ""   ;# TODO ($this, buffer)
    upvar 0 ($this,buffer) buffer
    set buffer ""

    # set timeout 3000 ;# 3000ms
    set expire [expr {[clock milliseconds] + $timeout}]
    fconfigure $sock -blocking 0
    set stat "ok"
    while {1} {
      if {[clock milliseconds]>$expire} {
        # puts "timeout"
        set stat "timeout"
        break
      }
      set size 12
      if {[string length $buffer]<$size} {
        append buffer [read $sock [expr {$size-[string length $buffer]}]]
      }
      if {[string length $buffer]<$size} {
        # recv $this 0
        continue
      }

      binary scan $buffer a4II magic type size
      debug "$magic $type $size"

      set size [expr {$size+12}]
      if {[string length $buffer]<$size} {
        append buffer [read $sock [expr {$size-[string length $buffer]}]]
      }

      if {[string length $buffer]<$size} {
        # recv $this 0
        continue
      }

      break;
    }
    fconfigure $sock -blocking 1

    if {$stat ne "ok"} {
      return $stat
    }

    # set data [read $sock $size]
    binary scan $buffer x12a* data


    binary scan $data H* hex
    debug "bytes = $hex"
    #set data [encoding convertfrom utf-8 $data]
    # assert $magic eq "\0RES"
    set type_text $lut($type)
    debug "RES $type $size $type_text [join [split $data "\0"]]"
    switch $type_text {
      "WORK_DATA" {
        return [linsert [split $data "\0"] 0 $type_text]
      }
      default {
        return [list $type_text $data]
      }
    }
  }
}




proc gearman::client::submit {this args} {
  variable {}
  variable protocol

  set sock $($this,sock)

  set task [lindex $args end-1]
  set data [lindex $args end]

  array set kargs {-priority "" -background 0 -uuid "" -timeout 3000}

  set argc [llength $args]
  for {set i 0 ; incr argc -2} {$i<$argc} {incr i} {
     set arg [lindex $args $i]
     switch -glob -- $arg {
       -back*   {set kargs(-background) 1}
       -high    {set kargs(-priority) "high"}
       -low     {set kargs(-priority) "low"}
       -id      -
       -uuid    {set kargs(-uuid) [lindex $args [incr i]}
       -timeout {set kargs(-timeout) [lindex $args [incr i]}
     }
  }

  set cmd "SUBMIT_JOB"
  if {$kargs(-priority) ne ""} {
    append cmd "_[string touuper $(-priority)]"
  }
  if {$kargs(-background)} {
    append cmd "_BG"
  }

  set cmd_id [lindex $protocol($cmd) 0]
  send $this $cmd_id $task $kargs(-uuid) $data

  if {$kargs(-background)} {
    set job [recv $this]
    debug "BG JOB: $job"
    return $job
  }

  #TODO: add timeout
  set data ""
  for {set n 0} {1} {incr n} {
    set res [recv $this $kargs(-timeout)]

    switch -- [lindex $res 0] {
      "JOB_CREATED" { debug "JOB $res" }
      "WORK_DATA"   {
        append data [lindex $res end]
      }
      "WORK_STATUS" {
        debug "STATUS: ..."
        #TODO# $kargs(-progress)
      }
      "WORK_COMPLETE" -
      "NO_JOB"      { break }
      "WORK_FAIL"        -
      "WORK_EXCEPTION"   -
      "WORK_WARNING"     -
      default       {
        # TODO:
        set data $res
        break
      }
    }
  }
  debug "data = $data"
  return $data
}

proc gearman::client::create {args} {
  variable {}
  set this [incr (this)]
  interp alias {} ::gearman::client'$this {} ::gearman::client::call $this
  connect $this {*}$args
  return gearman::client'$this
}

proc gearman::client::call {this subcmd args} {
  gearman::client::$subcmd $this {*}$args
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

