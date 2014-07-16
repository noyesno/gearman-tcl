#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#

namespace eval gearman::worker {
  set debug 0

  proc debug {args} {
    variable debug
    if {!$debug} return
    puts "DEBUG: [join $args]"
  }

  array set protocol {
      ERROR                {  19     {code text}          {}                        }
      CAN_DO               {   1     {func}               {}                        }
      CANT_DO              {   2     {func}               {}                        }
      CAN_DO_TIMEOUT       {  23     {func timeout}       {}                        }
      RESET_ABILITIES      {   3     {}                   {}                        }
      PRE_SLEEP            {   4     {}                   {}                        }
      NOOP                 {   6     {}                   -                         }
      GRAP_JOB             {   9     {}                   {NO_JOB JOB_ASSIGN}       }
      GRAP_JOB_UNIQ        {  30     {}                   {NO_JOB JOB_ASSIGN_UNIQ}  }

      WORK_DATA            {  28     {job data}           -                         }
      WORK_COMPLETE        {  13     {job data}           -                         }
      WORK_WARNING         {  29     {job data}           -                         }
      WORK_FAIL            {  14     {job}                -                         }
      WORK_EXCEPTION       {  25     {job data}           -                         }
      SET_CLIENT_ID        {  22     {id}                 -                         }
      ALL_YOURS            {  24     {}                   -                         }

      NO_JOB               {  10     {}                   -                         }
      JOB_ASSIGN           {  11     {job func data}      -                         }
      JOB_ASSIGN_UNIQ      {  31     {job func uuid data} -                         }
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
    #fileevent $sock readable [list gearman::worker::recv]

    set ($this,sock) $sock
  }

  proc close {this} {
    variable {}

    ::close $($this,sock)
  }

  # register func callback
  proc register {this args} {
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
      send $this SET_CLIENT_ID $func
    }

    if {$callback eq ""} {
      send $this CANT_DO $func
      unset ($this,callback,$func)
    } elseif {$kargs(-timeout)>0} {
      send $this CAN_DO_TIMEOUT $func $kargs(-timeout)
      set ($this,callback,$func) $callback
    } else {
      send $this CAN_DO $func
      set ($this,callback,$func) $callback
    }
  }

  proc work {this} {
    variable {}

    #_sleep $this

    while 1 {
      send $this GRAP_JOB
      set res [recv $this]
      switch -- [lindex $res 0] {
        "NO_JOB"      {
          _sleep $this
        }
        "JOB_ASSIGN"  {
          debug "JOB $res"
          lassign [split [lindex $res end] "\0"] job func data
          _work $this $job $func $data
        }
        default {
          error "..." ;# TODO
        }
      }
    }
  }


  proc _sleep {this} {
    debug "pre_sleep"
    send $this PRE_SLEEP
    set res [recv $this]
    if {[lindex $res 0] ne "NOOP"} {
      error "Invalid Response $res"
    }
    debug "noop"
  }

  proc _work {this job func data} {
    variable {}
    # TODO: check $func existance

    debug "task $job $func"

    set callback $($this,callback,$func)

    catch {
      set result [uplevel #0 $callback $data]
    }

    # send $this WORK_DATA   $job $result
    send $this WORK_COMPLETE $job $result
    # send $this NO_JOB      $job $result
  }

  proc send {this type args} {
    variable {}
    variable protocol
    variable lut

    set sock $($this,sock)

    if [string is integer $type] {
      set type_text $lut($type)
      set type      $type
    } else {
      set type_text $type
      set type      [lindex $protocol($type) 0]
    }

    set data [join $args "\0"]
    #set data [encoding convertto utf-8 $data]
    set data [binary format "a*" $data]
    set size [string length $data]
    set buffer [binary format a4IIa* "\0REQ" $type $size $data]
    debug "REQ $type_text : $type $size [join [split $data "\0"]]"
    puts -nonewline $sock $buffer
    flush $sock
  }

  proc recv {this {timeout 0}} {
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
    if {$timeout>0} {
      fconfigure $sock -blocking 0
    }
    set stat "ok"
    while {1} {
      if {$timeout>0 && [clock milliseconds]>$expire} {
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
    if {$timeout>0} {
      fconfigure $sock -blocking 1
    }

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




proc gearman::worker::submit {this args} {
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

proc gearman::worker::create {args} {
  variable {}
  set this [incr (this)]
  interp alias {} ::gearman::worker'$this {} ::gearman::worker::call $this
  connect $this {*}$args
  return gearman::worker'$this
}

proc gearman::worker::call {this subcmd args} {
  gearman::worker::$subcmd $this {*}$args
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

