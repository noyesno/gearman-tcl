#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#

namespace eval german::client {
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


  proc connect {host {port 4730}} {
    variable sock

    set sock [socket $host $port]
    #fconfigure $sock -blocking 0
    #fileevent $sock readable [list german::client::recv]
  }

  proc close {} {
    variable sock

    ::close $sock
  }


  proc send {type args} {
    variable sock

    set data [join $args "\0"]
    #set data [encoding convertto utf-8 $data]
    set data [binary format "a*" $data]
    set size [string length $data]
    set buffer [binary format a4IIa* "\0REQ" $type $size $data]
    debug "REQ $type $size [join [split $data "\0"]]"
    puts -nonewline $sock $buffer
    flush $sock
  }

  proc recv {} {
    variable sock
    variable lut

    set head [read $sock 12]
    binary scan $head a4II magic type size
    debug "$magic $type $size"
    set data [read $sock $size]
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




proc german::client::submit_job {args} {
  variable sock
  variable protocol

  set task [lindex $args end-1]
  set data [lindex $args end]

  array set kargs {-priority "" -background 0 -uuid ""}

  set argc [llength $args]
  for {set i 0 ; incr argc -2} {$i<$argc} {incr i} {
     set arg [lindex $args $i]
     switch -glob -- $arg {
       -back* {set kargs(-background) 1}
       -high  {set kargs(-priority) "high"}
       -low   {set kargs(-priority) "low"}
       -id    -
       -uuid  {set kargs(-uuid) [lindex $args [incr i]}
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
  send $cmd_id $task $kargs(-uuid) $data

  if {$kargs(-background)} {
    set job [recv]
    debug "BG JOB: $job"
    return $job
  }

  set data ""
  while 1 {
    set res [recv]
    switch [lindex $res 0] {
      "JOB_CREATED" { debug "JOB $res" }
      "WORK_DATA"   {
        append data [lindex $res end]
      }
      "WORK_STATUS" {
        debug "STATUS: ..."
        #TODO# $kargs(-progress)
      }
      "NO_JOB"      { break }
      default       { break }
    }
  }
  debug "data = $data"
  return $data
}
