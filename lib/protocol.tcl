#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Jul, 2014                                                     #
#===================================================================#

package provide gearman::protocol 0.1

namespace eval gearman {
  set debug 0

  proc debug {args} {
    variable debug
    if {!$debug} return
    puts "DEBUG: [join $args]"
  }

}

namespace eval gearman::client   {}
namespace eval gearman::worker   {}
namespace eval gearman::protocol {}
namespace eval gearman::admin    {}

interp alias {} gearman::client::debug {}   gearman::debug
interp alias {} gearman::worker::debug {}   gearman::debug
interp alias {} gearman::protocol::debug {} gearman::debug
interp alias {} gearman::admin::debug {}    gearman::debug

#----------------------------------------------------------------#
# Protocol                                                       #
#----------------------------------------------------------------#

namespace eval gearman::protocol {
  # NO_JOB               {  10     {}         }
  # NO_JOB               {  10     {job data} }
  # TODO: bug workaround, treat client NO_JOB as WORK_COMPLETE
  array set protocol {
      ERROR                {  19     {code text} -                                                                                        }

      SUBMIT_JOB           {   7     {func uuid data} {JOB_CREATED WORK_DATA WORK_WARNING WORK_STATUS WORK_COMPLETE WORK_FAIL WORK_EXCEPTION}  }
      SUBMIT_JOB_LOW       {  33     {func uuid data} {JOB_CREATED WORK_DATA WORK_WARNING WORK_STATUS WORK_COMPLETE WORK_FAIL WORK_EXCEPTION}  }
      SUBMIT_JOB_HIGH      {  21     {func uuid data} {JOB_CREATED WORK_DATA WORK_WARNING WORK_STATUS WORK_COMPLETE WORK_FAIL WORK_EXCEPTION}  }
      SUBMIT_JOB_BG        {  18     {func uuid data} JOB_CREATED        }
      SUBMIT_JOB_LOW_BG    {  34     {func uuid data} {JOB_CREATED}      }
      SUBMIT_JOB_HIGH_BG   {  32     {func uuid data} {JOB_CREATED}      }
      SUBMIT_JOB_SCHED     {  35     {TODO} {JOB_CREATED}                }
      SUBMIT_JOB_EPOCH     {  36     {TODO} {JOB_CREATED}                }

      JOB_CREATED          {   8     {job} -                             }

      CAN_DO               {   1     {func}               {}                        }
      CANT_DO              {   2     {func}               {}                        }
      CAN_DO_TIMEOUT       {  23     {func timeout}       {}                        }
      RESET_ABILITIES      {   3     {}                   {}                        }
      PRE_SLEEP            {   4     {}                   {}                        }
      NOOP                 {   6     {}                   -                         }
      GRAP_JOB             {   9     {}                   {NO_JOB JOB_ASSIGN}       }
      GRAP_JOB_UNIQ        {  30     {}                   {NO_JOB JOB_ASSIGN_UNIQ}  }

      NO_JOB               {  10     {job data}                                     }
      JOB_ASSIGN           {  11     {job func data}      -                         }
      JOB_ASSIGN_UNIQ      {  31     {job func uuid data} -                         }


      WORK_DATA            {  28     {job data} -                        }
      WORK_COMPLETE        {  13     {job data} -                        }
      WORK_STATUS          {  12     {job numer denom} -                 }
      WORK_WARNING         {  29     {job data} -                        }
      WORK_FAIL            {  14     {job}      -                        }
      WORK_EXCEPTION       {  25     {job data} -                        }

      GET_STATUS           {  15     {job} STATUS_RES                    }
      STATUS_RES           {  20     {job known running numer denom} -   }
      ECHO_REQ             {  16     {data} ECHO_RES                     }
      ECHO_RES             {  17     {data} -                            }
      OPTION_REQ           {  26     {name} OPTION_RES                   }
      OPTION_RES           {  27     {name} -                            }
  }

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

  proc lookup {type} {
    variable protocol
    variable lut

    if [string is integer $type] {
      set type_text $lut($type)
      set type      $type
    } else {
      set type_text $type
      set type      [lindex $protocol($type) 0]
    }
    return [list $type_text $protocol($type_text)]
  }

  proc connect {host {port 4730}} {
    variable {}

    set this [incr (this)]

    set sock [socket $host $port]
    #fconfigure $sock -blocking 0
    #fileevent $sock readable [list gearman::client::recv]

    set ($this,sock) $sock

    return $this
  }

  proc close {this} {
    variable {}

    ::close $($this,sock)
  }

  proc send {this type args} {
    variable {}
    set sock $($this,sock)

    set proto [gearman::protocol::lookup $type]
    set type      [lindex $proto 1 0]
    set type_text [lindex $proto 0]

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

      # assert $magic eq "\0RES" ;# TODO

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


    binary scan $buffer H* hex
    debug "bytes head = $hex"
    binary scan $data H* hex
    debug "bytes body = $hex"
    #set data [encoding convertfrom utf-8 $data]
    set proto [gearman::protocol::lookup $type]
    set type_text [lindex $proto 0]
    debug "RES $type $size $type_text [join [split $data "\0"]]"

    set values [lindex $proto 1 1]
    debug "packet values = $values"
    set retc  [llength $values] ;# number of return values
    set retv [split_limit $data "\0" $retc]

    if {$retc != [llength $retv]} {
      error "Incorrect number of result values"
    }

    return [list $type_text $retv]
  }

  proc split_limit {data {sp " "} {limit -1}} {
    set result [list]

    if {$limit==0} {return $result}

    set start 0
    for {set i 1} {$limit<0 || $i<$limit} {incr i} {
      set pos [string first $sp $data $start]
      if {$pos<0} {
	break
      }

      set val [string range $data $start [incr pos -1]]
      lappend result $val

      set start [incr pos 2]
    }

    lappend result [string range $data $start end]

    return $result
  }
}

