#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#


package provide gearman 0.1

namespace eval gearman {

}

namespace eval gearman::admin {
    namespace ensemble create -command ::gearman::admin \
      -map        {
         "version"   "getkey version"
         "getpid"    "getkey getpid"
         "verbose"   "getkey verbose"
         "status"    "read_as_list status"
         "workers"   "read_as_list workers"
         "show jobs" "read_as_list {show jobs}"
      } \
      -subcommand {"create" "close" "version" "getpid" "verbose" "status" "workers" "show jobs"} \
      -unknown [namespace current]::unknown
}

proc gearman::admin::create {host {port 4730}} {
  variable sock

  set sock [socket $host $port]
  fconfigure $sock -buffering line
  return ::gearman::admin
}

proc gearman::admin::close {} {
  variable sock

  ::close $sock
}

# OK $value
proc gearman::admin::getkey {key} {
  variable sock

  puts $sock "$key"
  gets $sock line
  return [lindex $line 1]
}

proc gearman::admin::read_as_list {key} {
  variable sock

  puts $sock "$key"
  set result [list]
  while {1} {
    gets $sock line
    if {$line eq "."} break
    lappend result $line
  }
  return $result
}

return

#----------------------------------------------------------------#
# Self Test                                                      #
#----------------------------------------------------------------#

set host [lindex $argv 0]
set admin [gearman::admin create $host]

puts [gearman::admin version]
puts [$admin getpid]
puts [$admin workers]
puts [gearman::admin work]
$admin close

