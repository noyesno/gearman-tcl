#===================================================================#
# A Pure Tcl Implementation of Gearman Admin                        #
#-------------------------------------------------------------------#
# by: Sean Zhang                                                    #
# at: Apr, 2014                                                     #
#===================================================================#


package provide gearman 0.1

set dir [file dir [info script]]
source [file join $dir protocol.tcl]
source [file join $dir client.tcl]
source [file join $dir worker.tcl]
source [file join $dir admin.tcl]

