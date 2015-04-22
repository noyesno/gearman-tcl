#
# Tcl package index file
#



package ifneeded gearman::protocol 0.1 \
    [list source [file join $dir protocol.tcl] ]
package ifneeded gearman::client 0.1 \
    [list source [file join $dir client.tcl] ]
package ifneeded gearman::worker 0.1 \
    [list source [file join $dir worker.tcl] ]
package ifneeded gearman::admin 0.1 \
    [list source [file join $dir admin.tcl] ]

package ifneeded gearman 0.1 {
  package require gearman::client
  package require gearman::worker
  package require gearman::admin
  package provide gearman 0.1
}

#-- package ifneeded gearman 0.1 \
#--     [list load [file join $dir libtclgearman.so] tclgearman]
