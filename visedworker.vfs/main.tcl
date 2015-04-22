
starkit::startup

::tcl::tm::path add [file join $starkit::topdir lib]
lappend auto_path [file join [file dir [info script]] lib] $env(TCLLIBPATH)

package require supervisor

lassign $argv repo
supervisor::boot $repo
supervisor::wait forever

