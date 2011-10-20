#!perl

use Test::More tests => 1;

BEGIN {
    use_ok( 'SBG::MySQL::Backup' ) || print "Bail out!
";
}

diag( "Testing SBG::MySQL::Backup $SBG::MySQL::Backup::VERSION, Perl $], $^X" );
