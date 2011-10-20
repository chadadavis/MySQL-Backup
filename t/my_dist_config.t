#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use File::HomeDir;

use Test::More;

my $pkg = 'SBG::MySQL::Backup';
my $dist_name = $pkg;
$dist_name =~ s/::/-/g;
my $dir = File::HomeDir->my_dist_config($dist_name, { create => 1 } );

ok -d $dir, "Dist config dir exists: $dir";

done_testing;

