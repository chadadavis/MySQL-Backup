use strict;
use warnings;
use inc::Module::Install;
#use Module::AutoInstall;

name 'SBG-MySQL-Backup';
all_from 'lib/SBG/MySQL/Backup.pm';

# Update this list with something like:
#perl -MDevel::Modlist=stop,nocore,cpan,zerodefault bin/*
#or
#ack '^use '

requires 'Config::Auto'      => '0';
requires 'File::Basename'    => '0'; # Core module
requires 'File::stat'        => '0'; # Core module
requires 'List::MoreUtils'   => '0';
requires 'Log::Any'          => '0';
requires 'Log::Any::Adapter' => '0';
requires 'Moose'             => '0';
requires 'MooseX::Getopt'    => '0';
requires 'Moose::Autobox'    => '0';

# Only for testing, not installed
test_requires 'Test::More'   => '0'; 
test_requires 'File::Temp'   => '0'; 
test_requires 'File::stat'   => '0'; 

# Any required binaries
requires_external_bin 'mysql';
requires_external_bin 'mysqldump';
requires_external_bin 'mysqladmin';
# Restore
requires_external_bin 'zcat';
# Required for parallel compression of backups
requires_external_bin 'pigz';

# Cmd line interface
install_script 'script/mysqlbackup';

# Shared data files (can be found again with File::ShareDir or File::HomeDir
# install_share  'mysqlbackup.conf';

# Prompt to install above deps from CPAN
auto_install();

# Test all *.t files, 
WriteMakefile(test => { TESTS => 't/*.t' } );


