#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use File::Temp qw/tempdir/;
use File::stat;
use SBG::MySQL::Backup;

use Log::Any::Adapter;
#Log::Any::Adapter->set('+SBG::Log');

# TODO Need to drop test DB upon exception

TODO: { local $TODO = 'Clean parameters'; ok 0; }

# TODO should use the standard: DBI_USER and DBI_PASS here
# See perldoc DBI


diag "\nDatabase backup user (default: <none>): ";
my $user = $ENV{MYSQL_USER} || <STDIN>;
chomp $user;
diag "\nPassword for $user (default: <none>): ";
my $pass = $ENV{MYSQL_PASS} || <STDIN>;
chomp $pass;
diag "\nDatabase hostname (default: <none>): ";
my $host = $ENV{MYSQL_HOST} || <STDIN>;
chomp $host;

my $backuper = SBG::MySQL::Backup->new;
$backuper->user($user); # if $user;
$backuper->pass($pass) if $pass;
$backuper->host($host) if $host;

like($backuper->host, qr/[a-zA-Z0-9_-]/, 'hostname');

ok(-r $backuper->datadir, 'readable datadir: ' . $backuper->datadir);

my $log_bin = $backuper->log_bin;
ok(scalar(<${log_bin}*>), 'binary logging enabled');

TODO: { local $TODO = "Catch access denied"; ok 0; }

my $dbs = $backuper->show_databases(all=>1);
isa_ok($dbs, 'ARRAY');

# Setup a testing database
my $testdb = 'test_mysqlbackup';

my $dbs_before = $backuper->show_databases(all=>1);
my $match_before = grep { /$testdb/ } @$dbs_before;
is ($match_before, 0, "No $testdb before");

$backuper->_create($testdb);
my $dbs_w_test = $backuper->show_databases(all=>1);
my $present = grep { /$testdb/ } @$dbs_w_test; 
ok($present, "Create DB: $testdb") or diag explain $dbs_w_test;

$backuper->_drop($testdb);
my $dbs_after = $backuper->show_databases(all=>1);
my $match_after = grep { /$testdb/ } @$dbs_after;
is ($match_after, 0, "No $testdb after");

$backuper->_create($testdb);
my $tempdir = tempdir(CLEANUP=>!$ENV{DEBUG});
ok(-d $tempdir, "backupdir: $tempdir");
$backuper->backupdir($tempdir);
$backuper->backup($testdb);
my @fullbackupfiles = <${tempdir}/${testdb}-*.sql.gz>; 
is(scalar(@fullbackupfiles), 1, "Backup created in $tempdir") or 
    diag @fullbackupfiles;

my $newlog = `cat $tempdir/${testdb}-binlog-*.txt`; 
chomp $newlog;
$newlog = $backuper->datadir . "/$newlog";
ok(-e $newlog, "Log restarted at: $newlog");

# Verify mtime 
my $mtime_full = stat($fullbackupfiles[0])->mtime;
# Do another backup, sleep at least one second, to later verify mtime updated
sleep 1;
$backuper->backup($testdb); 
# Verify that no new backup was created;
my @unmodifiedbackupfiles = <${tempdir}/${testdb}-*.sql.gz>;
is(scalar(@unmodifiedbackupfiles), 1, "Unmodified database skipped") or
    diag @unmodifiedbackupfiles;
# Verify that mtime was updated, however, to reflect that the it was considered
my $mtime_unmodified = stat($unmodifiedbackupfiles[0])->mtime;
cmp_ok($mtime_unmodified, '>', $mtime_full, "mtime updated on skipped backup");


sub _table {
    my ($backuper, $db, $table) = @_;
    my $creds = $backuper->_creds;
    $table ||= 'temp';
    my $ret = system("echo 'create table $table (myint int)' | mysql $creds $db") == 0 or die;
    return $ret;
}
sub _modify {
    my ($backuper, $db, $table) = @_;
    my $creds = $backuper->_creds;
    $table ||= 'temp';
    my $ret = system("echo 'insert into $table values (5)' | mysql $creds $db") == 0 or die;
    return $ret;
}    

# Create a temp table 
ok _table($backuper, $testdb, 'mytable'), 'Create temporary table';

TODO: { local $TODO = "Test modification of MyISAM and InnoDB types"; ok 0; }

# Test that it's not yet been modified
TODO: { local $TODO = "Not yet modified"; ok 0; }

# Make a modification
ok _modify($backuper, $testdb, 'mytable'), 'Modify database';

# Test that it's been modified
TODO: { local $TODO = "Detected modification"; ok 0; }

# Should produce a new full backup now, since modified
sleep 1;
$backuper->backup($testdb);
my @modifiedbackupfiles = <${tempdir}/${testdb}-*.sql.gz>;
is(scalar(@modifiedbackupfiles), 2, "New backup of modified database") or 
    diag @modifiedbackupfiles;
    

# Force a new full backup of an unchanged database
$backuper->force(1);
sleep 1;
$backuper->backup($testdb);
my @forcedbackupfiles = <${tempdir}/${testdb}-*.sql.gz>;
is(scalar(@forcedbackupfiles), 3, "Forced backup of unmodified database") or
    diag @forcedbackupfiles;
# Turn force back off
$backuper->force(0);


TODO: { local $TODO = "Catch backup of non-existent DB"; ok 0; }

TODO: { local $TODO = "Incremental backup"; ok 0; }

TODO: { local $TODO = "Full restore"; ok 0; }

TODO: { local $TODO = "Incremental restore"; ok 0; }

TODO: { local $TODO = "Verify non-locking when only InnoDB"; ok 0; }


$backuper->_drop($testdb);
done_testing;
