package SBG::MySQL::Backup;

use warnings;
use strict;

=head1 NAME

B<SBG::MySQL::Backups> - Backup or restore one or more MySQL databases

=head1 VERSION

Version 0.02

=cut

our $VERSION = '0.02';


=head1 SYNOPSIS

    mysqlbackup B<--host dbhost> [--db <database1>] [--db <database2> ] [--backupdir /some-remote-mount/backups]

=head2 DESCRIPTION

Expects to be run on the database server locally, in order to verify time stamps of databases.

Rights needed by user 'backup'

 GRANT 
     SELECT, RELOAD, LOCK TABLES, SHOW VIEW, SUPER, REPLICATION CLIENT 
     ON *.* 
     TO 'backup'@'localhost';

Note, granting permissions to localhost will require one to login using -h
localhost explicitly. There may well be different rights if logging in using
the host's own name, e.g. -h dbhost even if these both refer to the same host.

Requires parallel gzip (pigz) for the compression.

Note, backups cannot be run in parallel.

Does a flush-logs before a mysqldump, not simultaneously. If restoring an
incremental backup by replaying the logs, any events that happened while the
dump was running might be replayed. This affects MyISAM only. InnoDB databases
(i.e. having InnoDB tables only) are dumped within a transaction.

The backup is not guaranteed to keep consistent state *between* databases and
assumes that databases are all independent. Otherwise, you need to add the
--lock-all-tables option or the --master-data option to --dumpopts (NB this
will lock your entire database server, all tables of all databases).


The binary log is flushed so that incremental backups to not backup redundant
binary log entries. This also simplifies an incremental restore.


Default options used with mysqldump:


All databases:
--skip-opt
--add-drop-table
--add-locks
--create-options
--extended-insert
--quick
--flush-logs

MyISAM
--opt 
--lock-tables
--disable-keys

InnoDB
--skip-lock-tablesn
--single-transaction

=head1 TODO

Config in ~/.sbgmysqlbackup

Purge old backups

Optinally, Backup the config (default /etc/my.cnf)

DBI can also get the TABLE_TYPE. Don't need to run a shell command

=head1 SEE ALSO

mk-parallel-dump from maatkit (now Percona Toolkit).

Backup example:

 stty -echo;
 cat | mk-parallel-dump \
     --setperdb --locktables --flushlog \
     --user $USER --password `cat -`
     --databases your_database
 stty echo

Restore example:

 stty -echo; 
 cat | mk-parallel-restore \
     --createdb --fifo --truncate \
     --user $USER --password `cat -` \
     ./backup/directory/for/your_database
 stty echo

If you do this, remember to also backup the logs.

=head1 AUTHOR

Chad Davis, C<< <chad.a.davis at gmail.com> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Chad Davis.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut


use Moose;
with 'MooseX::Getopt';

use Moose::Autobox;
use List::MoreUtils qw/uniq/;
use File::stat;
use Config::Auto;
use File::Basename qw/basename/;
use Log::Any qw/$log/;


# Command line options

has 'user' => (
    is            => 'rw',
    isa           => 'Str',
    documentation => 'MySQL backup user name',
);

has 'pass' => (
    is            => 'rw',
    isa           => 'Str',
    documentation => 'MySQL password, if required',
);

has 'port' => (
    is  => 'rw',
    isa => 'Int',
);

has 'backupdir' => (
    is            => 'rw',
    isa           => 'Str',
    default       => '.',
    documentation => 'Directory to save backup files in. Default "."',
);

has 'backuplogdir' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1, # Must be lazy, as it depends on another attribute
    default => sub { (shift)->backupdir . '/' . 'log-bin' },
    documentation =>
      'Directory to save log files in. Default <backupdir>/log-bin',
);

has 'mycnf' => (
    is            => 'rw',
    isa           => 'Str',
    default       => '/etc/my.cnf',
    documentation => 'Path to my.cnf, Default: /etc/my.cnf',
);

has 'datadir' => (
    is            => 'rw',
    isa           => 'Str',
    lazy_build    => 1,
    documentation => 'Directory of raw database files. Default /var/lib/mysql',
);

has 'log_bin' => (
    is         => 'rw',
    isa        => 'Str',
    lazy_build => 1,
    documentation =>
      'Base name of binary log files. Default /var/lib/mysql/<hostname>-bin',
);

has 'incremental' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
    documentation =>
      'Perform an incremental backup by flusing/copying binary logs',
);

has 'dumpopts' => (
    is  => 'rw',
    isa => 'Str',
    documentation => 'Option to pass to mysqldump. Defaults depends on wether a database contains only InnoDB tables',
);

has 'host' => (
    is            => 'rw',
    isa           => 'Str',
    lazy_build    => 1,
    documentation => 'Hostname of DB server, as defined by MySQL',
);

has 'force' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
    documentation =>
      'Backup even when a database has not changed since last backup',
);

has 'db' => (
    is         => 'rw',
    isa        => 'ArrayRef',
    lazy_build => 1,
    documentation =>'Names of DBs to backup. E.g. --db stuff --db other_stuff Or a file, which contains database names, one per line',
);

has 'restore' => (
    is => 'rw',
    isa => 'Bool',
    documentation => 'Restore the database(s) given with --db',
    );
    
has 'stop_datetime' => (
    is => 'rw',
    isa => 'Str',
    documentation => 'Point in time restore, e.g. "2001-01-01 01:01:01"',
    );
    
# Config::IniFiles object from /etc/my.cnf
has '_config' => (
    is         => 'rw',
    lazy_build => 1,
);

# (Derived) Login credentials, as a single string for the command line
has '_creds' => (
    is         => 'rw',
    isa        => 'Str',
    lazy_build => 1,
);

# dumpopts that apply to all databases
# NB --skip-opt is position dependent, it wipes the default opts.
has '_opts_general' => (
    is  => 'ro',
    isa => 'Str',
    default =>'--skip-opt --add-drop-table --add-locks --create-options --extended-insert --quick --flush-logs',
);

# dumpopts for InnoDB only
has '_opts_innodb' => (
    is      => 'ro',
    isa     => 'Str',
    default => '--skip-lock-tables --single-transaction',
);

# dumpopts for MyISAM and mixed databases
has '_opts_other' => (
    is      => 'ro',
    isa     => 'Str',
    default => '--opt --lock-tables --disable-keys',
);


sub _build__config {
    my ($self) = @_;
    return Config::Auto::parse( $self->mycnf );
}


sub _build_datadir {
    my ($self) = @_;
    return $self->_config->{'mysqld'}{'datadir'} || '/var/lib/mysql';
}


sub _build_log_bin {
    my ($self) = @_;
    my $value = $self->_config->{'mysqld'}{'log_bin'}
      || $self->_config->{'mysqld'}{'log-bin'};

    # Default bin log name: hostname-bin
    $value ||= $self->host . '-bin';

    # Make absolute, relative to datadir
    return File::Spec->rel2abs( $value, $self->datadir );
}


sub _build_host {
    my $hostname = `hostname -s`;
    chomp $hostname;
    return $hostname;
}


# Connection credentials string: user and password etc
sub _build__creds {
    my $self = shift;
    my $str;
    $str .= ' -u ' . $self->user         if $self->user;
    $str .= ' --password=' . $self->pass if $self->pass;
    $str .= ' -h ' . $self->host         if $self->host;
    $str .= ' -P ' . $self->port         if $self->port;
    return $str;
}


sub _build_db {
    my $self = shift;

    # If none given, lookup all db names
    return $self->show_databases;
}


# Get all non-blank, non-comment lines
# TODO REFACTOR this into another module
sub _lines {
    my ($path) = @_;
    my $fh;
    $path && -r $path or return;
    open $fh, '<', $path or return;
    my $lines = [];
    while (<$fh>) {
        chomp;
        next if /^\s*$/;    # blank line
        next if /^\s*#/;    # comment line
        s/#.*$//;           # remove trailing comments
        $lines->push($_);
    }
    return $lines;
}


=head2 show_databases

    my $database_names = $backuper->show_databases;
     
Get all database names. Works better than the L<mysqlshow(1)> command, 
as the output there is formatted.

Doesn't show 'test_*' databases unless:

    my $database_name = $backupers->show_databases(all=>1)

=cut

sub show_databases {
    my ($self, %ops) = @_;
    my $creds  = $self->_creds;
    my $cmd    = 'show databases;';
    open my $fh, "mysql $creds -N -B -e '$cmd'| " or die;
    my @dbs = <$fh>;
    chomp for @dbs;
    @dbs = grep { !/^information_schema$/ } @dbs;
    @dbs = grep { !/^test_/ } @dbs unless $ops{all};
    return \@dbs;
}


=head2 flush_logs

    $backuper->flush_logs();
    
Incremental backup (of all databases);

=cut

sub flush_logs {
    my ($self) = @_;
    my $creds = $self->_creds;
    system("mysqladmin $creds flush-logs");
    my $log_bin = $self->log_bin;
    my $dest    = $self->backuplogdir;
    mkdir $dest or die $!;
    
    my @files = <${log_bin}.[0-9]*>;
    # But don't backup the newly created live log (as it's still open)
    my $current = $self->_newest_binlog();
    @files = grep { ! /$current/ } @files;
    system("cp -au @files \"${dest}/\"");
}


# If --db parameter is a text file, read database names, one per line
sub _prepare {
    my ($self) = @_;
    my @dbs = $self->db->flatten;

    # If any "db" is a readable file, parse DB names out of it
    @dbs = map { -r $_ ? @{ _lines($_) } : $_ } @dbs;

    # Remove any spaces
    s/ //g for @dbs;
    $log->debug("Databases: @dbs");
    $self->db([ @dbs ]);
}

=head2 backup

 $backuper->backup('mydatabase');

Full backup fo the given database. Skipped if the DB does not seem to have changed. Otherwise, force a backup of an unchanged DB with:

 $backuper->force(1);

=cut

sub backup {
    my ( $self, $db ) = @_;
    if ( !$self->_changed($db) ) {
        if ( !$self->force ) { return }
    }

    # Destination file path
    my $time = _timestamp();
    my $file = $self->backupdir . "/$db-$time.sql";
    # Bin log file name (for restoring an incremental backup) saved in here
    my $logfile = $self->backupdir . "/$db-binlog-$time.txt";
    
    # mysqldump options depend on the table types in the DB
    my $opts = $self->dumpopts;
    unless ($opts) {
        my $type = $self->_table_types($db);
        my $type_opts =
          ( $type =~ /InnoDB/i ) ? $self->_opts_innodb : $self->_opts_other;
        $log->debug("Backing up database $db as table type: $type");
        $opts = join ' ', $self->_opts_general, $type_opts;
    }

    my $creds = $self->_creds;
    my $cmd   = "mysqldump $creds $opts $db";
    system("$cmd | pigz -c -9 > '${file}.gz'");
    my $binlog = $self->_newest_binlog;
    $log->debug("Database $db now begins at log: $binlog");
    system("echo $binlog > '${logfile}'");
}


sub _table_types {
    my ( $self, $db ) = @_;
    my $creds = $self->_creds;
    my $cmd = "select table_name,engine from tables where table_schema=\"$db\"";
    open my $fh, "mysql $creds -N -B information_schema -e '$cmd'| ";
    my @lines = <$fh>;
    chomp for @lines;

    # Map of table=>engine
    my %types = map { split ' ' } @lines;

    # Unique table engines (e.g. InnoDb, MyISAM, MEMORY, CSV, etc)
    my @types = uniq values %types;

    # Unless all tables are InnoDB, it's 'other'
    my $type = ( @types == 1 && $types[0] =~ /InnoDB/i ) ? 'innodb' : 'other';
    return $type;
}


sub _changed {
    my ( $self, $db ) = @_;
    my $dir = $self->datadir . "/$db";
    unless ( -d $dir ) {
        warn "MySQL database directory ($dir) does not exist\n";
        return;
    }
    my $dbtime = stat($dir)->mtime;
    
    my $prefix = $self->backupdir . "/$db-20";
    my $backup = _newest_file($prefix);

    # If there is no backup, then it's been changed, needs to be backed up
    return 1 unless $backup;
    my $backuptime = stat($backup)->mtime;
    if ( $dbtime <= $backuptime ) {
        $log->info("Unchanged: $db");

        # And mark the file as current (we saw it, don't delete it)
        `touch $backup`;
    }
    return $dbtime > $backuptime;
    
}


sub _timestamp {
    # Translate : to . as the shell doesn't like the former
    my $timestamp = `date +"%F_%T"|tr ':' '.'`;
    chomp $timestamp;
    return $timestamp;
}


# Newest file with the given prefix, if any
sub _newest_file {
    my ($prefix ) = @_;

    my $newest_path;
    my $newest_time;
    foreach my $file (<${prefix}*>) {
        my $mtime = stat($file)->mtime;
        if ( !defined($newest_path) || $mtime > $newest_time ) {
            $newest_path = $file;
            $newest_time = $mtime;
        }
    }
    return $newest_path;
}

# Name of most recent log file
sub _newest_binlog {
    my ($self)    = @_;
    my $creds     = $self->_creds;
    my ($logname) = split ' ', `echo 'show master status'|mysql -N $creds`;
    return $logname;
}


sub _create {
    my ($self, $db) = @_;
    my $creds = $self->_creds;
    $log->debug("Creating: $db");
    
    system("mysqladmin $creds create $db") == 0 || return;
    return 1;  
}

sub _drop {
    my ($self, $db) = @_;
    my $creds = $self->_creds;
    $log->info("Dropping: $db");
    system("mysqladmin $creds --force drop $db >/dev/null")  == 0 || return;
    return 1;
}

=head2 recreate

    $backuper->recreate('mydatabase');
    
Full restore of the last full backup of 'mydatabase'. Any existing 'mydatabase'
will be dropped first.

TODO Disable SQL_LOG_BIN by opening a pipe to the mysql process and feeding it:

 my $fifo="$tempdir/restore.fifo";
 mkfifo $fifo;
 chmod 666 $fifo; # necessary?
 zcat $backup > $fifo

 open my $mysql_fh, '|-', "mysql $creds";
 print $mysql, "
 SET SQL_LOG_BIN = 0;
 -- Then zcat $backup to some named pipe, and then:
 SOURCE $fifo; -- make sure to expand the variable before sending to MySQL
 -- Re-enable binary log
 SET SQL_LOG_BIN = 1;
 ";

 unlink $fifo;

=cut

sub recreate {
    my ( $self, $db ) = @_;
    my $prefix = $self->backupdir . "/$db-20";
    my $backup = _newest_file($prefix);
    unless ($backup) {
        warn("No backups found for: $db");
        return;
    }

    # Restore last full backup
    my $creds = $self->_creds;

    # Drop and re-create empty DB
    # TODO Bug shouldn't fail if DB doesn't exist, this is a full restore
    $self->_drop($db);
    $self->_create($db) or return;

    # Stream the data in
    $log->info("Restoring $db from $backup");
    system("zcat $backup | mysql $creds $db") == 0 or return;

}


=head2 replay

    $backuper->replay('mydatabase');
    
Perfrom an incremental backup by replaying the transaction log of a single DB.

To restore up to a give point in time, first do:

    $backuper->stop_datetime("2010-12-25 10:00:00");
    # Now only transaction before 10am are replayed. 
    # Useful if you corrupted your database at 10:05 and want to restore
    $backuper->replay('mydatabase');


=cut

sub replay {
    my ($self, $db) = @_;
    
    # Start by flushing the logs, so that we have copies of them all
    # Otherwise you cannot restore to a point-in-time after the last incremental
    # backup. So, *now* is the last incremental backup
    $self->flush_logs;
    
    my $creds = $self->_creds;
    
    # Figure out where to begin restoring the incremental backup from
    # TODO bug dont assume the 'binlog' in the name here
    my $indexprefix = $self->backupdir . "/$db-binlog-";
    my $indexfile = _newest_file($indexprefix);
    # Index file contains name of binary log file
    my $logfile = `cat $indexfile`;
    chomp $logfile;
    # Index number of logfile
    my ($logid) = $logfile =~ /\.(\d+)$/;
    
    # The first transaction log file in a series
    my $logdir = $self->backuplogdir;
    # Log file prefix
    my $log_bin = basename($self->log_bin);
    # Find all transaction log files needed, in lexicographic order
    my @logfiles = <${logdir}/${log_bin}*>;
    # Which log files have this ID, or larger (i.e. every log since the start)
    @logfiles = sort grep { /\.(\d+)$/ && $1 >= $logid } @logfiles;
    
    # Re-execute all the saved transactions since the full backup, 
    # on the given database (only!). Disable log-bin while replaying old logs
    $log->info("Replaying on $db:\n", join("\n",@logfiles), "\n");
    my $cmd = "mysqlbinlog @logfiles -d $db -D ";
    my $stopat = $self->stop_datetime;
    # If we should stop at a certain date / time (e.g. "2011-11-11 11:11:11")
    $cmd .= "--stop-datetime=\"$stopat\"" if $stopat;
    
    system("$cmd | mysql $creds");
}


__PACKAGE__->meta->make_immutable;
no Moose;
1;

