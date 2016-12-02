#!/usr/bin/perl
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2016 Hewlett Packard Enterprise Development LP
#
# @@@ END COPYRIGHT @@@

use File::Basename;
use File::Copy;
use File::Find;
use POSIX ":sys_wait_h";
use strict;
use warnings;

my %pid2file;
my %dqFiles;
my $nPids = 0;
my $totalBytes = 0;
my $maxSizeBytes = 0;
my $lastDrain = 0;
my $lastPrint = 0;
my $lastStagedGarbageCollected = 0;
my @sortedQfiles;
my @sortedDoneFiles;
my $numInFlight = 0;

# command line processing
die "Usage: $0 basedir [concurrency [throttle [garbageSeconds]]]\n"
  if @ARGV < 1;
my $baseDir = $ARGV[0];
my $concurrency = $ARGV[1] || 20;
my $throttle = $ARGV[2] || 200;
my $garbageSeconds = $ARGV[3] || 600;

# subroutine to convert value in bytes to KB, MB, GB or TB if appropriate
sub convertUnits($)
{
  my $retString;
  if ($_[0] > 1024*1024*1024*1024) {
    $retString = sprintf("%.2fTB", $_[0]/(1024*1024*1024*1024));
  } elsif ($_[0] > 1024*1024*1024) {
    $retString = sprintf("%.0fGB", $_[0]/(1024*1024*1024));
  } elsif ($_[0] > 1024*1024) {
    $retString = sprintf("%.0fMB", $_[0]/(1024*1024));
  } elsif ($_[0] > 1024) {
    $retString = sprintf("%.0fKB", $_[0]/1024);
  } else {
    $retString = sprintf("%d", $_[0]);
  }
  return $retString;
}

# Called by drainQ function to drain a single array. To dequeue a file, we add
# write permissions. The IO thread is polling file permissions, when it
# detects write permission, it begins transfering.
sub dq($\@)
{
  my $currTime = $_[0];
  my $nQueued = scalar @{$_[1]};
  my $numToDequeue = $throttle - $numInFlight;
  if ($numToDequeue > $nQueued) {
    $numToDequeue = $nQueued;
  }
  while ($numToDequeue > 0) {
    my $file = shift @{$_[1]};
    my $path = "$baseDir/HDFS/HDFSqueue/" . $file;
    chmod 0700, $path or warn "WARNING: can't chmod $path\n";
    die "ERROR $file already in dqFiles hash\n" if ($dqFiles{$file});
    $dqFiles{$file} = $currTime; # hash of dequeued files
    $numToDequeue--;
    $nQueued--;
    $numInFlight++;
  }
  return $nQueued;
}

# subroutine to allow queued file to proceed
sub drainQ($)
{
  my $currTime = $_[0];
  return if ($numInFlight >= $throttle);

  # first drain any files already sorted
  if (scalar @sortedQfiles) {
    my $rc = dq($currTime, @sortedQfiles);
    return if ($rc > 0);
  }

  # sorted file list is empty.  Grab all the files in the queue directory
  my @files;
  my $dirname = "$baseDir/HDFS/HDFSqueue/";
  opendir(my $dh, $dirname) or die "ERROR opendir($dirname): $!";
  while (my $file = readdir $dh) {
    next if ($file eq '.' or $file eq '..');
    next if ($dqFiles{$file}); # skip files we've already dequeued
    push @files, $file;
  }
  closedir($dh);
  # if queue directory is empty, we're done
  return if (scalar @files == 0);

  if ($numInFlight + scalar @files <= $throttle) {
    # all queued files can be dequeued, no need to sort
    dq($currTime, @files);
    return;
  }

  # not all files will be dequeued.  Sort the list to be fair
  @sortedQfiles =
    sort {-M $dirname . $b <=> -M $dirname . $a} @files;
  dq($currTime, @sortedQfiles);
}

sub printableTime($)
{
  my $time = $_[0];
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);
  $mon++;
  my $str = sprintf("%02d/%02d %02d:%02d:%02d", $mon, $mday, $hour, $min, $sec);
  return $str;
}

# subroutine to do process staged file:
# 1> do garbage collection
# 2> count total size of staged files
sub processStaged($)
{
  my $currTime = $_[0];

  my $size = 0;

  # Only do staged file garbage collection every 5 minutes.
  # If interval is less, just compute size of staged files and return.
  if ($lastStagedGarbageCollected > 0
  && $currTime - $lastStagedGarbageCollected < 300) {
    find(sub { if (-f $_) { $size += -s $_; delete $dqFiles{$_}; } },
      "$baseDir/HDFS/HDFSstaging");
    find(sub { if (-f $_) { $size += -s $_; delete $dqFiles{$_}; } },
      "$baseDir/HDFS/HDFSreaping");
    if ($size > $maxSizeBytes) {
      $maxSizeBytes = $size;
    }
    return;
  }

  # 1st perform garbage collection on staged files
  my $dirname = $baseDir . "/HDFS/HDFSstaging/";
  opendir(my $dh, $dirname) or die "ERROR opendir($dirname): $!";
  my @stagedFiles = grep /^_/, readdir $dh;
  closedir($dh);
  $lastStagedGarbageCollected = $currTime;
  foreach my $file (@stagedFiles) {
    my $deleted = 0;
    # File is being staged, remove from the dqFiles hash that is used to
    # perform garbage collection on queue control files
    delete $dqFiles{$file};
    my $path = $dirname . $file;
    my $fileTime = (stat($path))[9];
    # make sure stat returned a value, as the file could have been deleted
    # between the readdir call and the stat call
    if (defined $fileTime) {
      my $delta = $currTime - $fileTime;
      # Check to see if the staged file has exceeded the garbage threshold.
      if ($delta > $garbageSeconds) {
        if (unlink($path)) {
          my $mstr = printableTime($fileTime);
          my $str = printableTime($currTime);
          printf("%s: WARNING staging file %s %.1f minutes old (%s) discarded\n",
            $str, $path, $delta/60, $mstr);
          $numInFlight--;
          $deleted = 1;
        }
      }
    }
    if (! $deleted) {
      my $fileSize = -s $path;
      $size += $fileSize if (defined $fileSize);
    }
  }
  if ($size > $maxSizeBytes) {
    $maxSizeBytes = $size;
  }

  # Now perform garbage collection on dequeued control files
  for my $file (keys %dqFiles) {
    # Check to see if the dequeue file has exceeded the garbage threshold.
    if ($dqFiles{$file}) {
      # Staging has not begun for this file. If it had begun, this entry
      # would have been removed from the dqFiles hash.
      my $fileTime = $dqFiles{$file};
      my $delta = $currTime - $fileTime;
      if ($delta > $garbageSeconds) {
        delete $dqFiles{$file};
        my $path = $baseDir . "/HDFS/HDFSqueue/" . $file;
        if (unlink($path)) {
          # The dp2 never reaped the dequeued control file.
          my $str = printableTime($currTime);
          printf("%s: WARNING queue control file %s %.1f minutes old, discarded\n",
            $str, $path, $delta/60);
        } else {
          # The dp2 reaped the dequeued control file, but never started the
          # data transfer.
          my $path = $baseDir . "/HDFS/HDFSstaging/" . $file;
          my $str = printableTime($currTime);
          printf("%s: WARNING file %s dequeued but never staged in %.1f minutes, discarded\n",
            $str, $path, $delta/60);
        }
        $numInFlight--;
      }
    }
  }
  return;
}

# subroutine to print out statistics.
sub info()
{
  my @du_output;
  my $currTime = time();

  # drain queue every 2 seconds
  return if ($lastDrain > 0 && $currTime - $lastDrain < 2);
  drainQ($currTime);
  $lastDrain = $currTime;

  processStaged($currTime);

  # print statistics every 60 seconds
  return if ($lastPrint > 0 && $currTime - $lastPrint < 60);
  $lastPrint = $currTime;

  # normalize size
  my $maxSizeUnits = convertUnits($maxSizeBytes);
  my $totalBytesUnits = convertUnits($totalBytes);

  # determine total number of control files (indicate complete transfers)
  my $dirname = $baseDir. "/HDFS/HDFScontrol/";
  opendir(my $dh, $dirname) or die "ERROR opendir($dirname): $!";
  my $numStaged = 0;
  my $numReaping = 0;
  my $numOK = 0;
  my $numErr = 0;
  while (my $file = readdir $dh) {
    next if ($file eq '.' or $file eq '..');
    my $path = $dirname . $file;
    my $perm = (stat($path))[2];
    next if (!defined $perm);
    $perm &= 0777;
    if ($perm == 0644) {
      $numStaged++;
    } elsif ($perm == 0700) {
      $numReaping++;
    } elsif ($perm == 0600) {
      $numOK++;
    } elsif ($perm == 0601) {
      $numErr++;
    } else {
      printf "WARNING: %s unexpected perm %o\n", $file, $perm;
    }
  }
  closedir($dh);

  # determine total number of queued streams
  my $numQueued = 0;
  find(sub { if (-f $_) { $numQueued++; } }, "$baseDir/HDFS/HDFSqueue");

  my $str = printableTime($currTime);
  printf("%s T %5s MS %5s IF %3d Q %3d S %3d R %3d OK %3d ERR %3d\n",
    $str, $totalBytesUnits, $maxSizeUnits, $numInFlight, $numQueued,
    $numStaged, $numReaping, $numOK, $numErr);
}

# Subroutine to reap all child pids that have finished.
# pid2file is a hash whose key is the child pid, the value is the basename
# of the file that was transfered.
sub reapPid($)
{
  my $flBlock = $_[0];
  my $currTime = time();
  my $wpid;
  my $exitStatus;
  if ($flBlock) {
    $wpid = waitpid(-1, 0) or die "ERROR waitpaid(): $!";
    $exitStatus = $?;
  } else {
    $wpid = waitpid(-1, WNOHANG);
    $exitStatus = $?;
    if ($wpid == 0) {
      return 0;
    }
  }
  my $basename = $pid2file{$wpid};
  delete $pid2file{$wpid};
  if ($exitStatus) {
    my $str = printableTime($currTime);
    print "$str: ERROR pid $wpid fn $basename: $exitStatus\n";
  } else {
    # successful completion. Remove the script and it's output
    my $fn = "$baseDir/HDFS/HDFSlogs/$basename.sh";
    unlink($fn) or warn "WARNING unlink($fn): $!";
    $fn = "$baseDir/HDFS/HDFSlogs/$basename.out";
    unlink($fn) or warn "WARNING unlink($fn): $!";
  }
  $numInFlight--;
  $nPids--;
  return 1;
}

# Create sub-directories if they don't already exist
my $dir = $baseDir . "/HDFS";
unless (-e $dir or mkdir $dir) { die "Unable to create $dir: $!\n" };
$dir = $baseDir . "/HDFS/HDFScontrol";
unless (-e $dir or mkdir $dir) { die "Unable to create $dir: $!\n" };
$dir = $baseDir . "/HDFS/HDFSlogs";
unless (-e $dir or mkdir $dir) { die "Unable to create $dir: $!\n" };
$dir = $baseDir . "/HDFS/HDFSqueue";
unless (-e $dir or mkdir $dir) { die "Unable to create $dir: $!\n" };
$dir = $baseDir . "/HDFS/HDFSstaging";
unless (-e $dir or mkdir $dir) { die "Unable to create $dir: $!\n" };
$dir = $baseDir . "/HDFS/HDFSreaping";
unless (-e $dir or mkdir $dir) { die "Unable to create $dir: $!\n" };

# Create log file and remove buffering
my $path = $baseDir . "/HDFS/HDFSlogs/unloadCron.out";
open my $log_fh, '>>', $path or die "ERROR open($path): $!";
*STDOUT = $log_fh;
*STDERR = $log_fh;
my $old_fh = select($log_fh);
$| = 1;
select($old_fh);

my $pid;
while (1) {
  if (scalar @sortedDoneFiles == 0) {
    while (1) {
      info();
      while ($nPids > 0) {
        my $reaped = reapPid(0);
        last if ($reaped == 0);
      }
      # Grab & sort control files indicating complete transfers
      my $dirname = $baseDir. "/HDFS/HDFScontrol/";
      opendir(my $dh, $dirname) or die "ERROR opendir($dirname): $!";
      my @files;
      while (my $file = readdir $dh) {
        next if ($file eq '.' or $file eq '..');
        my $path = $dirname . $file;
        my $perm = (stat($path))[2];
        next if (!defined $perm);
        $perm &= 0777;
        next if ($perm == 0700 || $perm == 0600 || $perm == 0601);
        delete $dqFiles{$file};
        push @files, $file;
      }
      closedir($dh);
      if (scalar @files > 0) {
        @sortedDoneFiles =
          sort {-M $dirname . $b <=> -M $dirname . $a} @files;
        last;
      }
      sleep 1;
    }
  } elsif ($nPids > 0) {
    info();
    while ($nPids > 0) {
      my $reaped = reapPid(0);
      last if ($reaped == 0);
    }
  }

  # Grab the next staged file, move it to the reaping directory and change
  # the permission for the associated control file
  my $basename = shift @sortedDoneFiles;
  my $controlFile = $baseDir . "/HDFS/HDFScontrol/" . $basename;
  my $stagedFile = $baseDir . "/HDFS/HDFSstaging/" . $basename;
  my $reapFile = "$baseDir/HDFS/HDFSreaping/" . $basename;
  my $size = (stat($stagedFile))[7];
  if (!defined $size) {
    print "ERROR stat($stagedFile) undefined\n";
    chmod 0601, $controlFile or warn "ERROR: can't chmod $controlFile\n";
    next;
  }
  my $rtn = move($stagedFile, $reapFile);
  if ($rtn == 0) {
    print "ERROR move($stagedFile, $reapFile): $!\n";
    chmod 0601, $controlFile or warn "ERROR: can't chmod $controlFile\n";
    next;
  }
  $rtn = chmod 0700, $controlFile;
  if ($rtn == 0) {
    print "ERROR: can't chmod $controlFile: $!\n";
    chmod 0601, $controlFile or warn "ERROR: can't chmod $controlFile\n";
    next;
  }
  $totalBytes += $size;

  if ($nPids == $concurrency) {
    # If no pids available, wait for one to finish.
    reapPid(1);
  }

  # Loop until we can successfully create a child process
  while (1) {
    $pid = fork();
    last if (defined $pid);
    print("WARNING fork failed, sleep and retry\n");
    sleep(10);
  }

  if ($pid > 0) {
    # Store staged filename into a hash.  Key is child pid
    $pid2file{$pid} = $basename;
    $nPids++;
  } else {
    # Read the control file. Wait until the file has 3 lines to avoid
    # reading a partially transferred file
    my @lines;
    my $nSleep = 0;
    while (1) {
      open my $handle, '<', $controlFile or die "ERROR open($controlFile): $!";
      chomp(@lines = <$handle>);
      close $handle;
      my $nLines = scalar @lines;
      last if ($nLines == 3);
      if (++$nSleep == 10) {
        printf "$controlFile: not enough lines, sleep & retry\n";
        $nSleep = 0;
      }
      sleep(1);
    }

    # Create then run command file
    my $cmdFile = "$baseDir/HDFS/HDFSlogs/" . $basename . ".sh";
    open(my $fh, '>', $cmdFile) or die "ERROR open($cmdFile): $!";
    chmod 0755, $fh;
    print $fh "#!/bin/bash\n";

    if ($lines[0] =~ '^rm ') {
      print $fh "\n# Empty file, just remove it\n";
      print $fh "$lines[0]\n";
      print $fh "rc=\$?\n";
      print $fh "if [[ \$rc -ne 0 ]] ; then\n";
      print $fh "  echo \"$lines[0] failed\"\n";
      print $fh "  chmod 0601 $controlFile\n";
      print $fh "  rc=\$?\n";
      print $fh "  if [[ \$rc -ne 0 ]] ; then\n";
      print $fh "    echo \"chmod $controlFile failed\"\n";
      print $fh "  fi\n";
      print $fh "  exit 1\n";
      print $fh "fi\n";
    } else {
      print $fh "\n# Make sure data file exists in staging area.\n";
      print $fh "echo \"stat $lines[0]\"\n";
      print $fh "stat $lines[0]\n";
      print $fh "rc=\$?\n";
      print $fh "if [[ \$rc -ne 0 ]] ; then\n";
      print $fh "  echo \"stat $lines[0] failed\"\n";
      print $fh "  chmod 0601 $controlFile\n";
      print $fh "  rc=\$?\n";
      print $fh "  if [[ \$rc -ne 0 ]] ; then\n";
      print $fh "    echo \"chmod $controlFile failed\"\n";
      print $fh "  fi\n";
      print $fh "  exit 1\n";
      print $fh "fi\n";
      print $fh "\n# Remove target file from HDFS (may not exist, failure ok).\n";
      print $fh "echo \"hadoop fs -rm -skipTrash $lines[1] 2>/dev/null\"\n";
      print $fh "hadoop fs -rm -skipTrash $lines[1] 2>/dev/null\n";
      print $fh "\n# Move data file from staged area to HDFS.\n";
      print $fh "echo \"hadoop fs -moveFromLocal $lines[0] $lines[1]\"\n";
      print $fh "hadoop fs -moveFromLocal $lines[0] $lines[1]\n";
      print $fh "rc=\$?\n";
      print $fh "if [[ \$rc -ne 0 ]] ; then\n";
      print $fh "  echo \"moveFromLocal failed\"\n";
      print $fh "  chmod 0601 $controlFile\n";
      print $fh "  rc=\$?\n";
      print $fh "  if [[ \$rc -ne 0 ]] ; then\n";
      print $fh "    echo \"chmod $controlFile failed\"\n";
      print $fh "  fi\n";
      print $fh "  exit 1\n";
      print $fh "fi\n";
    }
    print $fh "\n# Change control file permissions to 0 to indicate success\n";
    print $fh "chmod 0600 $controlFile\n";
    print $fh "rc=\$?\n";
    print $fh "if [[ \$rc -ne 0 ]] ; then\n";
    print $fh "  echo \"chmod $controlFile failed\"\n";
    print $fh "  exit 1\n";
    print $fh "fi\n";
    print $fh "\necho \"normal exit\"\n";
    print $fh "exit 0\n";
    close($fh);
    my $cmd = "$cmdFile > $baseDir/HDFS/HDFSlogs/$basename.out 2>&1";
    exec($cmd);
    die;
  }
}
