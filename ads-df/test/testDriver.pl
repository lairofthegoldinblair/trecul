#
# Copyright (c) 2012, Akamai Technologies
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
#   Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
# 
#   Redistributions in binary form must reproduce the above
#   copyright notice, this list of conditions and the following
#   disclaimer in the documentation and/or other materials provided
#   with the distribution.
# 
#   Neither the name of the Akamai Technologies nor the names of its
#   contributors may be used to endorse or promote products derived
#   from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
#

use strict;

use Cwd qw(abs_path);
use File::Path;
use File::Spec;
use File::Temp;
use IO::File;

my $AdsDf="";
my $MpiExec="";

#
# Run a script as a compile/plan pair to
# test plan serialization.
#
sub runScriptSerializedPlan {
    my $testScript = shift;
    my $plan = `$AdsDf --file $testScript --compile`;
    my $tmpPlan = File::Temp->new();
    print $tmpPlan $plan;
    $tmpPlan->close();
    my @args = ("$AdsDf", "--file", "$tmpPlan", "--plan");
    system(@args);
}

#
# Run script without serialization.
#
sub runScript {
    my $testScript = shift;
    my @args = ("$AdsDf", "--file", "$testScript");
    system(@args);
}

#
# Run script without serialization and with intra process parallelism
#
sub runScriptIntraProcessParallel {
    my $testScript = shift;
    my $partitions = shift;
    my @args = ("$AdsDf", "--partitions", "$partitions", "--file", "$testScript");
    system(@args);
}

sub runScriptInterProcessParallel {
    my $testScript = shift;
    my $partitions = shift;
    my ($volume,$directories,$file) = File::Spec->splitpath(File::Spec->rel2abs($AdsDf));
    my @hosts = ("localhost") x $partitions;
    my @args = ("mpiexec", "-hosts", join(",", @hosts), "$file", "--file", "$testScript");
    system(@args);
}

#
# Compare an expected output
#
sub checkExpected {
    my $expectedOutput = shift;
    my $expectedBaseDirectory = shift;
    my $failed = 0;
    # Find generated matching output by removing trailing .expected.
    my $generatedOutput = $expectedOutput;
    # Now we only want to use the proper relative path to expected output, so
    # concatenate
    $expectedOutput = "$expectedBaseDirectory/$expectedOutput";
    $generatedOutput =~ s/.expected$//;
    if (-f $expectedOutput) {
        my @args =("diff", "$generatedOutput", "$expectedOutput");
        system(@args);
        if ($? != 0) {
            $failed = 1;
        } else {
            # Cleanup file if success
            unlink($generatedOutput);
        }
    } elsif (-d $expectedOutput) {
        opendir(DIR, $expectedOutput);
        my @contents = readdir(DIR);
        closedir(DIR);
        my $expectedFile;
        foreach $expectedFile (@contents) {
            if (-f "$expectedOutput/$expectedFile") {
                my @args =("diff", "$generatedOutput/$expectedFile", "$expectedOutput/$expectedFile");
                system(@args);
                if ($? != 0) {
                    $failed = 1;
                }
            }
        }
        rmtree($generatedOutput) unless $failed;
    }
    return $failed;
}

#
# Run all tests
#
sub runTests {
    my $testDirs = shift;
    my $failedTests = shift;
    my $runMode = shift;
    #
    # foreach test
    #
    my $nPassed = 0;
    my $nFailed = 0;
    my($testDir);

    foreach $testDir (@$testDirs)
    {
	my $failed = 0;
	chdir $testDir;
	opendir(DIR, ".");
	my @contents = readdir(DIR);
	closedir(DIR);
	my @testScripts = grep(/.iql$/, @contents);
	my $testScript;
        my @partitions = grep(/^[0-9]+$/, @contents);
        if (@partitions == 0 && $runMode <= 1) {
            foreach $testScript (@testScripts) {
                if ($runMode) {
                    &runScriptSerializedPlan($testScript);
                } else {
                    &runScript($testScript);
                }
            }
            my @expectedOutputs = grep(/expected$/, @contents);
            my $expectedOutput;
            foreach $expectedOutput (@expectedOutputs) {
                $failed += checkExpected($expectedOutput, ".");
            }
            if ($failed>0) {
                push(@$failedTests, $testDir);
                $nFailed++;
            } else {
                $nPassed++;
            }
        } elsif ($runMode != 1) {
            my $partition;
            foreach $partition (@partitions) {
                foreach $testScript (@testScripts) {
                    if ($runMode == 2) {
                        &runScriptInterProcessParallel($testScript, $partition);
                    } else {
                        &runScriptIntraProcessParallel($testScript, $partition);
                    }
                }
                opendir(DIR, $partition);
                my @partitionContents = readdir(DIR);
                closedir(DIR);
                my @expectedOutputs = grep(/expected$/, @partitionContents);
                my $expectedOutput;
                foreach $expectedOutput (@expectedOutputs) {
                    $failed += checkExpected($expectedOutput, $partition);
                }
                if ($failed>0) {
                    push(@$failedTests, "$testDir#$partition");
                    $nFailed++;
                } else {
                    $nPassed++;
                }
            }
        }
	chdir "..";
    }
    return ($nPassed, $nFailed);
}

my @testDirs;
print $ARGV[0]."\n";
chdir($ARGV[0]);
opendir(DIR, ".");
@testDirs = grep(/test[0-9]+$/,readdir(DIR));
closedir(DIR);

$AdsDf = $ARGV[1];

# Need ads-df on path for MPI to work
my ($volume,$directories,$file) = File::Spec->splitpath(File::Spec->rel2abs($AdsDf));
$ENV{PATH} = $ENV{PATH} . ":$directories";

if (@ARGV > 2) {
    # If using MPI, prepend to the path to make sure we use the desired mpiexec
    $MpiExec = $ARGV[2];
    ($volume,$directories,$file) = File::Spec->splitpath(File::Spec->rel2abs($MpiExec));
    $ENV{PATH} =  "$directories:" . $ENV{PATH};
}

print "Checking environment variables\n";
system("printenv");

my $testModes = $MpiExec ? 2 : 1;

my $totalFailures = 0;
for (my $i = 0; $i <= $testModes; ++$i) {
    if ($i == 0) {        
        print "Running single and multi-partition tests in process\n";
    } elsif ($i == 1) {        
        print "Running single partition tests with serialized plans\n";
    } else {
        print "Running multi-partition tests with mpiexec at $MpiExec\n";
    }
    my @failedTests;
    my ($nPassed, $nFailed) = runTests(\@testDirs, \@failedTests, $i);
    
    print "\nNumber of tests passed: $nPassed\n";
    print "Number of tests failed: $nFailed\n";
    
    if ($nFailed > 0) {
	my $failedTest;
	foreach $failedTest (@failedTests) {
	    print "$failedTest: failed\n";
	}
    }
    $totalFailures += $nFailed;
}    

if ($totalFailures) {
    die "Unit tests failed";
}
