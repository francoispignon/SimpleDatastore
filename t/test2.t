#!/usr/bin/perl -w -I..
# test basic SimpleDatastore functionality
use Data::Dumper;
use strict;
use Test::More tests => 12;
use SimpleDatastore;
use File::stat;

$CachedHash::Max = 10;

#Clean up old runs
if (-e "mytable.tab") {
	!system("rm mytable.tab mytable.def mytable.idx") ||
	die "Couldnt clean up old test runs: $!";
}
my $datastore = new SimpleDatastore();
$datastore->create_table("mytable",schema=>"STB=64K;TITLE=64K;PROVIDER=64;DATE=10K;REV=7;VIEW_TIME=5");
ok(-e "mytable.tab","table file created");

my $testdata=<<'END';
STB|TITLE|PROVIDER|DATE|REV|VIEW_TIME
stb1|the matrix|warner bros|2014-04-01|4.00|1:30
stb1|unbreakable|buena vista|2014-04-03|6.00|2:05
stb2|the hobbit|warner bros|2014-04-02|8.00|2:45
stb2|the hobbit2|warner bros|2014-04-02|8.00|2:45
stb2|the hobbit3|warner bros|2014-04-02|8.00|2:45
stb2|the hobbit4|warner bros|2014-04-02|8.00|2:45
stb3|the matrix|warner bros|2014-04-02|4.00|1:05
stb3|the matrix2|warner bros|2014-04-02|4.00|1:05
stb3|the matrix3|warner bros|2014-04-02|4.00|1:05
stb3|the matrix4|warner bros|2014-04-02|4.00|1:05
stb3|the matrix5|warner bros|2014-04-02|4.00|1:05
stb4|star trek|viacom|2015-05-05|1.00|2:00
stb5|ancillary title1|astudios|2015-05-05|5.00|2:00
stb5|ancillary title2|astudios|2015-07-03|11.00|13:00
stb5|ancillary title3|astudios|2015-05-05|2.00|2:00
END
local *TESTFILE;
open(TESTFILE,"> testdata.txt") || die "Couldnt open testdata.txt: $!";
print TESTFILE $testdata;
close TESTFILE;
$datastore->import_flatfile("testdata.txt",skipinitial=>1);
ok(0 != stat("mytable.tab")->size(),"mytable nonempty");

#Simple select with order (default sorting)
my $orderedresults_defaultsort = $datastore->query('select TITLE,REV,DATE order by REV');
print Dumper $orderedresults_defaultsort;
ok($orderedresults_defaultsort->[1]->[0] =~ /ancillary title2/, "default ordering, no filtering");

#Simple select with order (user provided sortingfunc)
my $orderedresults = $datastore->query('select TITLE,REV,DATE order by REV',sortingfuncs=>{REV=>sub {$_[0] <=> $_[1]}});
ok($#$orderedresults == 14,"user ordering, no filtering, row count correct");
ok(2==$#{$orderedresults->[0]}, "user ordering, no filtering, column count correct");
ok($orderedresults->[0]->[0] =~ /star trek/, "user ordering, no filtering, first result correct");
ok($orderedresults->[1]->[0] =~ /ancillary title3/, "user ordering, no filtering, second result correct");
ok($orderedresults->[$#$orderedresults]->[0] =~ /ancillary title2/, "user ordering, no filtering, last result correct");
my $orderedresults_twocolumns = $datastore->query('select TITLE,DATE order by REV',sortingfuncs=>{REV=>sub {$_[0] <=> $_[1]}});
ok(1==$#{$orderedresults_twocolumns->[0]}, "user ordering, no filtering, different select clause, column count correct");

###############################################################################
#simple where clause

my $whereresultset = $datastore->query('select TITLE,REV,DATE where PROVIDER="viacom"');
ok($#$whereresultset == 0,"simple where: correct count returned");
ok($whereresultset->[0]->[0] =~ /star trek/,"simple where: correct result returned");

# A primary key in an imported file identical to an existing 
# primary key already in the datastore should cause the old row 
# of the table to be overwritten with the contents given by 
# the newer imported file
my $newer_import_data=<<'END';
stb5|ancillary title2|astudios2|2015-07-03|15.00|3:00
stb5|ancillary title3|astudios3|2015-05-05|2.00|7:00
END

my $newer_import_file = "newerdata.txt";
local *NEWERDATA;
open(NEWERDATA,"> $newer_import_file") || die "Couldnt open $newer_import_file for writing: $!";
print NEWERDATA $newer_import_data;
close NEWERDATA;

$datastore->import_flatfile("newerdata.txt");
my $newprovider = $datastore->query("select PROVIDER where TITLE=\"ancillary title2\"");
ok($newprovider->[0]->[0] =~ /astudios2/,"newer import file with identical primary keys overwrites older one");

