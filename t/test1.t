#!/usr/bin/perl -w -I..
use strict;
use Test::More tests => 3;
use CachedHash;

$CachedHash::Max = 10;

my %hash;
tie %hash, 'CachedHash';
my %testhash = (one => 1, two => 2, three => 3, asdf=>52,elephant=>100,parrot=>2002,random=>99,coca=>1001,earth=>4344,red=>999,table=>10033,lamp=>4300,spoon=>3);
my $nkeys;
my $nexpected = keys %testhash;
my %myhash;
tie %myhash,'CachedHash';
%myhash = (one => 1, two => 2, three => 3, asdf=>52,elephant=>100,parrot=>2002,random=>99,coca=>1001,earth=>4344,red=>999,table=>10033,lamp=>4300,spoon=>3);
$nkeys = keys %myhash;
ok(-e ".cachefile",".cachefile created");
ok($nkeys == $nexpected, "count of keys as expected");
my @sorted_keys = sort (keys %myhash);
my @sort_expected = sort (keys %testhash);
ok(@sorted_keys==@sort_expected,"keys match expected");
