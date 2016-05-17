#!/usr/bin/perl -w
# Tie a hash to a simple disk cache 
#
# CachedHash adds two properties to the usual key uniqueness 
# property of a plain perl hash:
# 1) persistence
# 2) ordering
#
# Persistence means both that elements survive between 
# instantiations, and also that the total number of elements
# need not be limited by virtual memory. Ordering serves to
# promote locality of reference, so that sequential elements
# retrieved from secondary storage should require fewer
# disk accesses (the default perl Hash iterates over its
# key-value pairs in a pseudorandom order).
#
# Although this class's client may opt to use any arbitrary
# scalar as the value corresponding to keys (in particular, by
# customizing $CachedHash::pack_template), the default use case 
# is a signed long long ("q"), adequate for representing the value
# returned by ftell(3) on Linux. 
# $CachedHash::key_length is a fixed value, but may also be 
# customized by clients before creating a new CachedHash.
# (Note: A more efficient real world implementation would use 
# a Btree for persistence of hash elements -- see comments below)

use strict;
use Fcntl qw(SEEK_END SEEK_SET);
package CachedHash;
	require Tie::Hash;
	@CachedHash::ISA = qw(Tie::ExtraHash);
	$CachedHash::cachefile = ".cachefile";
	my $hashsize=0;
	#Arbitrary limit to hash size, reset to customize:
	$CachedHash::Max = 10000000;
	$CachedHash::pack_template = "q";
	$CachedHash::key_length = 141;
	my $valuelen=0;
	my $offsetlen=0;
	my $cache_entry_length = $CachedHash::key_length+$valuelen+$offsetlen;
        # All methods provided by default, define
        # only those needing overrides
        # Accessors access the storage in %{$_[0][0]};
        # TIEHASH should return an array reference with the first element
        # being the reference to the actual storage
	sub TIEHASH {
		my $class = shift;
		my $storage = bless [{}, @_], $class;
		if ($valuelen==0) {
			my $buf=pack($CachedHash::pack_template,1);
			$valuelen = length $buf;
		}
		if ($offsetlen==0) {
			my $buf=pack("q",1);
			$offsetlen = length $buf;
		}
		$cache_entry_length = $CachedHash::key_length+$valuelen+$offsetlen;
		if (! -e $CachedHash::cachefile) {
			!system("touch $CachedHash::cachefile") || die "Couldnt access cachefile $CachedHash::cachefile: $!";
		}
		open ($CachedHash::DISKCACHE,"+< $CachedHash::cachefile") || die "Couldnt open cachefile $CachedHash::cachefile: $!";
		$storage;

	}


	sub CLEAR {
		my $this = shift;
		$hashsize=0;
		$this->SUPER::CLEAR();
	}

	# Deterministically read the first entry in
	# cache
	sub FIRSTKEY {
		#
		my $this = shift;
		$CachedHash::inside_each = 1;
		if (!defined $CachedHash::iteration_count || $CachedHash::iteration_count == -1) {
			$CachedHash::iteration_count = 1;
		}
		my $key = $this->_read_first_cache_entry();
		return $key;
	}
	
	sub NEXTKEY {
		my $this = shift;
		my $key = $this->_read_next_cache_entry();
		$CachedHash::inside_each = 1;
		return $key;
	}

	sub _read_next_cache_entry {
		if (!@CachedHash::iteration_tuple) {
			die "No iteration_tuple defined! Something went wrong during iteration...";
		}
		if ($#CachedHash::iteration_tuple == -1) {
			return undef;
		}
		my $offset = $CachedHash::iteration_tuple[2] + $cache_entry_length;
		seek($CachedHash::DISKCACHE,$offset,0);
		my $buf;
		my $read_bytes=read($CachedHash::DISKCACHE,$buf,$cache_entry_length);
		my $retval;
		if ($read_bytes && $read_bytes != $cache_entry_length) {
			die("read_bytes $read_bytes != to cache_entry_length: $cache_entry_length: your disk cache appears to be corrupted");
		}
		if ($read_bytes) {
			my ($key,$value,$offset) = unpack("A" . $CachedHash::key_length . $CachedHash::pack_template . "q",$buf);
			@CachedHash::iteration_tuple = ($key,$value,$offset);
			return $key;
		}
		else {
			@CachedHash::iteration_tuple = ();
			return undef;
		}
	}


	sub _read_first_cache_entry {
		seek($CachedHash::DISKCACHE,0,Fcntl::SEEK_SET);
		my $buf;
		my $read_bytes=read($CachedHash::DISKCACHE,$buf,$cache_entry_length);
		my $retval;
		if ($read_bytes && $read_bytes != $cache_entry_length) {
			die("read_bytes $read_bytes != to cache_entry_length: $cache_entry_length: your disk cache appears to be corrupted");
		}
		if ($read_bytes) {
			my ($key,$value,$offset) = unpack("A" . $CachedHash::key_length . $CachedHash::pack_template . "q",$buf);
			@CachedHash::iteration_tuple = ($key,$value,$offset);
			return $key;
		}
		else {
			@CachedHash::iteration_tuple = ();
			return undef;
		}
	}

	sub EXISTS {
		my ($this,$key) = @_;
		if ($this->SUPER::EXISTS($key)) {
			return 1;
		}
		if ($hashsize<=$CachedHash::Max) {
			return 0;
		}
		my @valuetuple = _search_disk_cache($key);
		if (@valuetuple) {
			return 1;
		}
		return 0;
	}

	#When a hash is "full" (equal in size to the maximum configured), 
	#evict a random member from hash and replace it with this one,
	#and add it to the disk cache.
	#We could, alternatively, consider implementing a "least recently
	#used" (LRU) cache. Also, it'd be much more efficient to write
	#a whole block's worth of records at a time, instead of evicting
	#elements one-by-one. For that, we'd want to make _add_to_disk_cache
	#and _delete_from_disk_cache operate on blocks instead of lines
	sub STORE {
		my ($this,$key,$value) = @_;
		
			if ($this->SUPER::EXISTS($key)) {
				my @old_value_tuple = $this->SUPER::FETCH($key);
				my $existing_offset=$old_value_tuple[1];
				_overwrite_cache_entry($key,$value,$existing_offset);
				my @new_value_tuple = ($value,$existing_offset);
				$this->SUPER::STORE($key,@new_value_tuple);
				return;
			}
			my @cachedvaluetuple=_search_disk_cache($key);
			if ($#cachedvaluetuple==-1) {
				my $offset=_append_to_disk_cache($key,$value);
				$this->SUPER::STORE($key,($value,$offset));
				++$hashsize; 
			}
			if ($hashsize >= $CachedHash::Max) {
				my $tempkey;
				$tempkey = $this->SUPER::FIRSTKEY();
				#make sure we dont fetch the thing we just
				#added
				if ($tempkey eq $key) {
					$tempkey = $this->SUPER::NEXTKEY($tempkey);
				}
				#eject surplus key
				$this->SUPER::DELETE($tempkey);
			}
	}

	sub FETCH {
		my ($this,$key) = @_;
	  	if ($this->SUPER::EXISTS($key)) {
			my @tuple = $this->SUPER::FETCH($key);
			return $tuple[0];
		}
		if (defined $CachedHash::inside_each && $CachedHash::inside_each == 1) {
			#During iterations (eg, each, foreach),
			#do NOT call SUPER::EXISTS
			$CachedHash::inside_each = 0;
			if (@CachedHash::iteration_tuple) {
				return $CachedHash::iteration_tuple[1];
			}
			else {
				return undef;
			}
		}
		my @valuetuple =_search_disk_cache($key);
		if (@valuetuple && $hashsize>=$CachedHash::Max) {
			#evict a random element from hash, save it to
			#diskcache, and add the present element
			my $tempkey = $this->SUPER::FIRSTKEY();
			#Make sure you don't evict the key we want to add
			if ($tempkey eq $key) {
				$tempkey = $this->SUPER::NEXTKEY($tempkey);
			}
			$this->SUPER::DELETE($tempkey);
			$this->SUPER::STORE($key,@valuetuple);
			return $valuetuple[1];
		}
		elsif (@valuetuple and $#valuetuple==2) {
			$this->SUPER::STORE($key,@valuetuple);
		}
	}

	#We cowardly avoid addressing the thorny problem of
	#deleting from the cache! Perhaps some kind of "vacuum"
	#procedure should be periodically performed...
#        sub DELETE {
#        }

	#The following subroutines implement the (naive) disk cache. In a 
	#real world application, we'd probably need to store the
	#data in the file as a btree (eg using BerkeleyDB, DB_File), to 
	#make retrieval from large caches efficient, and leave some spare 
	#space in each block, to avoid excessive writes when adding new 
	#elements to the cache

	#Add an element to the disk cache
	sub _append_to_disk_cache {
		my ($key,$value) = @_;
		seek($CachedHash::DISKCACHE,0,Fcntl::SEEK_END);
		my $position = tell($CachedHash::DISKCACHE);
		my $buf = pack("A" . $CachedHash::key_length . $CachedHash::pack_template . "q",$key,$value,$position);
		print $CachedHash::DISKCACHE $buf;
		return $position;
	}
		
	sub _add_to_disk_cache {
		my ($key,$value) = @_;
		seek($CachedHash::DISKCACHE,0,Fcntl::SEEK_END);
		print $CachedHash::DISKCACHE "$key=$value\n";
	}


	#Linear search of disk cache. (Warning: bound to be very slow
	#for very large datasets; see comment above)
	sub _search_disk_cache {
		my $key = shift;
		seek $CachedHash::DISKCACHE,0,Fcntl::SEEK_SET;
		my $inputline;
		my $nread;
		my $buf;
		while($nread=read($CachedHash::DISKCACHE,$buf,$CachedHash::key_length+$valuelen+$offsetlen)) {
			my ($storedkey,$storedvalue,$storedoffset);
			eval {
			   ($storedkey,$storedvalue,$storedoffset) = unpack("A" . $CachedHash::key_length . $CachedHash::pack_template . "q", $buf);
			};
			if ($@) {
				my $pos = tell($CachedHash::DISKCACHE);
				print STDERR "Error reading data from database at offset $pos:\n";
				die $@;
			}
			if ($key eq $storedkey) {
				return ($storedvalue,$storedoffset);
			}
		}
		return ();
				
	}
	sub _overwrite_cache_entry {
		my ($key,$value,$existing_offset) = @_;
		seek($CachedHash::DISKCACHE,$existing_offset,0);
		my $buf;
		eval {
			$buf = pack("A" . $CachedHash::key_length . $CachedHash::pack_template . "q",$key,$value,$existing_offset);
			print $CachedHash::DISKCACHE $buf;
		};
		if ($@) {
			print STDERR "Failed to write packed entry to disk cache in _overwrite_cache_entry, possible database corruption?";
			die $@;
		}
	}

	1;
