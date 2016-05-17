#!/usr/bin/perl -w -I.
# SimpleDatastore accepts a simple schema definition and creates
# a table based on it, which can be populated from flat files by 
# repeatedly calling method import_flatfile() on it; any primary
# keys specified in the schema will be enforced on the resulting
# table, so as to guarantee uniqueness. (Primary keys are stored
# in an index managed by CachedHash.) When iterating over 
# elements of the table, the index is first used to locate the
# byte offset in the table from which to read the data for the
# corresponding row, so that we can fseek directly to those rows. 

use strict;
package SimpleDatastore;
use CachedHash;
use Fcntl qw(SEEK_END SEEK_SET);
use FileHandle;
my $hashtable;

sub new {
	my $class = shift;
	my $self = {};
	bless $self,$class;
	if (defined $_[1]) {
		$self->create_table(@_);
	}
	return $self;
}
 
sub create_table {
	my $self = shift;
	my $tablename = shift;
	$$self{tablename} = $tablename;
	$$self{index} = {};
	$CachedHash::cachefile = "$tablename.idx";
	if (-e "$tablename.tab") {
		die "Table $tablename already exists! Cowardly refusing to risk overwriting it. You should call \"open_table()\" instead...";
	}
	!system("touch $tablename.tab") || die "Couldnt touch $tablename.tab: $!";
	tie %{$$self{index}}, 'CachedHash';

	my %args = @_;
	#schema definition format:
	#columname=length[K];...)
	#where "length" is max length of the column in bytes, optionally
	#followed by capital "K" to indicate primary key columns
	#for simplicity sake, we will deduce the type of column (eg,
	#int, "varchar", etc) based on contents
	$$self{schema} = $args{schema}||"record=256K";
	#(if you dont specify a schema, then every row
	#is considered to consist of a single primary key 
	#column called "record")
	my $tabledef = "$tablename.def";
	local *TDEF;
	open(TDEF,"> $tabledef") || die "Couldnt open $tabledef for write: $!";
	print TDEF $$self{schema};
	close TDEF;
	$self->_parse_schema();
}

sub open_table {
	my $self = shift;
	my $tablename = shift;
	$$self{tablename} = $tablename;
	$$self{index} = {};
	if (! -e "$tablename.idx") {
		die "Couldnt find primary key index for table $tablename";
	}
	$CachedHash::cachefile = "$tablename.idx";
	tie %{$$self{index}}, 'CachedHash';
	my $tabledef = "$tablename.def";
	if (! -e $tabledef) {
		die "Couldnt find table definition $tablename.def for table $tablename";
	}
	local *TDEF;
	open (TDEF,"< $tabledef") || die "Couldnt open $tabledef for read: $!";
	$$self{schema}=<TDEF>;
	close TDEF;
	
	$self->_parse_schema();
}

sub _parse_schema {
    my $self = shift;
    my @columndefs = split(/;/,$$self{schema});
    #=(\d+[K]?)
    my $last_offset=0;
    $$self{rawrowlength} = 0; 
    my $key_column_idx = 0;
    my $column_count = @columndefs;
    $$self{column_count} = $column_count;
    foreach my $columnidx (0..$#columndefs) {
	$columndefs[$columnidx] =~ /^([^=]+)=(?:(\d+)([K]?))/ && do {
	    $$self{schema_array}->[$columnidx] = "$1=$2";
	    my $keyspec = $3;
	    my $columnwidth = $2;
	    my $columnname = $1;
	    $$self{column_names}->{$columnname} = $columnidx;
	    $$self{column_widths}->[$columnidx] = $columnwidth;
	    if (defined $keyspec && $keyspec =~ /K/) {
		$$self{schema_array}->[$columnidx] .= "$keyspec";
		push @{$$self{key_columns}},$columnidx;
		$$self{key_column_map}->{$columnidx} = $key_column_idx;
		$$self{reverse_key_column_map}->{$key_column_idx}=$columnidx;
		++$key_column_idx;
	    }
	    else {
		push @{$$self{nonkey_columns}},$columnidx;
		$$self{column_offsets}->{$columnidx} = $last_offset;
		$last_offset += $2;
		$$self{rawrowlength} += $columnwidth;
	    }
	};
    }
}

# Read in a flat file, and store the primary key columns to a CachedHash index
# The other columns will be stored to a "tab" file. The value of each key in
# the CachedHash index gives the byte offset of the remaining columns for
# the row corresponding to the given primary key, so that we may fseek
# directly to it.
sub import_flatfile {
    my ($self,$import_file) = @_;
    my %args = @_;
    my $delim = $args{delim} || '|';
    local *OUTPUTDST;
    open(OUTPUTDST,"+< $$self{tablename}.tab") || die "Couldnt open table file $$self{tablename}.tab: $!";
    seek(OUTPUTDST,0,Fcntl::SEEK_END);
    
    local *INPUTSRC;
    open(INPUTSRC,"< $import_file") || die "Couldnt open source file $import_file: $!";
    my $inputline_number = 0;
    my $skiplines=0;
    if (exists $args{skipinitial}) {
	$skiplines = $args{skipinitial};
    }
    my @key_columns = @{$$self{key_columns}};
    while(my $inputline=<INPUTSRC>) {
	++$inputline_number;
	if ($inputline_number <= $skiplines) {next;}
	chomp $inputline;
	my $quoted_delim = quotemeta($delim);
	my @row = split(/$quoted_delim/,$inputline);
	my $key="";
	foreach my $primarycolumn (@key_columns) {
	    $key .= $row[$primarycolumn] . ";";
	}
	
	my $packedstring = "";
	foreach my $column_number (@{$$self{nonkey_columns}}) {
	    my $columndata;
	    my $width = $$self{column_widths}->[$column_number];
	    $columndata = sprintf('%-' . $width . "s",$row[$column_number]);
	    $packedstring .= $columndata;
	}
	
	if (exists $$self{index}->{$key}) {
	    my $position = $$self{index}->{$key};
	    seek(OUTPUTDST,$position,0);
	    print OUTPUTDST $packedstring;
	}
	else {
	    seek(OUTPUTDST,0,Fcntl::SEEK_END);
	    my $position = tell(OUTPUTDST);
	    $$self{index}->{$key} = $position;
	    print OUTPUTDST $packedstring;
	}
	
    }
    close OUTPUTDST;
    close INPUTSRC;
}

sub query {
    my $self = shift;
    my $querystatement = shift;
    my %args = @_;
    
    my $columnlist;
    my $conditionlist;
    my $orderclause;
    my $groupclause;
    my $wheregrouporder;
    $querystatement =~ /where\s+(.*)/ && do {
	$wheregrouporder = $1;
    };
    my $whereclause = $wheregrouporder;
    if (defined $whereclause) {
	$whereclause =~ s/\s+group\s+by.*//;
	$whereclause =~ s/\s+order\s+by.*//;
    }
    $querystatement =~ /group\s+by\s+
					(\w+(?:\,\w+)*)
	/xi && do {
	    $groupclause = $1;
    };
    $querystatement =~ /order\s+by\s+
					(\w+(?:\,\w+)*)
	/xi && do {
	    $orderclause = $1;
    };
    
    $querystatement =~ /select\s+(	
					(?:[\w\(\)]+(?:\,[\w\(\)]+)*)
					|
					(?:\*)
				     )
				/xi && do {
				    $columnlist = $1;
			    };
    
    #Hash that will hold the resultset and intermediate
    #structures
    my $querystruct = {
	filtered_rows => [],
	groups => {},
	ordering => {},
	selects => {},
	final_outputs => {},
    };
    
    #apply conditions
    $self->_apply_filters($whereclause,$querystruct);
    $self->_apply_groups($groupclause,$querystruct);
    $self->_apply_select($columnlist,$querystruct);
    $self->_apply_order($orderclause,$querystruct,%args);
    
    #the GROUP BY clause groups the results into a 
    #single row for each unique combination of the 
    #specified groups; the remaining clauses, 
    #select and order, operate on these rows, and any 
    #aggregate functions applicable to them.
    #If no group clause has been specified, then 
    #the presence of any aggregate functions in select
    #causes the entire resultset to be collapsed into
    #a single group, and therefore only one row will
    #appear in the output.
    #
    if (exists $querystruct->{groups}->{UNIVERSAL_GROUP}) {
	#This means there were neither groups nor
	#aggregate functions specified (simplest case)
	return $querystruct->{final_outputs}->{by_group_sorted}->{UNIVERSAL_GROUP};
    }
    
    if (exists $querystruct->{final_outputs}->{AGG_UNIVERSAL_GROUP}) {
	#This means there were no groups specified,
	#but there were aggregate functions
	return $querystruct->{final_outputs}->{AGG_UNIVERSAL_GROUP};
    }
    
    #Otherwise, you have to iterate over all the groups
    my $final_output;
    foreach my $group (keys %{$querystruct->{final_outputs}->{by_group_sorted}}) {
	foreach my $row (@{$querystruct->{final_outputs}->{by_group_sorted}->{$group}}) {
	    push @$final_output,$row;
	}
    }
    return $final_output;
}

sub _apply_filters {
    my ($self,$conditions,$querystruct) = @_;
    my $condition_column_name;
    my $condition_column_value;
    my $condition_column_number;
    my $columnoffset;
    my $columnlen;
    my @selresultcolumns;
    
    my @conditionlist;
    #if the $conditions are simple and not nested
    #or associated with parens, we can interpret	
    #without any recursive parsing:
    my @condidx;
    my @condval;
    if (defined $conditions && $conditions !~ /[\(\)]/) {
	@conditionlist=split(/,/,$conditions);
	foreach my $cond (@conditionlist) {
	    $condition_column_name=$cond;
	    $condition_column_name =~ s/=.*//;
	    $condition_column_value = $cond;
	    $condition_column_value =~ s/.*=//;
	    $condition_column_value =~ s/\x22//g;
	    if (!exists $$self{column_names}->{$condition_column_name}) {
		die "Your query specified an unknown column: $condition_column_name";
	    }
	    push @condidx,$$self{column_names}->{$condition_column_name};
	    push @condval,$condition_column_value;
	}
    }
    
    my $rowtuple;
  ROWITERATION:
    while(defined ($rowtuple = $self->_iterate_rows())) {
	if (defined $conditions) {
	    foreach my $condnum (0..$#condidx) {
		my $idx=$condidx[$condnum];
		if (_trim($$rowtuple[$idx]) ne $condval[$condnum]) {
		    next ROWITERATION;
		}
	    }
	}
	push @{$querystruct->{filtered_rows}},$rowtuple;
    }
    return $querystruct;
}

sub _apply_groups {
    my ($self,$groups,$querystruct) = @_;
    if (!defined $groups) {
	$querystruct->{groups}->{UNIVERSAL_GROUP} = [0..$#{$querystruct->{filtered_rows}}];
	return $querystruct;
    }
    my @grouplist = split(/,/,$groups);
    my @groupidx;
    foreach my $group (@grouplist) {
	if (! exists $$self{column_names}->{$group}) {
	    die "You specified unknown group \"$group\" in your query";
	}
	push @grouplist,$$self{column_names}->{$group};
    }
    my @filtered_rows = @{$$querystruct{filtered_rows}};
    foreach my $rowidx (0..$#filtered_rows) {
	my $grpkey;
	my $row = $filtered_rows[$rowidx];
	foreach my $k (0..$#groupidx) {
	    my $columnval = $row->[$groupidx[$k]];
	    $grpkey .= $columnval;
	    if ($k != $#groupidx) {
		$grpkey .= q{|};
	    }
	}
	push @{$querystruct->{groups}->{$grpkey}},$rowidx;
    }
    
}

sub _apply_select {
    my ($self,$columns,$querystruct) = @_;
    my @columnlist = split(/,/,$columns);
    my %aggregate_funcs = ("min" => sub {
	my ($selidx,$grpref) = @_;
	my $retval = $querystruct->{filtered_rows}->[$grpref->[0]]->[$selidx];
	my $value;
	foreach my $idx (0..$#$grpref) {
	    $value = $querystruct->{filtered_rows}->[$grpref->[$idx]]->[$selidx];
	    if ($retval > $value) {
		$retval = $value
	    }
	};
	$retval;
			   },
			   "max"=> sub {
			       my ($selidx,$grpref) = @_;
			       my $retval = $querystruct->{filtered_rows}->[$grpref->[0]]->[$selidx];
			       my $value;
			       foreach my $idx (0..$#$grpref) {
				   $value = $querystruct->{filtered_rows}->[$grpref->[$idx]]->[$selidx];
				   if ($retval < $value) {
				       $retval = $value
				   }
			       };
			       $retval;
			   },
			   "sum"=> sub {
			       my ($selidx,$grpref) = @_;
			       my $retval = 0;
			       foreach my $idx (0..$#$grpref) {
				   $retval += $querystruct->{filtered_rows}->[$grpref->[$idx]]->[$selidx];
			       };
			       $retval;
			   },
			   "count"=> sub {
			       my ($selidx,$grpref) = @_;
			       my $count = @$grpref;
			       $count;
			   },
			   "collect"=> sub {
			       my ($selidx,$grpref) = @_;
			       my %unique;
			       my $value;
			       foreach my $idx (0..$#$grpref) {
				   $value = $querystruct->{filtered_rows}->[$grpref->[$idx]]->[$selidx];
				   $unique{$value}=1;
			       }
			       my @retval=keys %unique;
			       return \@retval;
			   },);
    #unless one of the aggregate functions appears, all
    #that we need to is add the specified columns to selects
    
    my @selection_operations;
  NEXTCOL:
    my $call_agg_func = 0;
    foreach my $col (@columnlist) {
	foreach my $agg (keys %aggregate_funcs) {
	    if ($agg =~ /$col/) {
		$call_agg_func = 1;
		$querystruct->{final_outputs}->{are_aggregated} = 1;
		my $apply_to_col;
		my $apply_to_colidx; 
		$col =~ /$agg\((\w+)\)/ && do {
		    $apply_to_col = $1;
		};
		if (exists $$self{column_names}->{$apply_to_col}) {
		    $apply_to_colidx = $$self{column_names}->{$apply_to_col};
		}
		else {
		    die "You specified unknown column $apply_to_col in aggregate function $agg";
		}
		if (!defined $apply_to_colidx && $agg ne  "count") {
		    die "You omitted a column name in aggregate function $agg";
		}
		push @selection_operations,[$aggregate_funcs{$agg},$apply_to_colidx];
		
		next NEXTCOL;
	    }
	}
	push @selection_operations,$$self{column_names}->{$col}
    }
    
    if (!$call_agg_func) {
	$querystruct->{selects}->{cols} = \@selection_operations;
	$querystruct->{final_outputs}->{by_group}->{UNIVERSAL_GROUP} = $querystruct->{filtered_rows};
	return $querystruct;
    }
    
    
    #Aggregate the groups according to specified functions
    #(If there were no GROUP BY clauses, then 
    #aggregate functions result in creating a 
    #"AGG_UNIVERSAL_GROUP" tuple under $querystruct->{final_outputs})
    
    foreach my $aggopp (@selection_operations) {
	
	foreach my $group (keys %{$querystruct->{groups}}) {
	    if (ref $aggopp eq "ARRAY") {
		my $grpref = $querystruct->{groups}->{$group};
		my $agg_result = &{$aggopp->[0]}($aggopp->[1],$grpref);
		push @{$querystruct->{final_outputs}->{by_group}->{$group}},$agg_result;
	    }
	    else {
		push @{$querystruct->{final_outputs}->{by_group}->{$group}},$querystruct->{filtered_rows}->[$querystruct->{groups}->{$group}->[0]]->[$aggopp]
	    }
	}
	if (exists $querystruct->{groups}->{UNIVERSAL_GROUP}) {
	    if (ref $aggopp eq "ARRAY") {
		my $grpref = [0..$#{$querystruct->{filtered_rows}}];
		my $agg_result = &{$aggopp->[0]}($aggopp->[1],$grpref);
		push @{$querystruct->{final_outputs}->{AGG_UNIVERSAL_GROUP}},$agg_result
	    }
	    else {
		push @{$querystruct->{final_outputs}->{AGG_UNIVERSAL_GROUP}},$querystruct->{filtered_rows}->[0]->[$aggopp]
	    }
	}
	
    }
    
    return $querystruct;

}

sub _apply_order {
    my $self = shift;
    my $orderclause = shift;
    my $querystruct = shift;
    my %args = @_;
    my $sortingfuncs = {};
    if (exists $args{sortingfuncs}) {
	$sortingfuncs = $args{sortingfuncs};
    }
    my @orderlist;
    if (defined $orderclause) {
	@orderlist=split(/,/,$orderclause);
    }
    my @orderidx;
    my $defaultsorting = sub($$) {$_[0] cmp $_[1]};
    my $sortingfunc;# = $defaultsorting;
    foreach my $ordercolumn (@orderlist) {
	if (!exists $$self{column_names}->{$ordercolumn}) {
	    die "You specified an unknown ordering column $ordercolumn in your query";
	}
	push @orderidx,$$self{column_names}->{$ordercolumn};
    }
    if (exists $querystruct->{final_outputs}->{AGG_UNIVERSAL_GROUP}) {
	return $querystruct; #nothing to do
    }
  GROUP_ORDERING:
    foreach my $groupname (keys %{$querystruct->{groups}}) {
	my $group = $querystruct->{groups}->{$groupname};
	if (0 == @orderlist || (!@$group) || $#$group==0) {
	    #Just copy "by_group" to "by_group_sorted"
	    #We don't bother sorting groups
	    #with less than two members, nor
	    #if no ordering was specified
	    $querystruct->{final_outputs}->{by_group_sorted}->{$groupname}=$querystruct->{final_outputs}->{by_group}->{$groupname};
	    next GROUP_ORDERING;
	}		
	foreach my $orderingcol (@orderlist) {
	    my $orderingidx = $$self{column_names}->{$orderingcol};
	    $sortingfunc = $defaultsorting;
	    if (exists $$sortingfuncs{$orderingcol}) {
		$sortingfunc = $$sortingfuncs{$orderingcol};
	    }
	    my $sortwrapper = sub($$) {
		&$sortingfunc($_[0]->[$orderingidx],$_[1]->[$orderingidx]);
	    };
	    my @unsorted_input ;
	    if (exists $querystruct->{final_outputs}->{by_group_sorted}->{$groupname}) {
		@unsorted_input = @{$querystruct->{final_outputs}->{by_group_sorted}->{$groupname}}
	    }
	    else {
		@unsorted_input = map { [values @{$querystruct->{filtered_rows}->[$_]}] } @$group;
		#				@unsorted_input = 
		#				map { [values @{$unsorted_input->[$_]}, $_] } (0 .. $#$group);
	    }
	    
	    my @sorted_results = sort $sortwrapper @unsorted_input;
	    $querystruct->{final_outputs}->{by_group_sorted}->{$groupname} = \@sorted_results;
	}
	
	
    }
    
    #Now prune the columns not specified in
    #$querystruct->{selects} from the final_outputs
    #(unless the results are aggregated, in which case,
    #there's nothing to prune: all the columns were
    #directly appended by select
    
    if (!exists $querystruct->{final_outputs}->{are_aggregated} || $querystruct->{final_outputs}->{are_aggregated} == 0) {
	foreach my $groupname (keys %{$querystruct->{groups}}) {
	    foreach my $entry_in_group (0..$#{$querystruct->{final_outputs}->{by_group_sorted}->{$groupname}}) {
		my $temp = [];
		foreach my $selcol (@{$querystruct->{selects}->{cols}}) {
			push @$temp,$querystruct->{final_outputs}->{by_group_sorted}->{$groupname}->[$entry_in_group]->[$selcol];
		}
		$querystruct->{final_outputs}->{by_group_sorted}->{$groupname}->[$entry_in_group] = $temp;
	    }
	}
	return $querystruct;
    }
}
sub _iterate_rows {
    my $self = shift;
    my $rowlength = $$self{rawrowlength};
    if (!exists $$self{iteration}) {
	$$self{iteration} = 0;
    }
    my ($key,$dataoffset) = each %{$$self{index}};
    if (!defined $key) {return undef;}
    my @keycolumns = split(/;/,$key);
    my $tuple;
    foreach my $key_column (@{$$self{key_columns}}) {
	$$tuple[$key_column] = $keycolumns[$$self{key_column_map}->{$key_column}];
    }
    my $dbfh;
    if (exists $$self{DBTABLE}) {
	$dbfh = $$self{DBTABLE};
    }
    else {
	$dbfh = new FileHandle();
	$dbfh->open("< $$self{tablename}.tab") || die "Couldnt open $$self{tablename}: $!";
	$$self{DBTABLE} = $dbfh;
    }
    
    seek($dbfh,$dataoffset,0);
    my $datarow;
    my $nread = read($dbfh,$datarow,$rowlength);
    if ($nread != $rowlength) {
	die "Wrong number of bytes reading from $$self{mytable}: expected $rowlength, got $nread";
    }
    foreach my $nonkey_column (@{$$self{nonkey_columns}}) {
	my $width = $$self{column_widths}->[$nonkey_column];
	my $coloffset = $$self{column_offsets}->{$nonkey_column};
	my $columndata = substr($datarow,$coloffset,$width);
	$$tuple[$nonkey_column] = $columndata;
    }
    return $tuple
}

	
	
		
sub _extract_column_data {
	my ($self,$key,$datarow,$columnidx) = @_;
	my $retval;
	if (grep($_==$columnidx,@{$$self{key_columns}})) {
		my @keydata=split(/;/,$key);
		my $idx=$$self{key_column_map}->{$columnidx};
		$retval = $keydata[$idx];
	}
	else {
		my $idx = $$self{column_offsets}->{$columnidx};
		my $len = $$self{column_widths}->[$columnidx];
		$retval = substr($datarow,$idx,$len);
	}
	return $retval;
}

sub _trim {
	my $str = shift;
	$str =~ s/\s*$//;
	return $str;
}

1;
