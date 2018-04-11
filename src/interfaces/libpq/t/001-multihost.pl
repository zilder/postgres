# Minimal test testing streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 28;

# Initialize master node

my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1, use_tcp => 1);
$node_master->start;
my $backup_name = 'my_backup';

# Take backup
$node_master->backup($backup_name);

# Create streaming standby linking to master
my $node_standby_1 = get_new_node('standby_1');

$node_standby_1->init_from_backup(
	$node_master, $backup_name,
	has_streaming => 1,
	use_tcp       => 1);
$node_standby_1->start;

# Take backup of standby 1 (not mandatory, but useful to check if
# pg_basebackup works on a standby).
$node_standby_1->backup($backup_name);

# Create second standby node linking to standby 1
my $node_standby_2 = get_new_node('standby_2');
$node_standby_2->init_from_backup(
	$node_standby_1, $backup_name,
	has_streaming => 1,
	use_tcp       => 1);
$node_standby_2->start;


sub get_host_port
{
	my $node = shift;
	return "$PostgresNode::test_localhost:" . $node->port;
}

sub multiconnstring
{
	my $nodes    = shift;
	my $database = shift || "postgres";
	my $params   = shift;
	my $extra    = "";
	if ($params)
	{
		my @cs;
		while (my ($key, $val) = each %$params)
		{
			push @cs, $key . "=" . $val;
		}
		$extra = "?" . join("&", @cs);
	}
	my $str =
	    "postgresql://"
	  . join(",", map({ get_host_port($_) } @$nodes))
	  . "/$database$extra";
	return $str;

}

sub connstring2
{
	my $nodes    = shift;
	my $database = shift;
	my $params   = shift;
	my @args     = ();
	for my $n (@$nodes)
	{
		push @args, "host=" . get_host_port($n);
	}
	push @args, "dbname=$database" if defined($database);
	while (my ($key, $val) = each %$params)
	{
		push @args, "$key=$val";
	}
	return join(" ", @args);
}

sub psql_conninfo
{
	my ($connstr) = shift;
	my ($timed_out);
	my ($retcode, $stdout, $stderr) =
	  psql($connstr, '\conninfo', timed_out => \$timed_out);
	if ($retcode == 0 && $stdout =~ /on host "([^"]*)" at port "([^"]*)"/s)
	{
		return "$1:$2";
	}
	else
	{
		return "STDOUT:$stdout\nSTDERR:$stderr";
	}
}

sub psql_server_addr
{
	my ($connstr) = shift;
	my ($timed_out);
	my $sql =
	  "select abbrev(inet_server_addr()) ||':'||inet_server_port();\n";
	my ($retcode, $stdout, $stderr) =
	  psql($connstr, $sql, timed_out => \$timed_out);
	if ($retcode == 0)
	{
		return $stdout;
	}
	else
	{
		return "STDOUT:$stdout\nSTDERR:$stderr";
	}
}
my $conninfo;

# Test 1.1 - all hosts available, master first, readwrite requested
$conninfo =
  psql_conninfo(
	multiconnstring([ $node_master, $node_standby_1, $node_standby_2 ]));
is($conninfo, get_host_port($node_master), "master first, rw, conninfo");

# Test 1.2
$conninfo =
  psql_server_addr(
	multiconnstring([ $node_master, $node_standby_1, $node_standby_2 ]));
is($conninfo, get_host_port($node_master), "master first, rw, server funcs");

# Test 2.1 - use symbolic name for master and IP for slave
$conninfo =
  psql_conninfo("postgresql://localhost:"
	  . $node_master->port
	  . ",127.0.0.1:"
	  . $node_standby_1->port
	  . "/postgres");

is( $conninfo,
	"localhost:" . $node_master->port,
	"master symbolic, rw, conninfo");

# Test 2.2 - check server-side connect info (would return numeric IP)
$conninfo =
  psql_server_addr("postgresql://localhost:"
	  . $node_master->port
	  . ",127.0.0.1:"
	  . $node_standby_1->port
	  . "/postgres");
is( $conninfo,
	"127.0.0.1:" . $node_master->port,
	'master symbolic, rw server funcs');

# Test 3.1 - all nodes available, master second, readwrite requested
$conninfo =
  psql_conninfo(
	multiconnstring([ $node_standby_1, $node_master, $node_standby_2 ]));

is($conninfo, get_host_port($node_master), "master second,rw, conninfo");

# Test 3.2 Check server-side connection info
$conninfo =
  psql_server_addr(
	multiconnstring([ $node_standby_1, $node_master, $node_standby_2 ]));

is($conninfo, get_host_port($node_master), "master second, rw, server funcs");

# Test 4.1 - use symbolic name for slave and IP for smaster
$conninfo =
  psql_conninfo("postgresql://localhost:"
	  . $node_standby_1->port
	  . ",127.0.0.1:"
	  . $node_master->port
	  . "/postgres");
is( $conninfo,
	"127.0.0.1:" . $node_master->port,
	"slave symbolic, rw,conninfo");

# Test 4.2 - check server-side connect info
$conninfo =
  psql_server_addr("postgresql://localhost:"
	  . $node_standby_1->port
	  . ",127.0.0.1:"
	  . $node_master->port
	  . "/postgres");
is( $conninfo,
	"127.0.0.1:" . $node_master->port,
	"slave symbolic rw, server funcs");

# Test 5 - all nodes available, master first, readonly requested

$conninfo = psql_conninfo(
	multiconnstring(
		[ $node_master, $node_standby_1, $node_standby_2 ],
		undef, { target_server_type => 'any' }));

is($conninfo, get_host_port($node_master), "master first, ro, conninfo");

# Test 6 - all nodes available, master second, readonly requested
$conninfo = psql_conninfo(
	multiconnstring(
		[ $node_standby_1, $node_master, $node_standby_2 ],
		undef, { target_server_type => 'any' }));

is($conninfo, get_host_port($node_standby_1), "master second, ro conninfo");

# Test 7.1 - all nodes available, random order, readonly.
# Expect that during six attempts any of three nodes would be collected
# at least once

my %conncount = ();
for (my $i = 0; $i < 9; $i++)
{
	my $conn = psql_conninfo(
		multiconnstring(
			[ $node_master, $node_standby_1, $node_standby_2 ],
			undef,
			{ target_server_type => 'any', hostorder => 'random' }));
	$conncount{$conn}++;
}
is(scalar(keys(%conncount)), 3, 'random order, readonly connect');

# Test 7.2 - alternate (jdbc compatible) syntax for randomized hosts

for (my $i = 0; $i < 6; $i++)
{
	my $conn = psql_conninfo(
		multiconnstring(
			[ $node_master, $node_standby_1, $node_standby_2 ],
			undef,
			{ targetServerType => 'any', loadBalanceHosts => "true" }));
	$conncount{$conn}++;
}

#diag(join(",",keys %conncount));
is(scalar(keys %conncount),
	3, "alternate JDBC-compatible syntax for random order");

# Test 8 - all nodes available, random order, readwrite
# Expect all six connections go to the master

%conncount = ();
for (my $i = 0; $i < 6; $i++)
{
	my $conn = psql_conninfo(
		multiconnstring(
			[ $node_master, $node_standby_1, $node_standby_2 ],
			undef, { hostorder => 'random' }));
	$conncount{$conn}++;
}

is(length(keys %conncount), 1, 'random order, rw connect only one node');
ok(exists $conncount{ get_host_port($node_master) },
	'random order, rw connects master');

# Test 8.1 one host in URL, master
$conninfo = psql_conninfo(multiconnstring([$node_master]));
is($conninfo, get_host_port($node_master), "old behavoir compat - master");


# Test 8.2 one host in URL, slave
$conninfo = psql_conninfo(multiconnstring([$node_standby_1]));
is($conninfo, get_host_port($node_standby_1), "old behavoir compat - slave");

# Test 9 - try to connect only slaves in rw mode

$conninfo =
  psql_conninfo(multiconnstring([ $node_standby_1, $node_standby_2 ]));
is( $conninfo,
"STDOUT:\nSTDERR:psql: cannot make RW connection to hot standby node 127.0.0.1",
	"cannot connect just slaves in RW mode");



# Test 10 - one of slaves is not available
$node_standby_1->stop();

# Test 10.1

$conninfo =
  psql_conninfo(
	multiconnstring([ $node_standby_1, $node_master, $node_standby_2 ]));

is($conninfo, get_host_port($node_master), "first node is unavailable");

# Test 10.2

$conninfo =
  psql_conninfo(
	multiconnstring([ $node_standby_2, $node_standby_1, $node_master ]));

is( $conninfo,
	get_host_port($node_master),
	"first node standby, second unavailable");

# Test 10.3

$conninfo = psql_conninfo(
	multiconnstring(
		[ $node_standby_1, $node_standby_2, $node_master ],
		undef, { target_server_type => 'any' }));
is( $conninfo,
	get_host_port($node_standby_2),
	"first node unavailable, second standmby, readonly mode");

$node_standby_1->start();

$node_master->stop();

$conninfo =
  psql_conninfo(
	multiconnstring([ $node_standby_1, $node_master, $node_standby_2 ]));

is( $conninfo,
"STDOUT:\nSTDERR:psql: cannot make RW connection to hot standby node 127.0.0.1",
	"master unavialble, cannot connect just slaves in RW mode");

$conninfo = psql_conninfo(
	multiconnstring(
		[ $node_master, $node_standby_1, $node_standby_2 ],
		undef, { target_server_type => 'any' }));

is( $conninfo,
	get_host_port($node_standby_1),
	"Master unavailable, read only ");

$node_master->start();

# Test 11 Alternate syntax

$conninfo =
  psql_conninfo(
	connstring2([ $node_standby_1, $node_standby_2, $node_master ]));

is( $conninfo,
	get_host_port($node_master),
	"Alternate syntax, master third, rw");



$conninfo =
  psql_conninfo(
	connstring2([ $node_master, $node_standby_1, $node_standby_2 ]));

is( $conninfo,
	get_host_port($node_master),
	"Alternate syntax, master first, rw");



$conninfo = psql_conninfo(
	connstring2(
		[ $node_standby_1, $node_standby_2, $node_master ],
		undef, { target_server_type => 'any' }));

is( $conninfo,
	get_host_port($node_standby_1),
	"Alternate syntax, master third, ro");



$conninfo = psql_conninfo(
	connstring2(
		[ $node_master, $node_standby_1, $node_standby_2 ],
		undef, { target_server_type => 'any' }));

is( $conninfo,
	get_host_port($node_master),
	"Alternate syntax, master first, ro");


# Test 11.5 one host in URL, master
$conninfo = psql_conninfo(connstring2([$node_master]));
is( $conninfo,
	get_host_port($node_master),
	"alt syntax old behavoir compat - master");


# Test 11.6 one host in URL, slave
$conninfo = psql_conninfo(connstring2([$node_standby_1]));
is( $conninfo,
	get_host_port($node_standby_1),
	"alt syntax old behavoir compat - slave");



