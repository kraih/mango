use Mojo::Base -strict;

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use List::Util 'first';
use Mango;
use Mango::BSON 'bson_code';
use Mojo::IOLoop;

# Run command blocking
my $mango = Mango->new($ENV{TEST_ONLINE});
my $db    = $mango->db;
ok $db->command('getnonce')->{nonce}, 'command was successful';

# Run command non-blocking
my ($fail, $result);
$db->command(
  'getnonce' => sub {
    my ($db, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc->{nonce};
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
ok $result, 'command was successful';

# Get database statistics blocking
is $db->stats->{db}, $db->name, 'right name';

# Get database statistics non-blocking
($fail, $result) = ();
$db->stats(
  sub {
    my ($db, $err, $stats) = @_;
    $fail   = $err;
    $result = $stats;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result->{db}, $db->name, 'right name';

# Get collection names blocking
my $collection = $db->collection('database_test');
$collection->insert({test => 1});
ok first { $_ eq 'database_test' } @{$db->collection_names},
  'found collection';
$collection->drop;

# Get collection names non-blocking
$collection->insert({test => 1});
($fail, $result) = ();
$db->collection_names(
  sub {
    my ($db, $err, $names) = @_;
    $fail   = $err;
    $result = $names;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
ok first { $_ eq 'database_test' } @$result, 'found collection';
$collection->drop;

# Interrupted blocking command
my $port = Mojo::IOLoop->generate_port;
$mango = Mango->new("mongodb://localhost:$port");
my $id = $mango->ioloop->server((port => $port) => sub { $_[1]->close });
eval { $mango->db->command('getnonce') };
like $@, qr/Premature connection close/, 'right error';
$mango->ioloop->remove($id);

# Interrupted non-blocking command
$port  = Mojo::IOLoop->generate_port;
$mango = Mango->new("mongodb://localhost:$port");
$id    = Mojo::IOLoop->server((port => $port) => sub { $_[1]->close });
($fail, $result) = ();
$mango->db->command(
  'getnonce' => sub {
    my ($db, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
Mojo::IOLoop->remove($id);
like $fail, qr/Premature connection close/, 'right error';
is_deeply $result, {}, 'command was not successful';

done_testing();
