use Mojo::Base -strict;

use Test::More;
use List::Util 'first';
use Mango;
use Mojo::IOLoop;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Run command blocking
my $mango = Mango->new($ENV{TEST_ONLINE});
ok $mango->db->command('getnonce')->{nonce}, 'command was successful';
ok !$mango->is_active, 'no operations in progress';

# Run command non-blocking
my ($fail, $nonce);
$mango->db->command(
  'getnonce' => sub {
    my ($db, $err, $doc) = @_;
    $fail  = $err;
    $nonce = $doc->{nonce};
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
ok $nonce, 'command was successful';

# Get collection names blocking
my $collection = $mango->db->collection('database_test');
$collection->insert({test => 1});
ok first { $_ eq 'database_test' } @{$mango->db->collection_names},
  'found collection';
$collection->drop;

# Get collection names non-blocking
$collection->insert({test => 1});
$fail = undef;
my $found;
$mango->db->collection_names(
  sub {
    my ($db, $err, $names) = @_;
    $fail  = $err;
    $found = $names;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
ok first { $_ eq 'database_test' } @$found, 'found collection';
$collection->drop;

# Interrupted blocking command
my $port = Mojo::IOLoop->generate_port;
$mango = Mango->new("mongodb://localhost:$port");
my $id = $mango->ioloop->server((port => $port) => sub { $_[1]->close });
eval { $mango->db->command('getnonce') };
like $@, qr/Premature connection close/, 'right error';
$mango->ioloop->remove($id);

# Interrupted non-blocking command
Mojo::IOLoop->generate_port;
$mango = Mango->new("mongodb://localhost:$port");
$id    = Mojo::IOLoop->server((port => $port) => sub { $_[1]->close });
$fail  = $nonce = undef;
$mango->db->command(
  'getnonce' => sub {
    my ($db, $err, $doc) = @_;
    $fail  = $err;
    $nonce = $doc;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
Mojo::IOLoop->remove($id);
like $fail, qr/Premature connection close/, 'right error';
ok !$nonce, 'command was not successful';

done_testing();
