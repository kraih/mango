use Mojo::Base -strict;

use Test::More;
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

done_testing();
