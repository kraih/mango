use Mojo::Base -strict;

use Test::More;
use Mango;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Cleanup before start
my $mango  = Mango->new($ENV{TEST_ONLINE});
my $gridfs = $mango->db->gridfs;
$gridfs->$_->remove for qw(files chunks);

# Blocking roundtrip
my $writer = $gridfs->writer;
my $oid    = $writer->id;
isa_ok $oid, 'Mango::BSON::ObjectID', 'right class';
$writer->write('hello ');
$writer->write('world!');
$writer->close;
my $reader = $gridfs->reader;
$reader->open($oid);
my $data;
while (defined(my $chunk = $reader->read)) { $data .= $chunk }
is $data, 'hello world!', 'right content';
$gridfs->$_->drop for qw(files chunks);

done_testing();
