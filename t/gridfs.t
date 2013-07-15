use Mojo::Base -strict;

use Test::More;
use Mango;
use Mojo::IOLoop;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Clean up before start
my $mango  = Mango->new($ENV{TEST_ONLINE});
my $gridfs = $mango->db->gridfs;
$gridfs->$_->remove for qw(files chunks);

# Blocking roundtrip
my $writer = $gridfs->writer;
$writer->filename('foo.txt')->content_type('text/plain');
$writer->write('hello ');
$writer->write('world!');
my $oid    = $writer->close;
my $reader = $gridfs->reader;
is $reader->tell, 0, 'right position';
$reader->open($oid);
is $reader->filename,     'foo.txt',    'right filename';
is $reader->content_type, 'text/plain', 'right content type';
is $reader->size,         12,           'right size';
is $reader->chunk_size,   262144,       'right chunk size';
is length $reader->upload_date, length(time) + 3, 'right time format';
my $data;
while (defined(my $chunk = $reader->read)) { $data .= $chunk }
is $reader->tell, 12, 'right position';
is $data, 'hello world!', 'right content';
$data = undef;
$reader->seek(0);
is $reader->tell, 0, 'right position';
$reader->seek(2);
is $reader->tell, 2, 'right position';
while (defined(my $chunk = $reader->read)) { $data .= $chunk }
is $data, 'llo world!', 'right content';
is_deeply $gridfs->list, ['foo.txt'], 'right files';
$gridfs->delete($oid);
is_deeply $gridfs->list, [], 'no files';
is $gridfs->chunks->find->count, 0, 'no chunks left';
$gridfs->$_->drop for qw(files chunks);

# Non-blocking roundtrip
$writer = $gridfs->writer->chunk_size(4);
$writer->filename('foo.txt')->content_type('text/plain');
my ($fail, $result);
my $delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $writer->write('he' => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail = $err;
    $writer->write('llo ' => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $writer->write('w'     => $delay->begin);
    $writer->write('orld!' => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $writer->close($delay->begin);
  },
  sub {
    my ($delay, $err, $oid) = @_;
    $fail ||= $err;
    $result = $oid;
  }
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
$reader = $gridfs->reader;
$fail   = undef;
$reader->open(
  $result => sub {
    my ($reader, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $reader->filename,     'foo.txt',    'right filename';
is $reader->content_type, 'text/plain', 'right content type';
is $reader->size,         12,           'right size';
is $reader->chunk_size,   4,            'right chunk size';
is length $reader->upload_date, length(time) + 3, 'right time format';
($fail, $data) = ();
my $cb;
$cb = sub {
  my ($reader, $err, $chunk) = @_;
  $fail ||= $err;
  return Mojo::IOLoop->stop unless defined $chunk;
  $data .= $chunk;
  $reader->read($cb);
};
$reader->$cb(undef, '');
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $data, 'hello world!', 'right content';
my ($before, $after);
$fail  = undef;
$delay = Mojo::IOLoop->delay(
  sub { $gridfs->list(shift->begin) },
  sub {
    my ($delay, $err, $names) = @_;
    $fail   = $err;
    $before = $names;
    $gridfs->delete($result => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $gridfs->list($delay->begin);
  },
  sub {
    my ($delay, $err, $names) = @_;
    $fail ||= $err;
    $after = $names;
  }
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is_deeply $before, ['foo.txt'], 'right files';
is_deeply $after, [], 'no files';
is $gridfs->chunks->find->count, 0, 'no chunks left';
$gridfs->$_->drop for qw(files chunks);

done_testing();
